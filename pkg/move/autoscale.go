package move

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/host"
	"github.com/block/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
)

// moveAutoscaleBounds deliberately uses the smallest target, not the sum of
// target capacity: skewed input can route every row to one shard. Also divide
// the client write budget across targets, since SetWriteWorkers is per shard.
func moveAutoscaleBounds(vcpus []int, groups []host.Group, clientCeiling int) (int, copier.AutoscaleConfig) {
	if len(vcpus) == 0 {
		return 0, copier.AutoscaleConfig{}
	}
	smallest := vcpus[0]
	maxShardsPerHost, targetCount := 1, 0
	for _, group := range groups {
		maxShardsPerHost = max(maxShardsPerHost, len(group.Indices))
		targetCount += len(group.Indices)
	}
	for _, n := range vcpus {
		if n < autoscale.MinVCPUs {
			return 0, copier.AutoscaleConfig{}
		}
		smallest = min(smallest, n)
	}
	readStart, readMax := autoscale.ReadBounds(smallest)
	// A distributed checksum can query each target concurrently. Schemas
	// sharing a host must share its read budget too.
	readStart = max(1, readStart/maxShardsPerHost)
	readMax = max(1, readMax/maxShardsPerHost)
	readMax = min(readMax, max(1, clientCeiling))
	readStart = min(readStart, readMax)
	writeBudget := max(1, clientCeiling/max(1, targetCount))
	writeStart := min(max(1, autoscale.WriteStart(smallest)/maxShardsPerHost), writeBudget)
	return readStart, copier.AutoscaleConfig{
		Enabled: true, StartThreads: writeStart,
		MaxThreads:     min(autoscale.Ceiling(writeStart, true), writeBudget),
		MaxReadThreads: readMax,
	}
}

// setupAutoscaling is shared by fresh and resumed moves. All targets must
// supply a usable signal before we override the configured thread counts.
// AuroraSetup and the controllers are shared with migration; move's only
// policy difference is conservative, lockstep scaling across targets (#1212).
func (r *Runner) setupAutoscaling(ctx context.Context) error {
	r.setThrottler(&throttler.Noop{})
	if !r.move.EnableExperimentalAutoscaling {
		return nil
	}
	groups := r.targetHosts()
	vcpus := make([]int, len(groups))
	for i, group := range groups {
		target := r.targets[group.Indices[0]]
		aurora, err := throttler.IsAurora(ctx, target.DB)
		if err != nil || !aurora {
			r.logger.Warn("move autoscaling disabled: every target must provide an Aurora load signal", "target", targetKey(target), "error", err)
			return nil
		}
		vcpus[i], err = throttler.AuroraVCPUs(ctx, target.DB)
		if err != nil {
			return fmt.Errorf("target %s CPU capacity: %w", targetKey(target), err)
		}
	}
	readStart, config := moveAutoscaleBounds(vcpus, groups, autoscale.ClientCeiling())
	if !config.Enabled {
		r.logger.Warn("move autoscaling disabled: target too small", "vcpus", vcpus, "min_vcpus", autoscale.MinVCPUs)
		return nil
	}
	var signals []throttler.Throttler
	var monitors []*sql.DB
	engaged := false
	defer func() {
		if !engaged {
			for _, signal := range signals {
				_ = signal.Close()
			}
			for _, db := range monitors {
				_ = db.Close()
			}
		}
	}()
	for _, group := range groups {
		target := r.targets[group.Indices[0]]
		result, err := (throttler.AuroraSetup{
			Source: target.DB,
			OpenMonitor: func() (*sql.DB, error) {
				cfg := *r.dbConfig
				cfg.MaxOpenConnections = 2
				return dbconn.NewWithConnectionType(target.Config.FormatDSN(), &cfg, "move target monitor")
			},
			// Growth above the initial write count requires a redo-log backstop.
			// Keep the same default as migration, without adding another tuning knob.
			CommitLatencyThreshold: 100 * time.Millisecond,
			Logger:                 r.logger.With("target", targetKey(target)),
		}).Build(ctx)
		if err != nil {
			return err
		}
		if result.MonitorDB != nil {
			monitors = append(monitors, result.MonitorDB)
		}
		if len(result.Throttlers) == 0 {
			// A second probe can fail after capacity discovery. Do not scale with
			// partial coverage, even if some other targets have healthy monitors.
			r.logger.Warn("move autoscaling disabled: target load signal unavailable", "target", targetKey(target))
			return nil
		}
		signals = append(signals, result.Throttlers...)
	}
	composite := throttler.NewMultiThrottler(signals...)
	if err := composite.Open(ctx); err != nil {
		return err
	}
	r.setThrottler(composite)
	r.monitorDBs = monitors
	engaged = true
	r.autoscale = config
	// Fixed independently of traffic distribution and the current worker
	// count. Each host owns a separate permit pool at this conservative cap.
	r.maxThreadsPerHost = min(autoscale.Ceiling(autoscale.WriteStart(slices.Min(vcpus)), true), autoscale.ClientCeiling())
	r.move.Threads = readStart
	r.move.WriteThreads = config.StartThreads
	r.logger.Info("move autoscaling engaged: busiest target controls all shard pools; --threads and --write-threads are ignored",
		"threads", readStart, "max_read_threads", config.MaxReadThreads,
		"write_threads_per_target", config.StartThreads, "max_write_threads_per_target", config.MaxThreads)
	return nil
}

// targetHosts is the common host view for monitoring and DDL scheduling.
func (r *Runner) targetHosts() []host.Group {
	configs := make([]*mysql.Config, len(r.targets))
	for i, target := range r.targets {
		configs[i] = target.Config
	}
	return host.GroupConfigs(configs)
}

func (r *Runner) setThrottler(t throttler.Throttler) {
	r.throttlerMu.Lock()
	defer r.throttlerMu.Unlock()
	r.throttler = t
}

func (r *Runner) currentThrottler() throttler.Throttler {
	r.throttlerMu.RLock()
	defer r.throttlerMu.RUnlock()
	return r.throttler
}
