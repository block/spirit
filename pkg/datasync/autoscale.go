package datasync

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/autoscale"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/throttler"
)

const syncCommitLatencyThreshold = 100 * time.Millisecond

func syncFlushBounds(vcpus, clientCeiling int) (int, int) {
	width, _ := autoscale.FlushBounds(vcpus)
	width = min(width, max(1, clientCeiling))
	return width, autoscale.FlushBatchSize(width)
}

func syncAutoscaleBounds(vcpus, clientCeiling, connections int) (int, copier.AutoscaleConfig) {
	if vcpus < autoscale.MinVCPUs {
		return 0, copier.AutoscaleConfig{}
	}
	// Verification reads and repair writes share the target pool. Reserve
	// the entire change-feed flush plus six checkpoint/metadata connections,
	// then partition the remainder so both worker ceilings fit simultaneously.
	flush, _ := syncFlushBounds(vcpus, clientCeiling)
	available := connections - flush - 6
	if available < 2 {
		return 0, copier.AutoscaleConfig{}
	}
	readBudget := min(max(1, clientCeiling), available/2)
	writeBudget := min(max(1, clientCeiling), available-available/2)
	readStart, readMax := autoscale.ReadBounds(vcpus)
	writeStart := min(autoscale.WriteStart(vcpus), writeBudget)
	return min(readStart, readBudget), copier.AutoscaleConfig{
		Enabled: true, StartThreads: writeStart,
		MaxThreads:     min(throttler.ResolveMaxWriteThreads(writeStart, true, true, syncCommitLatencyThreshold > 0), writeBudget),
		MaxReadThreads: min(readMax, readBudget),
	}
}

func (r *Runner) setupAutoscaling(ctx context.Context) error {
	if !r.sync.EnableExperimentalAutoscaling {
		return nil
	}
	// Only the single target we monitor may be scaled. A custom/sharded
	// applier could write elsewhere, where this signal would offer no protection.
	if injected := r.sync.Applier; injected != nil {
		a, ok := injected.(*applier.SingleTargetApplier)
		if !ok || len(a.GetTargets()) != 1 || a.GetTargets()[0].DB != r.target.DB {
			r.logger.Warn("sync autoscaling disabled: injected applier must use the monitored single target")
			return nil
		}
	}
	if r.target.Config == nil {
		r.logger.Warn("sync autoscaling disabled: target connection config unavailable")
		return nil
	}
	aurora, err := throttler.IsAurora(ctx, r.target.DB)
	if err != nil {
		r.logger.Warn("sync autoscaling disabled: target detection failed", "error", err)
		return nil
	}
	if !aurora {
		r.logger.Info("sync autoscaling disabled: target is not Aurora")
		return nil
	}
	vcpus, err := throttler.AuroraVCPUs(ctx, r.target.DB)
	if err != nil {
		return fmt.Errorf("sync target CPU capacity: %w", err)
	}
	readStart, config := syncAutoscaleBounds(vcpus, autoscale.ClientCeiling(), r.sync.MaxConnections)
	if !config.Enabled {
		r.logger.Info("sync autoscaling disabled: target too small", "vcpus", vcpus)
		return nil
	}
	result, err := (throttler.AuroraSetup{
		Source: r.target.DB,
		OpenMonitor: func() (*sql.DB, error) {
			cfg := *r.targetDBConfig
			cfg.MaxOpenConnections = 2
			return dbconn.NewWithConnectionType(r.target.Config.FormatDSN(), &cfg, "sync target monitor")
		},
		CommitLatencyThreshold: syncCommitLatencyThreshold,
		Logger:                 r.logger,
	}).Build(ctx)
	if err != nil {
		return err
	}
	if err := r.engageAutoscaling(ctx, result, readStart, config); err != nil {
		return err
	}
	if r.autoscale.Enabled {
		r.flushConcurrency, r.flushBatchSize = syncFlushBounds(vcpus, autoscale.ClientCeiling())
	}
	return nil
}

// engageAutoscaling takes ownership of the new monitor, including on failure.
func (r *Runner) engageAutoscaling(ctx context.Context, result throttler.AuroraResult, readStart int, config copier.AutoscaleConfig) error {
	signal := throttler.NewMultiThrottler(result.Throttlers...)
	engaged := false
	defer func() {
		if !engaged {
			_ = signal.Close()
			if result.MonitorDB != nil {
				_ = result.MonitorDB.Close()
			}
		}
	}()
	if len(result.Throttlers) == 0 {
		r.logger.Warn("sync autoscaling disabled: target load signal unavailable")
		return nil
	}
	if err := signal.Open(ctx); err != nil {
		return err
	}
	r.progMu.Lock()
	r.loadSignal = signal
	r.progMu.Unlock()
	r.monitorDB = result.MonitorDB
	r.autoscale = config
	r.sync.Threads, r.sync.WriteThreads = readStart, config.StartThreads
	engaged = true
	r.logger.Info("sync autoscaling engaged; configured thread counts overridden",
		"threads", readStart, "max_read_threads", config.MaxReadThreads,
		"write_threads", config.StartThreads, "max_write_threads", config.MaxThreads)
	return nil
}

func (r *Runner) currentLoadSignal() throttler.Throttler {
	r.progMu.RLock()
	defer r.progMu.RUnlock()
	if r.loadSignal == nil {
		return &throttler.Noop{}
	}
	return r.loadSignal
}

// TargetUnderLoad is safe before and during Run. Injected change sources can
// use it as their ClientConfig.UnderLoad callback, matching the built-in feed's
// adaptive flush control. Synchronous replication writes do not use the
// applier worker queue, so resizing that queue alone cannot control flushes.
func (r *Runner) TargetUnderLoad() bool {
	return throttler.GradualOnly(r.currentLoadSignal()).IsThrottled()
}

func (r *Runner) throttleStatus(state status.State) status.ThrottleStatus {
	if state != status.CopyRows && state != status.ApplyChangeset {
		return status.ThrottleStatus{}
	}
	paused, reason, util := throttler.Describe(r.currentLoadSignal())
	return status.ThrottleStatus{Throttled: paused, Reason: reason, Utilization: util}
}
