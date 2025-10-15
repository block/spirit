package copier

import (
	"strings"
	"sync"
	"time"
)

const (
	historyIncrements = 1 * time.Hour
	oldestHistory     = 24 * time.Hour
)

type copierETA struct {
	estimate time.Duration
	asOf     time.Time
}

type copierEtaHistory struct {
	latestEstimate *copierETA
	etaHistory     []copierETA
	mutex          sync.Mutex
}

func newcopierEtaHistory() *copierEtaHistory {
	return &copierEtaHistory{
		etaHistory: make([]copierETA, 0),
	}
}

// Store the given duration as an eta with the current time
func (c *copierEtaHistory) addCurrentEstimateAndCompare(estimate time.Duration) string {
	c.addETA(copierETA{estimate: estimate, asOf: time.Now()})
	return c.getComparison()
}

// Store the given eta if history is empty or if the last ETA was >= historyIncrements ago
func (c *copierEtaHistory) addETA(eta copierETA) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// If history is empty, store the eta
	if len(c.etaHistory) == 0 {
		c.etaHistory = append(c.etaHistory, eta)
		return
	}

	// Store whatever eta we get
	c.latestEstimate = &eta

	// Only store in the history if the last ETA was >= historyIncrements ago
	lastETA := c.etaHistory[len(c.etaHistory)-1]
	if time.Since(lastETA.asOf) >= historyIncrements {
		c.etaHistory = append(c.etaHistory, eta)
	}

	// Remove old history
	for len(c.etaHistory) > 0 && time.Since(c.etaHistory[0].asOf) > oldestHistory {
		c.etaHistory = c.etaHistory[1:]
	}
}

// Return a string showing the difference between the latest ETA and the oldest ETA in the format "+30h from 1d ago"
func (c *copierEtaHistory) getComparison() string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.latestEstimate == nil || len(c.etaHistory) == 0 {
		return ""
	}

	oldest := c.etaHistory[0]

	diff := oldest.estimate - c.latestEstimate.estimate
	ago := c.latestEstimate.asOf.Sub(oldest.asOf)

	relativeDiff := diff - ago
	diffStr := shortDuration(relativeDiff.Round(time.Minute))
	if diffStr == "0s" {
		diffStr = "±0m"
	}

	return diffStr + " from " + shortDuration(ago.Round(time.Minute)) + " ago"
}

func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}
