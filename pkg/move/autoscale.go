package move

import (
	"github.com/block/spirit/pkg/throttler"
)

func closeAuroraResults(results []throttler.AuroraResult) {
	for _, result := range results {
		for _, t := range result.Throttlers {
			_ = t.Close()
		}
		if result.MonitorDB != nil {
			_ = result.MonitorDB.Close()
		}
	}
}
