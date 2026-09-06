// Package host groups database connections that share a MySQL server endpoint.
// Host-level work (load monitoring, capacity budgets and index restoration)
// must not treat separate schemas on the same endpoint as separate machines.
package host

import "github.com/go-sql-driver/mysql"

// Host identifies an endpoint independently of schema and credentials. Ports
// and transports remain distinct. DNS aliases and proxies cannot be resolved
// to physical servers here; callers must use consistent direct endpoints.
type Host struct {
	Network string
	Address string
}

func (h Host) String() string { return h.Network + ":" + h.Address }

// Group contains indices into the original connection list, in input order.
type Group struct {
	Host    Host
	Indices []int
}

// GroupConfigs groups normalized, non-nil connection configurations. Groups
// retain first-seen order so their representative connection is deterministic.
func GroupConfigs(configs []*mysql.Config) []Group {
	var groups []Group
	positions := make(map[Host]int)
	for i, config := range configs {
		network := config.Net
		if network == "" {
			network = "tcp"
		}
		key := Host{Network: network, Address: config.Addr}
		pos, exists := positions[key]
		if !exists {
			pos = len(groups)
			positions[key] = pos
			groups = append(groups, Group{Host: key})
		}
		groups[pos].Indices = append(groups[pos].Indices, i)
	}
	return groups
}
