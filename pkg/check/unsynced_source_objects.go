package check

import (
	"context"
	"log/slog"
)

func init() {
	// A sync copies base tables only, but unlike a move it does not refuse
	// the other objects of the source schema: the source stays live, and a
	// trigger's writes reach the change feed as row events (the change feed
	// requires ROW binlog format), so the target still receives them. The
	// check only logs them, once, at post-setup.
	register(check{name: "unsynced_source_objects", run: unsyncedSourceObjectsCheck, phases: PhasePostSetup, runtimes: Sync})
}

// unsyncedSourceObjectKinds groups schemaObjectKinds into the attributes the
// log line reports them under.
var unsyncedSourceObjectKinds = map[string]string{
	"view":      "views",
	"trigger":   "triggers",
	"procedure": "routines",
	"function":  "routines",
	"event":     "events",
}

// unsyncedSourceObjectsCheck logs the views, triggers, procedures, functions
// and events of every source schema, which sync does not copy. It never
// fails. The source may be an injected change.Source whose SQL endpoint is
// not MySQL, or a user with only SELECT, so a query that fails is logged at
// Debug and skipped, and no visibility grant is required: an object the user
// cannot see is not reported.
func unsyncedSourceObjectsCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for _, src := range r.Sources {
		if src.DB == nil {
			continue
		}
		schema := src.schema()
		objects, err := schemaObjects(ctx, src.DB, schema, allObjectKinds)
		if err != nil {
			logger.Debug("could not list source schema objects; not reporting them", "schema", schema, "error", err)
			continue
		}
		if len(objects) == 0 {
			continue
		}
		var order []string
		byAttr := map[string][]string{}
		for _, o := range objects {
			attr := unsyncedSourceObjectKinds[o.kind]
			if _, ok := byAttr[attr]; !ok {
				order = append(order, attr)
			}
			byAttr[attr] = append(byAttr[attr], o.String())
		}
		attrs := []any{"schema", schema,
			"reason", "sync copies base tables only; rows these objects write on the source reach the target as row events"}
		for _, attr := range order {
			attrs = append(attrs, attr, byAttr[attr])
		}
		logger.Info("Source schema objects are not synced", attrs...)
	}
	return nil
}
