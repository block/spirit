package check

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/table"
)

func init() {
	// The check needs the moved tables, which are discovered after the
	// preflight checks run, so it has no preflight registration. The first
	// write to a target comes after the post-setup checks (a fresh copy, and
	// the --force or empty-checkpoint wipe) or after the resume checks (a
	// resume's delete above the watermark and its replay), so registering
	// under both still refuses before any target write. The privileges check
	// requires the target visibility grants at preflight.
	//
	// A move with no tables skips the post-setup and resume checks and goes
	// straight to the cutover callback, so the runner calls
	// TargetSchemaObjectsError itself on that path.
	//
	// The pre-cutover registration runs it again under the cutover's table
	// locks, just before traffic is switched: a trigger or an event created on
	// a target during the copy has already run for the rows written since, and
	// would go live with the target.
	registerCheck("target_schema_objects", targetSchemaObjectsCheck, ScopePostSetup)
	registerCheck("target_schema_objects_resume", targetSchemaObjectsCheck, ScopeResume)
	registerCheck("target_schema_objects_precutover", targetSchemaObjectsCheck, ScopePreCutover)
}

// targetObjectKinds are the object kinds refused on a target: triggers and
// events (indexes into schemaObjectKinds). They run on their own on the
// target. Views, procedures and functions run only when something invokes
// them, and move never does.
var targetObjectKinds = []int{0, 4}

// targetObjectVisibilityKinds is the visibility the target check needs for
// targetObjectKinds.
var targetObjectVisibilityKinds = []schemaObject{schemaTriggers, schemaEvents}

func targetSchemaObjectsCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	return TargetSchemaObjectsError(ctx, r.Targets, r.SourceTables)
}

// TargetSchemaObjectsError returns a refusal (see ErrRefused) listing, for
// every target, each trigger on a table move writes to and each event in the
// target schema, grouped by target, or nil.
//
// Move writes to the moved tables on every target, and to its checkpoint table
// on targets[0]. A pre-created target table may carry a trigger (target_state
// accepts an empty table whose definition matches the source), and a trigger
// fires once for every copied row and again for every replayed change, so its
// writes are applied twice or more. An event runs on its own schedule and can
// write to the moved tables. A trigger on another table is not refused: it
// fires only when something else writes to that table.
//
// Table names are compared the way the target compares them: case-insensitively
// when its lower_case_table_names is nonzero, and exactly when it is 0.
//
// information_schema only shows triggers and events the user has the TRIGGER
// and EVENT privilege on, so every call first checks that the user has both on
// the target schema, and refuses if not (see targetObjectVisibility).
//
// --force does not bypass the refusal: the runner runs this check before it
// wipes the target, and the wipe never drops events.
func TargetSchemaObjectsError(ctx context.Context, targets []applier.Target, tables []*table.TableInfo) error {
	var groups []string
	for i, target := range targets {
		if target.DB == nil || target.Config == nil {
			return fmt.Errorf("target %d database connection or config is not initialized", i)
		}
		schema := target.Config.DBName
		if err := targetObjectVisibility(ctx, target.DB, schema); err != nil {
			return fmt.Errorf("target %d (%s): %w", i, schema, err)
		}
		var lowerCaseTableNames int
		if err := target.DB.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
			return fmt.Errorf("failed to read lower_case_table_names on target %d (%s): %w", i, schema, err)
		}
		written := make([]string, 0, len(tables)+1)
		for _, t := range tables {
			written = append(written, t.TableName)
		}
		if i == 0 {
			written = append(written, moveCheckpointTableName)
		}
		objects, err := schemaObjects(ctx, target.DB, schema, targetObjectKinds)
		if err != nil {
			return fmt.Errorf("failed to list triggers and events on target %d (%s): %w", i, schema, err)
		}
		if found := refusedTargetObjects(objects, written, lowerCaseTableNames); len(found) > 0 {
			groups = append(groups, fmt.Sprintf("target %d (%s): %s", i, schema, strings.Join(found, ", ")))
		}
	}
	if len(groups) == 0 {
		return nil
	}
	return refuse(fmt.Errorf("cannot move: triggers on the tables move writes to on the target, and events in the target schema, run on their own and can write to the moved tables; they must be dropped before the move can continue: %s",
		strings.Join(groups, "; ")))
}

// refusedTargetObjects describes the objects the target check refuses: every
// event, and every trigger on a table in written. Table names are folded to
// lower case when lowerCaseTableNames is nonzero.
func refusedTargetObjects(objects []foundObject, written []string, lowerCaseTableNames int) []string {
	fold := func(name string) string {
		if lowerCaseTableNames != 0 {
			return strings.ToLower(name)
		}
		return name
	}
	writes := make(map[string]bool, len(written))
	for _, name := range written {
		writes[fold(name)] = true
	}
	var found []string
	for _, o := range objects {
		if o.kind == "trigger" && !writes[fold(o.onTable)] {
			continue
		}
		found = append(found, o.String())
	}
	return found
}
