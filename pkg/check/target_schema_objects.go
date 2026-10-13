package check

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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
	//
	// A sync runs its post-setup checks on every start, a fresh sync and a
	// resume, before it creates, drops or writes any target table, including
	// the --force wipe. It has no cutover, and does not check again while it
	// runs; the continuous checksum is the backstop for an object added later.
	register(check{name: "target_schema_objects", run: targetSchemaObjectsCheck, phases: PhasePostSetup | PhaseResume | PhaseCutoverLocked, runtimes: Move | Sync})
}

// targetObjectKinds are the object kinds refused on a target: triggers and
// events (indexes into schemaObjectKinds). They run on their own on the
// target. Views, procedures and functions run only when something invokes
// them, and neither move nor sync does.
var targetObjectKinds = []int{0, 4}

// targetObjectVisibilityKinds is the visibility the target check needs for
// targetObjectKinds.
var targetObjectVisibilityKinds = []schemaObject{schemaTriggers, schemaEvents}

func targetSchemaObjectsCheck(ctx context.Context, r Resources, _ *slog.Logger) error {
	return TargetSchemaObjectsError(ctx, r)
}

// TargetSchemaObjectsError returns a refusal (see ErrRefused) listing, for
// every target in r.Targets, each trigger on a table the run writes to and
// each event in the target schema, grouped by target, or nil.
//
// The run writes to r.SourceTables on every target, and to r.CheckpointTable
// on targets[0]. A pre-created target table may carry a trigger (move's
// target_state accepts an empty table whose definition matches the source,
// and a sync resumes onto tables that exist), and a trigger fires once for
// every copied row and again for every replayed change, so its writes are
// applied twice or more. An event runs on its own schedule and can write to
// the tables the run writes to. A trigger on another table is not refused: it
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
func TargetSchemaObjectsError(ctx context.Context, r Resources) error {
	var groups []string
	for i, target := range r.Targets {
		if target.DB == nil || target.Config == nil {
			return fmt.Errorf("target %d database connection or config is not initialized", i)
		}
		schema := target.Config.DBName
		if err := targetObjectVisibility(ctx, r.Runtime, target.DB, schema); err != nil {
			return fmt.Errorf("target %d (%s): %w", i, schema, err)
		}
		var lowerCaseTableNames int
		if err := target.DB.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
			return fmt.Errorf("failed to read lower_case_table_names on target %d (%s): %w", i, schema, err)
		}
		written := make([]string, 0, len(r.SourceTables)+1)
		for _, t := range r.SourceTables {
			written = append(written, t.TableName)
		}
		if i == 0 && r.CheckpointTable != "" {
			written = append(written, r.CheckpointTable)
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
	noun := r.Runtime.noun()
	return refuse(fmt.Errorf("cannot %s: triggers on the tables %s writes to on the target, and events in the target schema, run on their own and can write to the %s tables; they must be dropped before the %s can continue: %s",
		noun, noun, r.Runtime.participle(), noun, strings.Join(groups, "; ")))
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
