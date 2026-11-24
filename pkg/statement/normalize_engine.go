package statement

import "strings"

func init() { registerNormalizer(engineNormalizer{}) }

// engineNormalizer rewrites the table ENGINE option to the name MySQL stores.
// MySQL resolves an engine name case-insensitively and through a few legacy
// aliases, and SHOW CREATE TABLE prints the engine's own name: ENGINE=innodb
// and ENGINE=innobase are both reported as ENGINE=InnoDB, and ENGINE=heap as
// ENGINE=MEMORY. Without the rule, a schema that writes ENGINE=innodb differs
// from its own live table, and a diff with IgnoreEngine=false emits
// ENGINE=InnoDB on every run.
//
// A name that is not in engineCanonicalNames is left as written: MySQL rejects
// an unknown engine under the default sql_mode (NO_ENGINE_SUBSTITUTION), and
// the spelling of an engine that is not listed cannot be confirmed here.
// Per-partition ENGINE clauses are not rewritten, because Diff does not compare
// them (see partitionDefinitionEqual).
type engineNormalizer struct{}

// engineCanonicalNames maps a lowercased engine name or alias to the name SHOW
// CREATE TABLE reports for it. MRG_MyISAM is the spelling SHOW CREATE TABLE
// uses, which differs from the MRG_MYISAM in information_schema.ENGINES.
var engineCanonicalNames = map[string]string{
	"innodb":     "InnoDB",
	"innobase":   "InnoDB",
	"myisam":     "MyISAM",
	"memory":     "MEMORY",
	"heap":       "MEMORY",
	"mrg_myisam": "MRG_MyISAM",
	"merge":      "MRG_MyISAM",
	"csv":        "CSV",
	"archive":    "ARCHIVE",
	"blackhole":  "BLACKHOLE",
}

func (engineNormalizer) Name() string { return "engine" }

func (engineNormalizer) Normalize(ct *CreateTable) *CreateTable {
	engine := ct.TableOptions.getEngine()
	if engine == nil {
		return ct
	}
	if canonical, ok := engineCanonicalNames[strings.ToLower(*engine)]; ok {
		ct.TableOptions.Engine = &canonical
	}
	return ct
}
