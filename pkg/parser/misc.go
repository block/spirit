// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentChar(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch byte) bool {
	return ch >= 0x80
}

// See https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
func isInCorrectIdentifierName(name string) bool {
	if len(name) == 0 {
		return true
	}
	if name[len(name)-1] == ' ' {
		return true
	}
	return false
}

// Initialize a lookup table for isUserVarChar
var isUserVarCharTable [256]bool

func init() {
	for i := range 256 {
		ch := byte(i)
		isUserVarCharTable[i] = isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || ch == '.' || isIdentExtend(ch)
	}
}

func isUserVarChar(ch byte) bool {
	return isUserVarCharTable[ch]
}

type trieNode struct {
	childs [256]*trieNode
	token  int
	fn     func(s *Scanner) (int, Pos, string)
}

var ruleTable trieNode

func initTokenByte(c byte, tok int) {
	if ruleTable.childs[c] == nil {
		ruleTable.childs[c] = &trieNode{}
	}
	ruleTable.childs[c].token = tok
}

func initTokenString(str string, tok int) {
	node := &ruleTable
	for _, c := range str {
		if node.childs[c] == nil {
			node.childs[c] = &trieNode{}
		}
		node = node.childs[c]
	}
	node.token = tok
}

func initTokenFunc(str string, fn func(s *Scanner) (int, Pos, string)) {
	for i := range len(str) {
		c := str[i]
		if ruleTable.childs[c] == nil {
			ruleTable.childs[c] = &trieNode{}
		}
		ruleTable.childs[c].fn = fn
	}
}

func init() {
	// invalid is a special token defined in parser.y, when parser meet
	// this token, it will throw an error.
	// set root trie node's token to invalid, so when input match nothing
	// in the trie, invalid will be the default return token.
	ruleTable.token = invalid
	initTokenByte('/', int('/'))
	initTokenByte('+', int('+'))
	initTokenByte('>', int('>'))
	initTokenByte('<', int('<'))
	initTokenByte('(', int('('))
	initTokenByte(')', int(')'))
	initTokenByte('[', int('['))
	initTokenByte(']', int(']'))
	initTokenByte(';', int(';'))
	initTokenByte(',', int(','))
	initTokenByte('&', int('&'))
	initTokenByte('%', int('%'))
	initTokenByte(':', int(':'))
	initTokenByte('|', int('|'))
	initTokenByte('!', int('!'))
	initTokenByte('^', int('^'))
	initTokenByte('~', int('~'))
	initTokenByte('\\', int('\\'))
	initTokenByte('?', paramMarker)
	initTokenByte('=', eq)
	initTokenByte('{', int('{'))
	initTokenByte('}', int('}'))

	initTokenString("||", pipes)
	initTokenString("&&", andand)
	initTokenString(":=", assignmentEq)
	initTokenString("<=>", nulleq)
	initTokenString(">=", ge)
	initTokenString("<=", le)
	initTokenString("!=", neq)
	initTokenString("<>", neqSynonym)
	initTokenString("<<", lsh)
	initTokenString(">>", rsh)
	initTokenString("\\N", null)

	initTokenFunc("@", startWithAt)
	initTokenFunc("/", startWithSlash)
	initTokenFunc("*", startWithStar)
	initTokenFunc("-", startWithDash)
	initTokenFunc("#", startWithSharp)
	initTokenFunc("Xx", startWithXx)
	initTokenFunc("Nn", startWithNn)
	initTokenFunc("Bb", startWithBb)
	initTokenFunc(".", startWithDot)
	initTokenFunc("_ACDEFGHIJKLMOPQRSTUVWYZacdefghijklmopqrstuvwyz", scanIdentifier)
	initTokenFunc("$", startWithDollar)
	initTokenFunc("`", scanQuotedIdent)
	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", startString)
}

// isInTokenMap indicates whether the target string is contained in tokenMap.
func isInTokenMap(target string) bool {
	_, ok := tokenMap[target]
	return ok
}

// tokenMap is a map of known identifiers to the parser token ID.
// Please try to keep the map in alphabetical order.
var tokenMap = map[string]int{
	"ACCOUNT":                    account,
	"ACTION":                     action,
	"ACTIVE":                     active,
	"ADMIN":                      admin,
	"ADD":                        add,
	"ADDDATE":                    addDate,
	"AFTER":                      after,
	"AGAINST":                    against,
	"ALGORITHM":                  algorithm,
	"ALL":                        all,
	"ALTER":                      alter,
	"ALWAYS":                     always,
	"ANALYZE":                    analyze,
	"AND":                        and,
	"ANY":                        any,
	"ARRAY":                      array,
	"AS":                         as,
	"ASC":                        asc,
	"ASCII":                      ascii,
	"AT":                         at,
	"ATTRIBUTE":                  attribute,
	"AUTHENTICATION":             authentication,
	"AUTO":                       auto,
	"AUTO_INCREMENT":             autoIncrement,
	"AUTOEXTEND_SIZE":            autoextendSize,
	"AVG_ROW_LENGTH":             avgRowLength,
	"AVG":                        avg,
	"BACKUP":                     backup,
	"BEFORE":                     before,
	"BEGIN":                      begin,
	"BETWEEN":                    between,
	"BIGINT":                     bigIntType,
	"BINARY":                     binaryType,
	"BINLOG":                     binlog,
	"BIT":                        bitType,
	"BLOB":                       blobType,
	"BLOCK":                      block,
	"BOOL":                       boolType,
	"BOOLEAN":                    booleanType,
	"BOTH":                       both,
	"BTREE":                      btree,
	"BUCKETS":                    buckets,
	"BY":                         by,
	"BYTE":                       byteType,
	"CACHE":                      cache,
	"CALL":                       call,
	"CASCADE":                    cascade,
	"CASCADED":                   cascaded,
	"CASE":                       caseKwd,
	"CHAIN":                      chain,
	"CHALLENGE_RESPONSE":         challengeResponse,
	"CHANGE":                     change,
	"CHANGED":                    changed,
	"CHANNEL":                    channel,
	"CHAR":                       charType,
	"CHARACTER":                  character,
	"CHARSET":                    charsetKwd,
	"CHECK":                      check,
	"CHECKSUM":                   checksum,
	"CIPHER":                     cipher,
	"CLIENT":                     client,
	"CLONE":                      clone,
	"CLOSE":                      closeKwd,
	"COALESCE":                   coalesce,
	"COLLATE":                    collate,
	"CODE":                       code,
	"COLLATION":                  collation,
	"COLUMN_FORMAT":              columnFormat,
	"COLUMN":                     column,
	"COLUMNS":                    columns,
	"COMMENT":                    comment,
	"COMMIT":                     commit,
	"COMMITTED":                  committed,
	"COMPACT":                    compact,
	"COMPONENT":                  component,
	"COMPRESS":                   compress,
	"COMPRESSED":                 compressed,
	"COMPRESSION":                compression,
	"CONCURRENT":                 concurrent,
	"CONDITION":                  condition,
	"CONFIG":                     config,
	"CONNECTION":                 connection,
	"CONSISTENT":                 consistent,
	"CONSTRAINT":                 constraint,
	"CONTEXT":                    context,
	"CONVERT":                    convert,
	"COPY":                       copyKwd,
	"CPU":                        cpu,
	"CREATE":                     create,
	"CROSS":                      cross,
	"CURRENT_DATE":               currentDate,
	"CURRENT_ROLE":               currentRole,
	"CURRENT_TIME":               currentTime,
	"CURRENT_TIMESTAMP":          currentTs,
	"CURRENT_USER":               currentUser,
	"CURRENT":                    current,
	"DATA":                       data,
	"DATABASE":                   database,
	"DATABASES":                  databases,
	"DATAFILE":                   datafile,
	"DATE":                       dateType,
	"DATETIME":                   datetimeType,
	"DAY_HOUR":                   dayHour,
	"DAY_MICROSECOND":            dayMicrosecond,
	"DAY_MINUTE":                 dayMinute,
	"DAY_SECOND":                 daySecond,
	"DAY":                        day,
	"DEALLOCATE":                 deallocate,
	"DEC":                        decimalType,
	"DECIMAL":                    decimalType,
	"DEFAULT":                    defaultKwd,
	"DEFINED":                    defined,
	"DEFAULT_AUTH":               defaultAuth,
	"DEFINER":                    definer,
	"DEFINITION":                 definition,
	"DELAY_KEY_WRITE":            delayKeyWrite,
	"DELAYED":                    delayed,
	"DELETE":                     deleteKwd,
	"DESC":                       desc,
	"DESCRIBE":                   describe,
	"DESCRIPTION":                description,
	"DIAGNOSTICS":                diagnostics,
	"DIRECTORY":                  directory,
	"DISABLE":                    disable,
	"DISCARD":                    discard,
	"DISK":                       disk,
	"DISTINCT":                   distinct,
	"DISTINCTROW":                distinct,
	"DIV":                        div,
	"DO":                         do,
	"DOUBLE":                     doubleType,
	"DROP":                       drop,
	"DUAL":                       dual,
	"DUMPFILE":                   dumpfile,
	"DUPLICATE":                  duplicate,
	"DYNAMIC":                    dynamic,
	"ELSE":                       elseKwd,
	"EMPTY":                      empty,
	"ENABLE":                     enable,
	"ENCLOSED":                   enclosed,
	"ENCRYPTION":                 encryption,
	"END":                        end,
	"ENFORCED":                   enforced,
	"ENGINE":                     engine,
	"ENGINES":                    engines,
	"ENGINE_ATTRIBUTE":           engine_attribute,
	"EXPORT":                     export,
	"EXTENT_SIZE":                extentSize,
	"FILE_BLOCK_SIZE":            fileBlockSize,
	"FINISH":                     finish,
	"GET":                        get,
	"GROUPING":                   grouping,
	"HELP":                       help,
	"INACTIVE":                   inactive,
	"INITIAL_SIZE":               initialSize,
	"INITIATE":                   initiate,
	"KEYRING":                    keyring,
	"LOGFILE":                    logfile,
	"MANUAL":                     manual,
	"MAX_SIZE":                   maxSize,
	"MIGRATE":                    migrate,
	"MUTEX":                      mutex,
	"NAME":                       nameKwd,
	"ONE":                        one,
	"OPTIMIZER_COSTS":            optimizerCosts,
	"ORGANIZATION":               organization,
	"PHASE":                      phase,
	"QUALIFY":                    qualify,
	"RECOVER":                    recover,
	"REFERENCE":                  reference,
	"REGISTRATION":               registration,
	"RELAY":                      relay,
	"RELAYLOG":                   relaylog,
	"DUALITY":                    duality,
	"RELATIONAL":                 relational,
	"DECLARE":                    declare,
	"DETERMINISTIC":              deterministic,
	"CONTINUE":                   continueKwd,
	"CURSOR":                     cursor,
	"EACH":                       each,
	"ELSEIF":                     elseifKwd,
	"EXIT":                       exit,
	"INOUT":                      inout,
	"ITERATE":                    iterate,
	"LEAVE":                      leave,
	"LOOP":                       loop,
	"MODIFIES":                   modifies,
	"OUT":                        out,
	"READS":                      reads,
	"RESIGNAL":                   resignal,
	"RETURN":                     returnKwd,
	"SIGNAL":                     signal,
	"SQLEXCEPTION":               sqlexception,
	"SQLSTATE":                   sqlstate,
	"SQLWARNING":                 sqlwarning,
	"WHILE":                      while,
	"RETURNS":                    returns,
	"CONTAINS":                   contains,
	"FOLLOWS":                    follows,
	"PRECEDES":                   precedes,
	"SCHEDULE":                   schedule,
	"COMPLETION":                 completion,
	"PRESERVE":                   preserve,
	"STARTS":                     starts,
	"ENDS":                       ends,
	"EVERY":                      every,
	"STRING":                     stringKwd,
	"AGGREGATE":                  aggregate,
	"RESUME":                     resume,
	"ROTATE":                     rotate,
	"SECONDARY_ENGINE_ATTRIBUTE": secondaryEngineAttribute,
	"ENUM":                       enum,
	"ERROR":                      errorKwd,
	"ERRORS":                     identSQLErrors,
	"ESCAPE":                     escape,
	"ESCAPED":                    escaped,
	"EVENT":                      event,
	"EVENTS":                     events,
	"EXCEPT":                     except,
	"EXCHANGE":                   exchange,
	"EXECUTE":                    execute,
	"EXISTS":                     exists,
	"EXPANSION":                  expansion,
	"EXPIRE":                     expire,
	"EXPLAIN":                    explain,
	"EXTENDED":                   extended,
	"FACTOR":                     factor,
	"EXPLORE":                    explore,
	"FALSE":                      falseKwd,
	"FAST":                       fast,
	"FAULTS":                     faultsSym,
	"FETCH":                      fetch,
	"FIELDS":                     fields,
	"FILE":                       file,
	"FILTER":                     filter,
	"FIRST":                      first,
	"FIXED":                      fixed,
	"FLOAT":                      floatType,
	"FLOAT4":                     float4Type,
	"FLOAT8":                     float8Type,
	"FLUSH":                      flush,
	"FOLLOWING":                  following,
	"FOR":                        forKwd,
	"FORCE":                      force,
	"FOREIGN":                    foreign,
	"FORMAT":                     format,
	"FOUND":                      found,
	"FROM":                       from,
	"FULL":                       full,
	"FULLTEXT":                   fulltext,
	"FUNCTION":                   function,
	"GENERAL":                    general,
	"GENERATED":                  generated,
	"GEOMETRY":                   geometry,
	"GEOMCOLLECTION":             geomCollection,
	"GEOMETRYCOLLECTION":         geometryCollection,
	"GET_FORMAT":                 getFormat,
	"GLOBAL":                     global,
	"GRANT":                      grant,
	"GRANTS":                     grants,
	"GROUP":                      group,
	"GROUP_REPLICATION":          groupReplication,
	"GTIDS":                      gtids,
	"HANDLER":                    handler,
	"HASH":                       hash,
	"HAVING":                     having,
	"HIGH_PRIORITY":              highPriority,
	"HISTORY":                    history,
	"HISTOGRAM":                  histogram,
	"HOST":                       host,
	"HOSTS":                      hosts,
	"HOUR_MICROSECOND":           hourMicrosecond,
	"HOUR_MINUTE":                hourMinute,
	"HOUR_SECOND":                hourSecond,
	"HOUR":                       hour,
	"IDENTIFIED":                 identified,
	"IF":                         ifKwd,
	"IGNORE":                     ignore,
	"IMPORT":                     importKwd,
	"IN":                         in,
	"INDEX":                      index,
	"INDEXES":                    indexes,
	"INITIAL":                    initial,
	"INFILE":                     infile,
	"INNER":                      inner,
	"INPLACE":                    inplace,
	"INSERT_METHOD":              insertMethod,
	"INSERT":                     insert,
	"INSTALL":                    install,
	"INSTANCE":                   instance,
	"INSTANT":                    instant,
	"INT":                        intType,
	"INT1":                       int1Type,
	"INT2":                       int2Type,
	"INT3":                       int3Type,
	"INT4":                       int4Type,
	"INT8":                       int8Type,
	"INTEGER":                    integerType,
	"INTERSECT":                  intersect,
	"INTERVAL":                   interval,
	"INTO":                       into,
	"INVISIBLE":                  invisible,
	"INVOKER":                    invoker,
	"IO":                         io,
	"IO_THREAD":                  ioThread,
	"IPC":                        ipc,
	"IS":                         is,
	"ISOLATION":                  isolation,
	"ISSUER":                     issuer,
	"JOIN":                       join,
	"JSON_ARRAYAGG":              jsonArrayagg,
	"JSON_OBJECTAGG":             jsonObjectAgg,
	"JSON_SUM_CRC32":             jsonSumCrc32,
	"JSON_VALUE":                 jsonValue,
	"JSON":                       jsonType,
	"JSON_TABLE":                 jsonTable,
	"KEY_BLOCK_SIZE":             keyBlockSize,
	"KEY":                        key,
	"KEYS":                       keys,
	"KILL":                       kill,
	"LANGUAGE":                   language,
	"LAST":                       last,
	"LATERAL":                    lateral,
	"LEADING":                    leading,
	"LEAVES":                     leaves,
	"LEFT":                       left,
	"LESS":                       less,
	"LEVEL":                      level,
	"LIBRARY":                    library,
	"LIKE":                       like,
	"LIMIT":                      limit,
	"LINEAR":                     linear,
	"LINES":                      lines,
	"LINESTRING":                 lineString,
	"LIST":                       list,
	"LOAD":                       load,
	"LOCAL":                      local,
	"LOCALTIME":                  localTime,
	"LOCALTIMESTAMP":             localTs,
	"LOCK":                       lock,
	"LOCKED":                     locked,
	"LOG":                        log,
	"LOGS":                       logs,
	"LONG":                       long,
	"LONGBLOB":                   longblobType,
	"LONGTEXT":                   longtextType,
	"LOW_PRIORITY":               lowPriority,
	"MASTER":                     master,
	"MATCH":                      match,
	"MAX_CONNECTIONS_PER_HOUR":   maxConnectionsPerHour,
	"MAX_QUERIES_PER_HOUR":       maxQueriesPerHour,
	"MAX_ROWS":                   maxRows,
	"MAX_UPDATES_PER_HOUR":       maxUpdatesPerHour,
	"MAX_USER_CONNECTIONS":       maxUserConnections,
	"MAXVALUE":                   maxValue,
	"MEDIUM":                     medium,
	"MEDIUMBLOB":                 mediumblobType,
	"MEDIUMINT":                  mediumIntType,
	"MEDIUMTEXT":                 mediumtextType,
	"MEMORY":                     memory,
	"MEMBER":                     member,
	"MERGE":                      merge,
	"MICROSECOND":                microsecond,
	"MIDDLEINT":                  middleIntType,
	"MIN_ROWS":                   minRows,
	"MINUTE_MICROSECOND":         minuteMicrosecond,
	"MINUTE_SECOND":              minuteSecond,
	"MINUTE":                     minute,
	"MOD":                        mod,
	"MODE":                       mode,
	"MODIFY":                     modify,
	"MONTH":                      month,
	"MULTILINESTRING":            multiLineString,
	"MULTIPOINT":                 multiPoint,
	"MULTIPOLYGON":               multiPolygon,
	"NAMES":                      names,
	"NATIONAL":                   national,
	"NATURAL":                    natural,
	"NCHAR":                      ncharType,
	"NESTED":                     nested,
	"NEVER":                      never,
	"NEXT":                       next,
	"NO_WRITE_TO_BINLOG":         noWriteToBinLog,
	"NO":                         no,
	"NODEGROUP":                  nodegroup,
	"NONE":                       none,
	"NOT":                        not,
	"NOWAIT":                     nowait,
	"NO_WAIT":                    noWaitTablespace,
	"REDO_BUFFER_SIZE":           redoBufferSize,
	"SETS":                       sets,
	"SOUNDS":                     sounds,
	"STACKED":                    stacked,
	"UNDO_BUFFER_SIZE":           undoBufferSize,
	"NULL":                       null,
	"NULLS":                      nulls,
	"NUMERIC":                    numericType,
	"NVARCHAR":                   nvarcharType,
	"OF":                         of,
	"OFFSET":                     offset,
	"OLD":                        old,
	"ON":                         on,
	"ONLY":                       only,
	"OPEN":                       open,
	"OPTIONAL":                   optional,
	"OPTIMIZE":                   optimize,
	"OPTION":                     option,
	"OPTIONALLY":                 optionally,
	"OPTIONS":                    options,
	"OR":                         or,
	"ORDER":                      order,
	"ORDINALITY":                 ordinality,
	"OUTER":                      outer,
	"OUTFILE":                    outfile,
	"OWNER":                      owner,
	"PACK_KEYS":                  packKeys,
	"PAGE":                       pageSym,
	"PARSE_TREE":                 parseTree,
	"PARSER":                     parser,
	"PARTIAL":                    partial,
	"PARTITION":                  partition,
	"PARTITIONING":               partitioning,
	"PARTITIONS":                 partitions,
	"PASSWORD":                   password,
	"PATH":                       path,
	"PERSIST":                    persist,
	"PERSIST_ONLY":               persistOnly,
	"PLUGIN":                     plugin,
	"PLUGIN_DIR":                 pluginDir,
	"PLUGINS":                    plugins,
	"POINT":                      point,
	"POLYGON":                    polygon,
	"PORT":                       port,
	"PRECEDING":                  preceding,
	"PRECISION":                  precisionType,
	"PREPARE":                    prepare,
	"PREV":                       prev,
	"PRIMARY":                    primary,
	"PRIVILEGES":                 privileges,
	"PROCEDURE":                  procedure,
	"PROCESS":                    process,
	"PROCESSLIST":                processlist,
	"PROFILE":                    profile,
	"PROFILES":                   profiles,
	"PROXY":                      proxy,
	"PURGE":                      purge,
	"QUARTER":                    quarter,
	"QUERY":                      query,
	"QUICK":                      quick,
	"RANDOM":                     random,
	"RANGE":                      rangeKwd,
	"READ":                       read,
	"REAL":                       realType,
	"REBUILD":                    rebuild,
	"RECURSIVE":                  recursive,
	"REDUNDANT":                  redundant,
	"REFERENCES":                 references,
	"REGEXP":                     regexpKwd,
	"RELEASE":                    release,
	"RELOAD":                     reload,
	"REMOVE":                     remove,
	"RENAME":                     rename,
	"REORGANIZE":                 reorganize,
	"REPAIR":                     repair,
	"REPEAT":                     repeat,
	"REPEATABLE":                 repeatable,
	"REPLACE":                    replace,
	"REPLICA":                    replica,
	"REPLICAS":                   replicas,
	"REPLICATION":                replication,
	"REQUIRE":                    requireKwd,
	"RESOURCE":                   resource,
	"RESPECT":                    respect,
	"RESET":                      reset,
	"RESTART":                    restart,
	"RETAIN":                     retain,
	"RESTRICT":                   restrict,
	"REVERSE":                    reverse,
	"REVOKE":                     revoke,
	"RIGHT":                      right,
	"RLIKE":                      rlike,
	"ROLE":                       role,
	"ROLLBACK":                   rollback,
	"ROLLUP":                     rollup,
	"ROUTINE":                    routine,
	"ROW_COUNT":                  rowCount,
	"ROW_FORMAT":                 rowFormat,
	"ROW":                        row,
	"ROWS":                       rows,
	"RTREE":                      rtree,
	"S3":                         s3,
	"SAN":                        san,
	"SAVEPOINT":                  savepoint,
	"SCHEMA":                     database,
	"SCHEMAS":                    databases,
	"SECOND_MICROSECOND":         secondMicrosecond,
	"SECOND":                     second,
	"SECONDARY":                  secondary,
	"SECONDARY_ENGINE":           secondaryEngine,
	"SECONDARY_LOAD":             secondaryLoad,
	"SECONDARY_UNLOAD":           secondaryUnload,
	"SECURITY":                   security,
	"SELECT":                     selectKwd,
	"SEPARATOR":                  separator,
	"SERIAL":                     serial,
	"SERIALIZABLE":               serializable,
	"SERVER":                     server,
	"SESSION":                    session,
	"SET":                        set,
	"SHARE":                      share,
	"SHOW":                       show,
	"SHUTDOWN":                   shutdown,
	"SIGNED":                     signed,
	"SIMPLE":                     simple,
	"SKIP":                       skip,
	"SLAVE":                      slave,
	"SLOW":                       slow,
	"SMALLINT":                   smallIntType,
	"SNAPSHOT":                   snapshot,
	"SOCKET":                     socket,
	"SOME":                       some,
	"SONAME":                     soname,
	"SOURCE":                     source,
	"SPATIAL":                    spatial,
	"SQL_BIG_RESULT":             sqlBigResult,
	"SQL_BUFFER_RESULT":          sqlBufferResult,
	"SQL_CACHE":                  sqlCache,
	"SQL_CALC_FOUND_ROWS":        sqlCalcFoundRows,
	"SQL_NO_CACHE":               sqlNoCache,
	"SQL_SMALL_RESULT":           sqlSmallResult,
	"SQL_THREAD":                 sqlThread,
	"SQL_TSI_DAY":                sqlTsiDay,
	"SQL_TSI_HOUR":               sqlTsiHour,
	"SQL_TSI_MINUTE":             sqlTsiMinute,
	"SQL_TSI_MONTH":              sqlTsiMonth,
	"SQL_TSI_QUARTER":            sqlTsiQuarter,
	"SQL_TSI_SECOND":             sqlTsiSecond,
	"SQL_TSI_WEEK":               sqlTsiWeek,
	"SQL_TSI_YEAR":               sqlTsiYear,
	"SQL":                        sql,
	"SRID":                       srid,
	"SSL":                        ssl,
	"START":                      start,
	"STARTING":                   starting,
	"STATS_AUTO_RECALC":          statsAutoRecalc,
	"STATS_PERSISTENT":           statsPersistent,
	"STATS_SAMPLE_PAGES":         statsSamplePages,
	"STATUS":                     status,
	"STOP":                       stop,
	"STORAGE":                    storage,
	"STORED":                     stored,
	"STRAIGHT_JOIN":              straightJoin,
	"SUBDATE":                    subDate,
	"SUBJECT":                    subject,
	"SUBPARTITION":               subpartition,
	"SUBPARTITIONS":              subpartitions,
	"SUPER":                      super,
	"SUSPEND":                    suspend,
	"SWAPS":                      swaps,
	"SWITCHES":                   switchesSym,
	"SYSTEM":                     system,
	"TABLE":                      tableKwd,
	"TABLES":                     tables,
	"TABLESPACE":                 tablespace,
	"TEMPORARY":                  temporary,
	"TEMPTABLE":                  temptable,
	"TERMINATED":                 terminated,
	"TEXT":                       textType,
	"THAN":                       than,
	"THEN":                       then,
	"THREAD_PRIORITY":            threadPriority,
	"TIME":                       timeType,
	"TIMESTAMP":                  timestampType,
	"TIMESTAMPADD":               timestampAdd,
	"TIMESTAMPDIFF":              timestampDiff,
	"TINYBLOB":                   tinyblobType,
	"TINYINT":                    tinyIntType,
	"TINYTEXT":                   tinytextType,
	"TLS":                        tls,
	"TO":                         to,
	"TOKEN_ISSUER":               tokenIssuer,
	"TRADITIONAL":                traditional,
	"TRAILING":                   trailing,
	"TRANSACTION":                transaction,
	"TRIGGER":                    trigger,
	"TREE":                       tree,
	"TRIGGERS":                   triggers,
	"TRUE":                       trueKwd,
	"TRUNCATE":                   truncate,
	"TYPE":                       tp,
	"UNBOUNDED":                  unbounded,
	"UNCOMMITTED":                uncommitted,
	"UNDEFINED":                  undefined,
	"UNDO":                       undo,
	"UNDOFILE":                   undofile,
	"UNICODE":                    unicodeSym,
	"UNINSTALL":                  uninstall,
	"UNION":                      union,
	"UNIQUE":                     unique,
	"UNKNOWN":                    unknown,
	"UNREGISTER":                 unregister,
	"UNLOCK":                     unlock,
	"UNSIGNED":                   unsigned,
	"UNTIL":                      until,
	"UPDATE":                     update,
	"UPGRADE":                    upgrade,
	"USAGE":                      usage,
	"USE":                        use,
	"USER":                       user,
	"USER_RESOURCES":             userResources,
	"USE_FRM":                    useFrm,
	"USING":                      using,
	"UTC_DATE":                   utcDate,
	"UTC_TIME":                   utcTime,
	"UTC_TIMESTAMP":              utcTimestamp,
	"VALIDATION":                 validation,
	"VALUE":                      value,
	"VALUES":                     values,
	"VARBINARY":                  varbinaryType,
	"VARCHAR":                    varcharType,
	"VARCHARACTER":               varcharacter,
	"VARIABLES":                  variables,
	"VARYING":                    varying,
	"VCPU":                       vcpu,
	"VECTOR":                     vectorType,
	"VIEW":                       view,
	"VIRTUAL":                    virtual,
	"VISIBLE":                    visible,
	"WARNINGS":                   warnings,
	"WEEK":                       week,
	"WEIGHT_STRING":              weightString,
	"WHEN":                       when,
	"WHERE":                      where,
	"WITH":                       with,
	"WITHOUT":                    without,
	"WORK":                       work,
	"WRAPPER":                    wrapper,
	"WRITE":                      write,
	"X509":                       x509,
	"XA":                         xa,
	"XID":                        xid,
	"XML":                        xml,
	"XOR":                        xor,
	"YEAR_MONTH":                 yearMonth,
	"YEAR":                       yearType,
	"ZEROFILL":                   zerofill,
	"WAIT":                       wait,
	"FAILED_LOGIN_ATTEMPTS":      failedLoginAttempts,
	"PASSWORD_LOCK_TIME":         passwordLockTime,
	"RETURNING":                  returning,
	"REUSE":                      reuse,
	"ZONE":                       zone,
}

// See https://dev.mysql.com/doc/refman/8.0/en/function-resolution.html for details.
// ADDDATE, SESSION_USER, SUBDATE, and SYSTEM_USER are exceptions because they are actually recognized as
// identifiers even in `create table adddate (a int)`.
var btFuncTokenMap = map[string]int{
	"BIT_AND":               builtinBitAnd,
	"BIT_OR":                builtinBitOr,
	"BIT_XOR":               builtinBitXor,
	"CAST":                  builtinCast,
	"COUNT":                 builtinCount,
	"APPROX_COUNT_DISTINCT": builtinApproxCountDistinct,
	"APPROX_PERCENTILE":     builtinApproxPercentile,
	"CURDATE":               builtinCurDate,
	"CURTIME":               builtinCurTime,
	"DATE_ADD":              builtinDateAdd,
	"DATE_SUB":              builtinDateSub,
	"EXTRACT":               builtinExtract,
	"GROUP_CONCAT":          builtinGroupConcat,
	"JSON_DUALITY_OBJECT":   builtinJSONDualityObject,
	"MAX":                   builtinMax,
	"MID":                   builtinSubstring,
	"MIN":                   builtinMin,
	"NOW":                   builtinNow,
	"POSITION":              builtinPosition,
	"STD":                   builtinStddevPop,
	"STDDEV":                builtinStddevPop,
	"STDDEV_POP":            builtinStddevPop,
	"STDDEV_SAMP":           builtinStddevSamp,
	"ST_COLLECT":            builtinStCollect,
	"SUBSTR":                builtinSubstring,
	"SUBSTRING":             builtinSubstring,
	"SUM":                   builtinSum,
	"SYSDATE":               builtinSysDate,
	"TRANSLATE":             builtinTranslate,
	"TRIM":                  builtinTrim,
	"VARIANCE":              builtinVarPop,
	"VAR_POP":               builtinVarPop,
	"VAR_SAMP":              builtinVarSamp,
}

var windowFuncTokenMap = map[string]int{
	"CUME_DIST":    cumeDist,
	"DENSE_RANK":   denseRank,
	"FIRST_VALUE":  firstValue,
	"GROUPS":       groups,
	"LAG":          lag,
	"LAST_VALUE":   lastValue,
	"LEAD":         lead,
	"NTH_VALUE":    nthValue,
	"NTILE":        ntile,
	"OVER":         over,
	"PERCENT_RANK": percentRank,
	"RANK":         rank,
	"ROW_NUMBER":   rowNumber,
	"WINDOW":       window,
}

// aliases are strings directly map to another string and use the same token.
var aliases = map[string]string{
	"SCHEMA":  "DATABASE",
	"SCHEMAS": "DATABASES",
	"DEC":     "DECIMAL",
}

// hintedTokens is a set of tokens which recognizes a hint.
// According to https://dev.mysql.com/doc/refman/8.0/en/optimizer-hints.html,
// only SELECT, INSERT, REPLACE, UPDATE and DELETE accept optimizer hints.
// A /*+ ... */ anywhere else is warned about and skipped as a comment,
// matching MySQL, rather than failing the parse.
var hintedTokens = map[int]struct{}{
	selectKwd: {},
	insert:    {},
	replace:   {},
	update:    {},
	deleteKwd: {},
}

var hintTokenMap = map[string]int{
	// MySQL 8.0 hint names
	"JOIN_FIXED_ORDER":      hintJoinFixedOrder,
	"JOIN_ORDER":            hintJoinOrder,
	"JOIN_PREFIX":           hintJoinPrefix,
	"JOIN_SUFFIX":           hintJoinSuffix,
	"BKA":                   hintBKA,
	"NO_BKA":                hintNoBKA,
	"BNL":                   hintBNL,
	"NO_BNL":                hintNoBNL,
	"HASH_JOIN":             hintHashJoin,
	"NO_HASH_JOIN":          hintNoHashJoin,
	"MERGE":                 hintMerge,
	"NO_MERGE":              hintNoMerge,
	"INDEX_MERGE":           hintIndexMerge,
	"NO_INDEX_MERGE":        hintNoIndexMerge,
	"MRR":                   hintMRR,
	"NO_MRR":                hintNoMRR,
	"NO_ICP":                hintNoICP,
	"NO_RANGE_OPTIMIZATION": hintNoRangeOptimization,
	"SKIP_SCAN":             hintSkipScan,
	"NO_SKIP_SCAN":          hintNoSkipScan,
	"SEMIJOIN":              hintSemijoin,
	"NO_SEMIJOIN":           hintNoSemijoin,
	"ORDER_INDEX":           hintOrderIndex,
	"NO_ORDER_INDEX":        hintNoOrderIndex,
	"MAX_EXECUTION_TIME":    hintMaxExecutionTime,
	"SET_VAR":               hintSetVar,
	"RESOURCE_GROUP":        hintResourceGroup,
	"QB_NAME":               hintQBName,

	// SEMIJOIN() strategies
	"DUPSWEEDOUT":     hintDupsWeedOut,
	"FIRSTMATCH":      hintFirstMatch,
	"LOOSESCAN":       hintLooseScan,
	"MATERIALIZATION": hintMaterialization,
}

func (s *Scanner) isTokenIdentifier(lit string, offset int) int {
	// An identifier before or after '.' means it is part of a qualified identifier.
	// We do not parse it as keyword.
	if s.r.peek() == '.' {
		return 0
	}

	for idx := offset - 1; idx >= 0; idx-- {
		if s.r.s[idx] == ' ' {
			continue
		} else if s.r.s[idx] == '.' {
			return 0
		}
		break
	}

	buf := &s.buf
	buf.Reset()
	buf.Grow(len(lit))
	data := buf.Bytes()[:len(lit)]

	for i := range len(lit) {
		c := lit[i]
		if c >= 'a' && c <= 'z' {
			data[i] = c + 'A' - 'a'
		} else {
			data[i] = c
		}
	}

	checkBtFuncToken := s.r.peek() == '('
	if !checkBtFuncToken && s.sqlMode.HasIgnoreSpaceMode() {
		s.skipWhitespace()
		checkBtFuncToken = s.r.peek() == '('
	}

	if checkBtFuncToken {
		if tok := btFuncTokenMap[string(data)]; tok != 0 {
			return tok
		}
	}
	tok, ok := tokenMap[string(data)]
	if !ok && s.supportWindowFunc {
		tok = windowFuncTokenMap[string(data)]
	}
	return tok
}
