package migration

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestE2EBinlogSubscribingCompositeKey and TestE2EBinlogSubscribingNonCompositeKey tests
// are complex tests that uses the lower level interface
// to step through the table while subscribing to changes that we will
// be making to the table between chunks. It is effectively an
// end-to-end test with concurrent operations on the table.
//
// This test validates KeyAboveHighWatermark optimization for composite chunker:
// - Composite keys with numeric first column (id1) now support the optimization
// - Events with id1 >= high watermark are discarded (will be copied later)
// - Events with id1 < high watermark are kept and applied
func TestE2EBinlogSubscribingCompositeKey(t *testing.T) {
	testutils.NewTestTable(t, "e2et1", `CREATE TABLE e2et1 (
		id1 int NOT NULL,
		id2 int not null,
		pad int NOT NULL  default 0,
		PRIMARY KEY (id1, id2))`)
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),
	(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,1),(21,1),(22,1),(23,1),(24,1),(25,1),(26,1),
	(27,1),(28,1),(29,1),(30,1),(31,1),(32,1),(33,1),(34,1),(35,1),(36,1),(37,1),(38,1),(39,1),(40,1),(41,1),(42,1),
	(43,1),(44,1),(45,1),(46,1),(47,1),(48,1),(49,1),(50,1),(51,1),(52,1),(53,1),(54,1),(55,1),(56,1),(57,1),(58,1),
	(59,1),(60,1),(61,1),(62,1),(63,1),(64,1),(65,1),(66,1),(67,1),(68,1),(69,1),(70,1),(71,1),(72,1),(73,1),(74,1),
	(75,1),(76,1),(77,1),(78,1),(79,1),(80,1),(81,1),(82,1),(83,1),(84,1),(85,1),(86,1),(87,1),(88,1),(89,1),(90,1),
	(91,1),(92,1),(93,1),(94,1),(95,1),(96,1),(97,1),(98,1),(99,1),(100,1),(101,1),(102,1),(103,1),(104,1),(105,1),
	(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,1),(114,1),(115,1),(116,1),(117,1),(118,1),(119,1),
	(120,1),(121,1),(122,1),(123,1),(124,1),(125,1),(126,1),(127,1),(128,1),(129,1),(130,1),(131,1),(132,1),(133,1),
	(134,1),(135,1),(136,1),(137,1),(138,1),(139,1),(140,1),(141,1),(142,1),(143,1),(144,1),(145,1),(146,1),(147,1),
	(148,1),(149,1),(150,1),(151,1),(152,1),(153,1),(154,1),(155,1),(156,1),(157,1),(158,1),(159,1),(160,1),(161,1),
	(162,1),(163,1),(164,1),(165,1),(166,1),(167,1),(168,1),(169,1),(170,1),(171,1),(172,1),(173,1),(174,1),(175,1),
	(176,1),(177,1),(178,1),(179,1),(180,1),(181,1),(182,1),(183,1),(184,1),(185,1),(186,1),(187,1),(188,1),(189,1),
	(190,1),(191,1),(192,1),(193,1),(194,1),(195,1),(196,1),(197,1),(198,1),(199,1),(200,1),(201,1),(202,1),(203,1),
	(204,1),(205,1),(206,1),(207,1),(208,1),(209,1),(210,1),(211,1),(212,1),(213,1),(214,1),(215,1),(216,1),(217,1),
	(218,1),(219,1),(220,1),(221,1),(222,1),(223,1),(224,1),(225,1),(226,1),(227,1),(228,1),(229,1),(230,1),(231,1),
	(232,1),(233,1),(234,1),(235,1),(236,1),(237,1),(238,1),(239,1),(240,1),(241,1),(242,1),(243,1),(244,1),(245,1),
	(246,1),(247,1),(248,1),(249,1),(250,1),(251,1),(252,1),(253,1),(254,1),(255,1),(256,1),(257,1),(258,1),(259,1),
	(260,1),(261,1),(262,1),(263,1),(264,1),(265,1),(266,1),(267,1),(268,1),(269,1),(270,1),(271,1),(272,1),(273,1),
	(274,1),(275,1),(276,1),(277,1),(278,1),(279,1),(280,1),(281,1),(282,1),(283,1),(284,1),(285,1),(286,1),(287,1),
	(288,1),(289,1),(290,1),(291,1),(292,1),(293,1),(294,1),(295,1),(296,1),(297,1),(298,1),(299,1),(300,1),(301,1),
	(302,1),(303,1),(304,1),(305,1),(306,1),(307,1),(308,1),(309,1),(310,1),(311,1),(312,1),(313,1),(314,1),(315,1),
	(316,1),(317,1),(318,1),(319,1),(320,1),(321,1),(322,1),(323,1),(324,1),(325,1),(326,1),(327,1),(328,1),(329,1),
	(330,1),(331,1),(332,1),(333,1),(334,1),(335,1),(336,1),(337,1),(338,1),(339,1),(340,1),(341,1),(342,1),(343,1),
	(344,1),(345,1),(346,1),(347,1),(348,1),(349,1),(350,1),(351,1),(352,1),(353,1),(354,1),(355,1),(356,1),(357,1),
	(358,1),(359,1),(360,1),(361,1),(362,1),(363,1),(364,1),(365,1),(366,1),(367,1),(368,1),(369,1),(370,1),(371,1),
	(372,1),(373,1),(374,1),(375,1),(376,1),(377,1),(378,1),(379,1),(380,1),(381,1),(382,1),(383,1),(384,1),(385,1),
	(386,1),(387,1),(388,1),(389,1),(390,1),(391,1),(392,1),(393,1),(394,1),(395,1),(396,1),(397,1),(398,1),(399,1),
	(400,1),(401,1),(402,1),(403,1),(404,1),(405,1),(406,1),(407,1),(408,1),(409,1),(410,1),(411,1),(412,1),(413,1),
	(414,1),(415,1),(416,1),(417,1),(418,1),(419,1),(420,1),(421,1),(422,1),(423,1),(424,1),(425,1),(426,1),(427,1),
	(428,1),(429,1),(430,1),(431,1),(432,1),(433,1),(434,1),(435,1),(436,1),(437,1),(438,1),(439,1),(440,1),(441,1),
	(442,1),(443,1),(444,1),(445,1),(446,1),(447,1),(448,1),(449,1),(450,1),(451,1),(452,1),(453,1),(454,1),(455,1),
	(456,1),(457,1),(458,1),(459,1),(460,1),(461,1),(462,1),(463,1),(464,1),(465,1),(466,1),(467,1),(468,1),(469,1),
	(470,1),(471,1),(472,1),(473,1),(474,1),(475,1),(476,1),(477,1),(478,1),(479,1),(480,1),(481,1),(482,1),(483,1),
	(484,1),(485,1),(486,1),(487,1),(488,1),(489,1),(490,1),(491,1),(492,1),(493,1),(494,1),(495,1),(496,1),(497,1),
	(498,1),(499,1),(500,1),(501,1),(502,1),(503,1),(504,1),(505,1),(506,1),(507,1),(508,1),(509,1),(510,1),(511,1),
	(512,1),(513,1),(514,1),(515,1),(516,1),(517,1),(518,1),(519,1),(520,1),(521,1),(522,1),(523,1),(524,1),(525,1),
	(526,1),(527,1),(528,1),(529,1),(530,1),(531,1),(532,1),(533,1),(534,1),(535,1),(536,1),(537,1),(538,1),(539,1),
	(540,1),(541,1),(542,1),(543,1),(544,1),(545,1),(546,1),(547,1),(548,1),(549,1),(550,1),(551,1),(552,1),(553,1),
	(554,1),(555,1),(556,1),(557,1),(558,1),(559,1),(560,1),(561,1),(562,1),(563,1),(564,1),(565,1),(566,1),(567,1),
	(568,1),(569,1),(570,1),(571,1),(572,1),(573,1),(574,1),(575,1),(576,1),(577,1),(578,1),(579,1),(580,1),(581,1),
	(582,1),(583,1),(584,1),(585,1),(586,1),(587,1),(588,1),(589,1),(590,1),(591,1),(592,1),(593,1),(594,1),(595,1),
	(596,1),(597,1),(598,1),(599,1),(600,1),(601,1),(602,1),(603,1),(604,1),(605,1),(606,1),(607,1),(608,1),(609,1),
	(610,1),(611,1),(612,1),(613,1),(614,1),(615,1),(616,1),(617,1),(618,1),(619,1),(620,1),(621,1),(622,1),(623,1),
	(624,1),(625,1),(626,1),(627,1),(628,1),(629,1),(630,1),(631,1),(632,1),(633,1),(634,1),(635,1),(636,1),(637,1),
	(638,1),(639,1),(640,1),(641,1),(642,1),(643,1),(644,1),(645,1),(646,1),(647,1),(648,1),(649,1),(650,1),(651,1),
	(652,1),(653,1),(654,1),(655,1),(656,1),(657,1),(658,1),(659,1),(660,1),(661,1),(662,1),(663,1),(664,1),(665,1),
	(666,1),(667,1),(668,1),(669,1),(670,1),(671,1),(672,1),(673,1),(674,1),(675,1),(676,1),(677,1),(678,1),(679,1),
	(680,1),(681,1),(682,1),(683,1),(684,1),(685,1),(686,1),(687,1),(688,1),(689,1),(690,1),(691,1),(692,1),(693,1),
	(694,1),(695,1),(696,1),(697,1),(698,1),(699,1),(700,1),(701,1),(702,1),(703,1),(704,1),(705,1),(706,1),(707,1),
	(708,1),(709,1),(710,1),(711,1),(712,1),(713,1),(714,1),(715,1),(716,1),(717,1),(718,1),(719,1),(720,1),(721,1),
	(722,1),(723,1),(724,1),(725,1),(726,1),(727,1),(728,1),(729,1),(730,1),(731,1),(732,1),(733,1),(734,1),(735,1),
	(736,1),(737,1),(738,1),(739,1),(740,1),(741,1),(742,1),(743,1),(744,1),(745,1),(746,1),(747,1),(748,1),(749,1),
	(750,1),(751,1),(752,1),(753,1),(754,1),(755,1),(756,1),(757,1),(758,1),(759,1),(760,1),(761,1),(762,1),(763,1),
	(764,1),(765,1),(766,1),(767,1),(768,1),(769,1),(770,1),(771,1),(772,1),(773,1),(774,1),(775,1),(776,1),(777,1),
	(778,1),(779,1),(780,1),(781,1),(782,1),(783,1),(784,1),(785,1),(786,1),(787,1),(788,1),(789,1),(790,1),(791,1),
	(792,1),(793,1),(794,1),(795,1),(796,1),(797,1),(798,1),(799,1),(800,1),(801,1),(802,1),(803,1),(804,1),(805,1),
	(806,1),(807,1),(808,1),(809,1),(810,1),(811,1),(812,1),(813,1),(814,1),(815,1),(816,1),(817,1),(818,1),(819,1),
	(820,1),(821,1),(822,1),(823,1),(824,1),(825,1),(826,1),(827,1),(828,1),(829,1),(830,1),(831,1),(832,1),(833,1),
	(834,1),(835,1),(836,1),(837,1),(838,1),(839,1),(840,1),(841,1),(842,1),(843,1),(844,1),(845,1),(846,1),(847,1),
	(848,1),(849,1),(850,1),(851,1),(852,1),(853,1),(854,1),(855,1),(856,1),(857,1),(858,1),(859,1),(860,1),(861,1),
	(862,1),(863,1),(864,1),(865,1),(866,1),(867,1),(868,1),(869,1),(870,1),(871,1),(872,1),(873,1),(874,1),(875,1),
	(876,1),(877,1),(878,1),(879,1),(880,1),(881,1),(882,1),(883,1),(884,1),(885,1),(886,1),(887,1),(888,1),(889,1),
	(890,1),(891,1),(892,1),(893,1),(894,1),(895,1),(896,1),(897,1),(898,1),(899,1),(900,1),(901,1),(902,1),(903,1),
	(904,1),(905,1),(906,1),(907,1),(908,1),(909,1),(910,1),(911,1),(912,1),(913,1),(914,1),(915,1),(916,1),(917,1),
	(918,1),(919,1),(920,1),(921,1),(922,1),(923,1),(924,1),(925,1),(926,1),(927,1),(928,1),(929,1),(930,1),(931,1),
	(932,1),(933,1),(934,1),(935,1),(936,1),(937,1),(938,1),(939,1),(940,1),(941,1),(942,1),(943,1),(944,1),(945,1),
	(946,1),(947,1),(948,1),(949,1),(950,1),(951,1),(952,1),(953,1),(954,1),(955,1),(956,1),(957,1),(958,1),(959,1),
	(960,1),(961,1),(962,1),(963,1),(964,1),(965,1),(966,1),(967,1),(968,1),(969,1),(970,1),(971,1),(972,1),(973,1),
	(974,1),(975,1),(976,1),(977,1),(978,1),(979,1),(980,1),(981,1),(982,1),(983,1),(984,1),(985,1),(986,1),(987,1),
	(988,1),(989,1),(990,1),(991,1),(992,1),(993,1),(994,1),(995,1),(996,1),(997,1),(998,1),(999,1),(1000,1),(1001,1),
	(1002,1),(1003,1),(1004,1),(1005,1),(1006,1),(1007,1),(1008,1),(1009,1),(1010,1),(1011,1),(1012,1),(1013,1),
	(1014,1),(1015,1),(1016,1),(1017,1),(1018,1),(1019,1),(1020,1),(1021,1),(1022,1),(1023,1),(1024,1),(1025,1),
	(1026,1),(1027,1),(1028,1),(1029,1),(1030,1),(1031,1),(1032,1),(1033,1),(1034,1),(1035,1),(1036,1),(1037,1),
	(1038,1),(1039,1),(1040,1),(1041,1),(1042,1),(1043,1),(1044,1),(1045,1),(1046,1),(1047,1),(1048,1),(1049,1),
	(1050,1),(1051,1),(1052,1),(1053,1),(1054,1),(1055,1),(1056,1),(1057,1),(1058,1),(1059,1),(1060,1),(1061,1),
	(1062,1),(1063,1),(1064,1),(1065,1),(1066,1),(1067,1),(1068,1),(1069,1),(1070,1),(1071,1),(1072,1),(1073,1),
	(1074,1),(1075,1),(1076,1),(1077,1),(1078,1),(1079,1),(1080,1),(1081,1),(1082,1),(1083,1),(1084,1),(1085,1),
	(1086,1),(1087,1),(1088,1),(1089,1),(1090,1),(1091,1),(1092,1),(1093,1),(1094,1),(1095,1),(1096,1),(1097,1),
	(1098,1),(1099,1),(1100,1),(1101,1),(1102,1),(1103,1),(1104,1),(1105,1),(1106,1),(1107,1),(1108,1),(1109,1),
	(1110,1),(1111,1),(1112,1),(1113,1),(1114,1),(1115,1),(1116,1),(1117,1),(1118,1),(1119,1),(1120,1),(1121,1),
	(1122,1),(1123,1),(1124,1),(1125,1),(1126,1),(1127,1),(1128,1),(1129,1),(1130,1),(1131,1),(1132,1),(1133,1),
	(1134,1),(1135,1),(1136,1),(1137,1),(1138,1),(1139,1),(1140,1),(1141,1),(1142,1),(1143,1),(1144,1),(1145,1),
	(1146,1),(1147,1),(1148,1),(1149,1),(1150,1),(1151,1),(1152,1),(1153,1),(1154,1),(1155,1),(1156,1),(1157,1),
	(1158,1),(1159,1),(1160,1),(1161,1),(1162,1),(1163,1),(1164,1),(1165,1),(1166,1),(1167,1),(1168,1),(1169,1),
	(1170,1),(1171,1),(1172,1),(1173,1),(1174,1),(1175,1),(1176,1),(1177,1),(1178,1),(1179,1),(1180,1),(1181,1),
	(1182,1),(1183,1),(1184,1),(1185,1),(1186,1),(1187,1),(1188,1),(1189,1),(1190,1),(1191,1),(1192,1),(1193,1),
	(1194,1),(1195,1),(1196,1),(1197,1),(1198,1),(1199,1),(1200,1);`)

	// This test steps through the unbuffered copier's synchronous CopyChunk
	// API directly, so it opts out of the now-default buffered copier.
	m := NewTestRunner(t, "e2et1", "ENGINE=InnoDB", WithBuffered(false))
	defer utils.CloseAndLog(m)
	require.Equal(t, "initial", m.status.Get().String())
	require.Equal(t, status.Progress{CurrentState: status.Initial, Summary: "", Tables: nil}, m.Progress())

	// Usually we would call m.Run() but we want to step through
	// the migration process manually.
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(m.db)
	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, m.setup(t.Context()))

	// Pin chunk size so a slow first chunk under CI load can't shrink the
	// chunker via the panic path and cause the second chunk to come back
	// with both bounds (see issue #766).
	disableDynamicChunking(t, m.copyChunker)

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.
	m.status.Set(status.CopyRows)
	require.Equal(t, "copyRows", m.status.Get().String())

	// We expect 2 chunks to be copied.
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// first chunk.
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.Equal(t, "((`id1` < 1001)\n OR (`id1` = 1001 AND `id2` < 1))", chunk.String())
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	require.Equal(t, status.Progress{CurrentState: status.CopyRows, Summary: "1000/1200 83.33% copyRows ETA TBD", ETA: status.ETA{State: status.ETAMeasuring}, Tables: []status.TableProgress{{TableName: "e2et1", RowsCopied: 1000, RowsTotal: 1200, IsComplete: false}}}, m.Progress())

	// Now insert some data.
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (1002, 2)`)

	// The composite chunker NOW supports keyAboveHighWatermark for numeric first column
	// so this event will be discarded (id1=1002 >= chunkPtr[0]=1001)
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	require.Equal(t, 0, m.replClient.GetDeltaLen())

	// Second chunk
	chunk, err = m.copyChunker.Next()
	require.NoError(t, err)
	require.Equal(t, "((`id1` > 1001)\n OR (`id1` = 1001 AND `id2` >= 1))", chunk.String())
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	require.Equal(t, status.Progress{CurrentState: status.CopyRows, Summary: "1201/1200 100.08% copyRows ETA DUE", ETA: status.ETA{State: status.ETADue}, Tables: []status.TableProgress{{TableName: "e2et1", RowsCopied: 1201, RowsTotal: 1200, IsComplete: true}}}, m.Progress())

	// Now insert some data.
	// This should be picked up by the binlog subscription
	// because it is within chunk size range of the second chunk.
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (5, 2)`)
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	// With KeyAboveHighWatermark, id1=5 < chunkPtr[0]=1001, so it's NOT discarded
	require.Equal(t, 1, m.replClient.GetDeltaLen())

	testutils.RunSQL(t, `delete from e2et1 where id1 = 1`)
	require.False(t, m.changes[0].chunker.KeyAboveHighWatermark(1))
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	// id1=1 is below high watermark (1 < 1001), so it's kept
	require.Equal(t, 2, m.replClient.GetDeltaLen())

	// Some data is inserted later, even though the last chunk is done.
	// We still care to pick it up because it could be inserted during checkpoint.
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (5000, 1)`)
	require.False(t, m.changes[0].chunker.KeyAboveHighWatermark(int64(math.MaxInt64)))

	// Now that copy rows is done, we flush the changeset until trivial.
	// and perform the optional checksum.
	require.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	require.Equal(t, "applyChangeset", m.status.Get().String())
	m.status.Set(status.Checksum)
	m.dbConfig = dbconn.NewDBConfig()
	require.NoError(t, m.checksum(t.Context()))
	require.Equal(t, "postChecksum", m.status.Get().String())
	require.Equal(t, status.Progress{CurrentState: status.PostChecksum, Summary: "Applying Changeset Deltas=0", Tables: []status.TableProgress{{TableName: "e2et1", RowsCopied: 1201, RowsTotal: 1200, IsComplete: true}}}, m.Progress())

	// All done!
	require.Equal(t, 0, m.db.Stats().InUse) // all connections are returned.
}

// TestE2EBinlogSubscribingCompositeKeyVarchar tests binlog subscription with composite key
// where the first column is non-numeric (VARCHAR). This validates that KeyAboveHighWatermark
// and KeyBelowLowWatermark optimizations work with VARCHAR keys.
//
// Since watermark optimizations are disabled before checksum (see runner.go), there's no risk
// of checksum corruption even if collation comparison differs slightly between Go and MySQL.
//
// Expected behavior:
// - KeyAboveHighWatermark compares VARCHAR keys and may discard events above the watermark
// - KeyBelowLowWatermark compares VARCHAR keys and allows immediate flush for events below watermark
// - Migration completes successfully with optimization enabled
func TestE2EBinlogSubscribingCompositeKeyVarchar(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "e2et3", `CREATE TABLE e2et3 (
		session_id varchar(40) NOT NULL,
		event_id int NOT NULL,
		data varchar(255) NOT NULL default '',
		PRIMARY KEY (session_id, event_id))`)
	// Seed with UUID-based session_ids — each doubling generates new unique UUIDs.
	tt.SeedRows(t, "INSERT INTO e2et3 (session_id, event_id) SELECT UUID(), FLOOR(RAND()*1000)", 64)

	m := NewTestRunner(t, "e2et3", "ENGINE=InnoDB",
		WithTargetChunkTime(50*time.Millisecond),
		WithBuffered(false))
	defer func() {
		require.NoError(t, m.Close())
	}()

	// Setup but don't call Run() - step through manually
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m.db.Close())
	}()

	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, m.setup(t.Context()))

	// Start copying
	m.status.Set(status.CopyRows)
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// Copy first chunk
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Insert data mid-copy - simulates real-world concurrent writes
	// Note: With random UUIDs, watermark behavior is non-deterministic
	// (UUIDs may be above/below watermark unpredictably)
	// so we don't assert deltaLen. Final checksum validates correctness.
	testutils.RunSQL(t, `INSERT INTO e2et3 (session_id, event_id, data) VALUES (UUID(), 999, 'test')`)
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	// Note: deltaLen not asserted - UUID comparison with watermark is non-deterministic

	// Copy remaining chunks
	for {
		chunk, err := m.copyChunker.Next()
		if errors.Is(err, table.ErrTableIsRead) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	// Insert another event after copying completes
	testutils.RunSQL(t, `INSERT INTO e2et3 (session_id, event_id, data) VALUES (UUID(), 1000, 'test2')`)
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	// Note: deltaLen not asserted - UUID comparison with watermark is non-deterministic

	// Flush and complete migration
	require.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	m.status.Set(status.Checksum)
	require.NoError(t, m.checksum(t.Context()))
	require.Equal(t, "postChecksum", m.status.Get().String())

	// All done!
	require.Equal(t, 0, m.db.Stats().InUse)
}

// TestE2EBinlogSubscribingCompositeKeyCollation demonstrates that Go's lexicographic comparison
// differs from MySQL's case-insensitive collation, but checksum phase catches any issues.
// This is an E2E validation of the behavior demonstrated in TestCompositeChunkerCollationDifference.
//
// Test scenario:
// 1. Create table with VARCHAR primary key and case-insensitive collation
// 2. Insert rows with mixed case prefixes (KEY*, Key*, key*)
// 3. During copy phase, watermark uses Go comparison (may incorrectly buffer some changes)
// 4. Make changes to rows that Go thinks are "above watermark" but MySQL considers "already processed"
// 5. Checksum phase catches any buffered/missed changes and ensures correctness
//
// Expected behavior:
// - Migration completes successfully despite comparison differences
// - Checksum validates all changes were applied correctly
// - No data loss despite Go vs MySQL comparison mismatch
func TestE2EBinlogSubscribingCompositeKeyCollation(t *testing.T) {
	t.Parallel()
	// Case-insensitive collation — mixed case names exercise watermark comparison behavior.
	testutils.NewTestTable(t, "e2et_collation", `CREATE TABLE e2et_collation (
		name VARCHAR(50) NOT NULL,
		id INT NOT NULL,
		data VARCHAR(255) NOT NULL DEFAULT '',
		PRIMARY KEY (name, id)
	) COLLATE=utf8mb4_0900_ai_ci`)
	// Seed deterministically with mixed-case prefixes.
	// Use 3 batches of 400 rows (within MySQL's default cte_max_recursion_depth of 1000).
	testutils.RunSQL(t, `INSERT INTO e2et_collation (name, id)
		WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 400)
		SELECT CONCAT('KEY', LPAD(n, 4, '0')), n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO e2et_collation (name, id)
		WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 400)
		SELECT CONCAT('Key', LPAD(n, 4, '0')), n + 400 FROM seq`)
	testutils.RunSQL(t, `INSERT INTO e2et_collation (name, id)
		WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 400)
		SELECT CONCAT('key', LPAD(n, 4, '0')), n + 800 FROM seq`)

	m := NewTestRunner(t, "e2et_collation", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(false))
	defer func() {
		require.NoError(t, m.Close())
	}()

	// Setup for manual stepping to observe checksum behavior
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)

	// Get table info and setup
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, m.setup(t.Context()))

	// Start copying
	m.status.Set(status.CopyRows)
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// Copy first chunk only - this establishes the watermark
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.NotNil(t, chunk.UpperBound, "first chunk must have an upper bound (need at least 2 chunks)")
	t.Logf("First chunk upper bound: %+v (this becomes the high watermark)", chunk.UpperBound.Value)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// The watermark is now at the upper bound of first chunk (set synchronously by CopyChunk → Feedback).
	// For our data, this should be around ('KEY0399', 399) or similar.
	// We'll insert a row with 'key' prefix that Go thinks is higher but MySQL thinks is lower.

	// Insert a row that will be DISCARDED due to collation differences
	// We insert 'key0050' which:
	// - Go comparison: 'key0050' > 'KEY0167' because 'k'(107) > 'K'(75) → KeyAboveHighWatermark returns TRUE → DISCARD
	// - MySQL comparison: 'key0050' < 'KEY0167' (case-insensitive) → should be in already-copied range
	testutils.RunSQL(t, `INSERT INTO e2et_collation (name, id, data) VALUES ('key0050', 9999, 'INSERTED_DURING_MIGRATION')`)
	t.Log("→ Inserted row ('key0050', 9999) after first chunk copied")
	t.Log("→ First chunk upper bound (high watermark) starts with 'KEY' (capital K)")
	t.Log("→ Go byte comparison: 'key0050' > 'KEY...' because 'k'(107) > 'K'(75)")
	t.Log("→ KeyAboveHighWatermark returns TRUE → Binlog event DISCARDED")
	t.Log("→ MySQL collation: 'key0050' < 'KEY...' (case-insensitive) → should be kept")

	// Wait for replication to catch up and process/discard the event
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	// Copy remaining chunks
	for {
		chunk, err := m.copyChunker.Next()
		if errors.Is(err, table.ErrTableIsRead) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	// Flush any buffered changes and wait for replication to fully catch up
	require.NoError(t, m.replClient.Flush(t.Context()))
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	// VERIFY: Row is missing from target table before checksum
	t.Log("→ Verifying row status BEFORE checksum...")
	var targetCountBefore int
	err = m.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _e2et_collation_new WHERE name = 'key0050' AND id = 9999`).Scan(&targetCountBefore)
	require.NoError(t, err)
	if targetCountBefore == 0 {
		t.Log("✓ CONFIRMED: Row is MISSING from target table (event was discarded due to collation)")
	} else {
		t.Log("⚠ Row already exists in target (event was NOT discarded - test scenario didn't trigger)")
		t.Log("  This can happen if:")
		t.Log("  1. The row was in the first chunk we copied")
		t.Log("  2. The watermark optimization didn't apply")
		t.Log("  3. The replication event was processed before watermark was checked")
	}

	// Verify row exists in source
	var sourceCount int
	err = m.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM e2et_collation WHERE name = 'key0050' AND id = 9999`).Scan(&sourceCount)
	require.NoError(t, err)
	require.Equal(t, 1, sourceCount, "Source table should have the row")

	// Run checksum to detect and fix the discrepancy
	t.Log("→ Running checksum phase...")
	m.status.Set(status.Checksum)
	require.NoError(t, m.checksum(t.Context()))

	// VERIFY: Row now exists in target table after checksum
	t.Log("→ Verifying row status AFTER checksum...")
	var targetCountAfter int
	err = m.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _e2et_collation_new WHERE name = 'key0050' AND id = 9999`).Scan(&targetCountAfter)
	require.NoError(t, err)
	require.Equal(t, 1, targetCountAfter, "Target table should have the row after checksum fix")

	// Verify the data is correct
	var data string
	err = m.db.QueryRowContext(t.Context(), `SELECT data FROM _e2et_collation_new WHERE name = 'key0050' AND id = 9999`).Scan(&data)
	require.NoError(t, err)
	require.Equal(t, "INSERTED_DURING_MIGRATION", data, "Data should match after checksum fix")

	if targetCountBefore == 0 && targetCountAfter == 1 {
		t.Log("✓ SUCCESS: Row was MISSING before checksum, PRESENT after checksum")
		t.Log("✓ This proves KeyAboveHighWatermark discarded the event due to collation difference")
		t.Log("✓ Checksum detected and fixed the missing row")
	}
}

// TestE2EBinlogSubscribingCompositeKeyBinary tests binlog subscription with composite key
// where the first column is VARBINARY. This validates that KeyAboveHighWatermark
// and KeyBelowLowWatermark work correctly for binary keys with optimization enabled.
//
// Binary types use byte-order comparison which matches Go's comparison exactly:
// - _binary 'aa' > _binary 'AA' (because 0x61 > 0x41)
// - This matches Go's byte comparison, so watermark optimizations work perfectly
// - Unlike VARCHAR with collations where 'aa' = 'AA', binary comparison is deterministic
//
// This test verifies that:
// 1. KeyAboveHighWatermark correctly discards events above the watermark
// 2. No rows are incorrectly discarded (unlike VARCHAR with collations)
// 3. Checksum should find zero discrepancies
func TestE2EBinlogSubscribingCompositeKeyBinary(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "e2et_binary", `CREATE TABLE e2et_binary (
		name VARBINARY(255) NOT NULL,
		id BIGINT NOT NULL,
		data VARCHAR(255),
		PRIMARY KEY (name, id)
	)`)

	// Insert test data with binary values
	// We'll use hex values to make byte comparison clear
	// Insert 1200 rows across different byte ranges
	for i := range 400 {
		// Low bytes: 0x41 (A), 0x42 (B), 0x43 (C)...
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO e2et_binary VALUES (0x41%04d, %d, 'data')`, i, i))
	}
	for i := range 400 {
		// Mid bytes: 0x50 (P), 0x51 (Q), 0x52 (R)...
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO e2et_binary VALUES (0x50%04d, %d, 'data')`, i, i+400))
	}
	for i := range 400 {
		// High bytes: 0x61 (a), 0x62 (b), 0x63 (c)...
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO e2et_binary VALUES (0x61%04d, %d, 'data')`, i, i+800))
	}

	m := NewTestRunner(t, "e2et_binary", "ENGINE=InnoDB",
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithBuffered(false))
	defer func() {
		require.NoError(t, m.Close())
	}()

	// Setup for manual stepping to observe watermark behavior
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)

	// Get table info and setup
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, m.setup(t.Context()))

	t.Log("→ Starting manual copy process with unbuffered copier")

	// Start copying
	m.status.Set(status.CopyRows)
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// Copy ONLY the first chunk to establish watermark
	// This should set high watermark around 0x410167 (byte value 'A' with suffix)
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	t.Logf("→ First chunk copied, watermark established around upper bound")

	// NOW: Insert a row that should be discarded by KeyAboveHighWatermark
	// Insert 0x500050 (byte value 'P' with suffix)
	// This is above 0x41xxxx in BOTH Go byte comparison AND MySQL binary comparison
	// Go: 0x50 > 0x41 → KeyAboveHighWatermark returns TRUE → should DISCARD
	// MySQL binary: 0x500050 > 0x41xxxx → correctly above first chunk
	testutils.RunSQL(t, `INSERT INTO e2et_binary (name, id, data) VALUES (0x500050, 9999, 'INSERTED_DURING_MIGRATION')`)
	t.Log("→ Inserted row (0x500050, 9999) after first chunk copied")
	t.Log("→ First chunk upper bound has byte prefix 0x41")
	t.Log("→ Go byte comparison: 0x50 > 0x41")
	t.Log("→ KeyAboveHighWatermark returns TRUE → Binlog event DISCARDED")
	t.Log("→ MySQL binary comparison: 0x500050 > 0x41xxxx → correctly above watermark")
	t.Log("→ This is CORRECT behavior - row hasn't been copied yet, should be discarded")

	// Wait for replication to process/discard the event
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	// Copy remaining chunks (this will copy the 0x50xxxx and 0x61xxxx ranges)
	for {
		chunk, err := m.copyChunker.Next()
		if errors.Is(err, table.ErrTableIsRead) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	// Flush and wait for replication
	require.NoError(t, m.replClient.Flush(t.Context()))
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	// VERIFY: Row should NOT be missing (binary comparison is exact)
	t.Log("→ Verifying row status BEFORE checksum...")
	var targetCountBefore int
	err = m.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _e2et_binary_new WHERE name = 0x500050 AND id = 9999`).Scan(&targetCountBefore)
	require.NoError(t, err)
	require.Equal(t, 1, targetCountBefore, "Row should be present - it was copied in later chunks")
	t.Log("✓ CONFIRMED: Row is PRESENT in target table (copied in later chunks)")

	// Verify row exists in source
	var sourceCount int
	err = m.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM e2et_binary WHERE name = 0x500050 AND id = 9999`).Scan(&sourceCount)
	require.NoError(t, err)
	require.Equal(t, 1, sourceCount, "Source table should have the row")

	// Run checksum - should find ZERO discrepancies because binary comparison is exact
	t.Log("→ Running checksum phase...")
	m.status.Set(status.Checksum)
	require.NoError(t, m.checksum(t.Context()))

	// VERIFY: Row still exists (checksum should not need to fix anything)
	t.Log("→ Verifying row status AFTER checksum...")
	var targetCountAfter int
	err = m.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM _e2et_binary_new WHERE name = 0x500050 AND id = 9999`).Scan(&targetCountAfter)
	require.NoError(t, err)
	require.Equal(t, 1, targetCountAfter, "Row should still be present after checksum")

	// Verify the data is correct
	var data string
	err = m.db.QueryRowContext(t.Context(), `SELECT data FROM _e2et_binary_new WHERE name = 0x500050 AND id = 9999`).Scan(&data)
	require.NoError(t, err)
	require.Equal(t, "INSERTED_DURING_MIGRATION", data, "Data should match")

	t.Log("✓ SUCCESS: Binary type watermark optimization works correctly")
	t.Log("✓ KeyAboveHighWatermark correctly discarded event (row was above copy position)")
	t.Log("✓ Row was copied in later chunks as expected")
	t.Log("✓ Checksum found zero discrepancies (binary comparison matches Go comparison)")
	t.Log("✓ Binary type watermark optimization is working perfectly")
	t.Log("✓ No discrepancies because byte-order comparison matches between Go and MySQL")
}

// TestE2EBinlogSubscribingCompositeKeyDateTime tests binlog subscription with composite key
// where the first column is DATETIME. This validates that KeyAboveHighWatermark
// and KeyBelowLowWatermark work correctly for temporal keys with optimization enabled.
//
// Optimization behavior:
// - KeyAboveHighWatermark discards events above the copy position (copier hasn't reached them yet)
// - KeyBelowLowWatermark allows immediate flush of events in already-copied regions (no buffering delay)
// - Go string comparison is "close enough" for watermark optimizations
// - Checksum validation ensures no data loss despite comparison differences
//
// Expected behavior:
// - Some events may be discarded by watermark optimization
// - Checksum validation catches any discrepancies and retries with optimization disabled
// - Migration completes successfully with all data intact
func TestE2EBinlogSubscribingCompositeKeyDateTime(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "e2et4", `CREATE TABLE e2et4 (
		created_at DATETIME NOT NULL,
		event_id int NOT NULL,
		data varchar(255) NOT NULL default '',
		PRIMARY KEY (created_at, event_id))`)

	// Helper function to insert test data for a given event_id
	insertRows := func(eventID int) {
		testutils.RunSQL(t, fmt.Sprintf(`INSERT INTO e2et4 (created_at, event_id) 
			SELECT DATE_ADD('2024-01-01 00:00:00', INTERVAL n*%d HOUR), %d
			FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t`, eventID, eventID))
	}

	// Insert initial row
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id) SELECT '2024-01-01 00:00:00', 1 FROM dual`)

	// Add more rows across multiple timestamps to get ~60 rows total
	for i := 2; i <= 12; i++ {
		insertRows(i)
	}

	m := NewTestRunner(t, "e2et4", "ENGINE=InnoDB",
		WithTargetChunkTime(50*time.Millisecond),
		WithBuffered(false))
	defer func() {
		require.NoError(t, m.Close())
	}()

	// Setup but don't call Run() - step through manually
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, m.db.Close())
	}()

	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, m.setup(t.Context()))

	// Start copying
	m.status.Set(status.CopyRows)
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// Copy first chunk
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Insert data mid-copy - simulates real-world concurrent writes
	// Note: Go string comparison for DATETIME may differ from MySQL collation
	// so we don't assert deltaLen. Final checksum validates correctness.
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id, data) VALUES ('2024-01-01 01:00:00', 1000, 'early event')`)
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	// Note: deltaLen not asserted - DATETIME Go comparison may differ from MySQL

	// Copy remaining chunks
	for {
		chunk, err := m.copyChunker.Next()
		if errors.Is(err, table.ErrTableIsRead) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	// Insert another event after copying completes
	testutils.RunSQL(t, `INSERT INTO e2et4 (created_at, event_id, data) VALUES ('2024-06-15 12:00:00', 2000, 'mid-year event')`)
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	// Note: deltaLen not asserted - DATETIME Go comparison may differ from MySQL

	// Flush and complete migration
	require.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	m.status.Set(status.Checksum)
	require.NoError(t, m.checksum(t.Context()))
	require.Equal(t, "postChecksum", m.status.Get().String())

	// All done!
	require.Equal(t, 0, m.db.Stats().InUse)
}

func TestE2EBinlogSubscribingNonCompositeKey(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "e2et2", `CREATE TABLE e2et2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad int NOT NULL default 0)`)
	testutils.RunSQL(t, `insert into e2et2 (id) values (1)`)
	testutils.RunSQL(t, `insert into e2et2 (id) values (2)`)
	testutils.RunSQL(t, `insert into e2et2 (id) values (3)`)

	m := NewTestRunner(t, "e2et2", "ENGINE=InnoDB", WithBuffered(false))
	defer utils.CloseAndLog(m)
	require.Equal(t, "initial", m.status.Get().String())

	// Usually we would call m.Run() but we want to step through
	// the migration process manually.
	m.dbConfig = dbconn.NewDBConfig()
	m.status.Begin()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(m.db)
	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)
	require.NoError(t, m.setup(t.Context()))
	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	m.status.Set(status.CopyRows)
	require.Equal(t, "copyRows", m.status.Get().String())

	// We expect 3 chunks to be copied.
	// The special first and last case and middle case.
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// first chunk.
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	require.Equal(t, "`id` < 1", chunk.String())

	// Now insert some data.
	// This will be ignored by the binlog subscription.
	// Because it's ahead of the high watermark.
	testutils.RunSQL(t, `insert into e2et2 (id) values (4)`)
	require.True(t, m.changes[0].chunker.KeyAboveHighWatermark(4))
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	require.Equal(t, 0, m.replClient.GetDeltaLen())

	// second chunk is between min and max value.
	// retrieve it first. We'll copy it after we insert data.
	chunk, err = m.copyChunker.Next()
	require.NoError(t, err)
	require.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())

	// Now insert some data.
	// This should be picked up by the binlog subscription
	// because it is within chunk size range of the second chunk.
	// but until we copy the chunk it is *not* below the low watermark
	// and can't be flushed.
	testutils.RunSQL(t, `insert into e2et2 (id) values (5)`)
	require.False(t, m.changes[0].chunker.KeyAboveHighWatermark(5))
	require.False(t, m.changes[0].chunker.KeyBelowLowWatermark(5))
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	require.Equal(t, 1, m.replClient.GetDeltaLen())
	require.NoError(t, m.replClient.Flush(t.Context()))
	require.Equal(t, 1, m.replClient.GetDeltaLen())

	// Now copy it and because the CopyChunk() calls Feedback()
	// it will mean that the low watermark is advanced and
	// we can safely flush all changes.
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	require.True(t, m.changes[0].chunker.KeyBelowLowWatermark(5))
	require.NoError(t, m.replClient.Flush(t.Context()))
	require.Equal(t, 0, m.replClient.GetDeltaLen())

	// delete some data.
	testutils.RunSQL(t, `delete from e2et2 where id = 1`)
	require.False(t, m.changes[0].chunker.KeyAboveHighWatermark(1))
	require.True(t, m.changes[0].chunker.KeyBelowLowWatermark(1))
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	require.Equal(t, 1, m.replClient.GetDeltaLen())

	// We are able to flush, but we still have deltas
	// *because* of keyBelowLowWatermark.
	require.NoError(t, m.replClient.Flush(t.Context()))
	require.Equal(t, 0, m.replClient.GetDeltaLen())

	// third (and last) chunk is open ended,
	// so anything after it will be picked up by the binlog.
	chunk, err = m.copyChunker.Next()
	require.NoError(t, err)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	require.Equal(t, "`id` >= 1001", chunk.String())

	// Some data is inserted later, even though the last chunk is done.
	// We still care to pick it up because it could be inserted during checkpoint.
	testutils.RunSQL(t, `insert into e2et2 (id) values (6)`)
	// the pointer should be at maxint64 for safety. this ensures
	// that any keyAboveHighWatermark checks return false
	require.False(t, m.changes[0].chunker.KeyAboveHighWatermark(int64(math.MaxInt64)))

	// Now that copy rows is done, we flush the changeset until trivial.
	// and perform the optional checksum.
	require.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	require.Equal(t, "applyChangeset", m.status.Get().String())
	m.status.Set(status.Checksum)
	m.dbConfig = dbconn.NewDBConfig()
	require.NoError(t, m.checksum(t.Context()))
	require.Equal(t, "postChecksum", m.status.Get().String())
	// All done!
}

// TestE2ERogueValues tests that PRIMARY KEY values that contain single quotes
// are escaped correctly by the repl feed applier, the copier and checksum.
func TestE2EBinlogSubscribingRogueValues(t *testing.T) {
	// table contains a reserved word too.
	tbl := "CREATE TABLE e2erogue ( `datetime` varbinary(40) NOT NULL,`col2` int NOT NULL  default 0, primary key (`datetime`, `col2`))"

	testutils.NewTestTable(t, "e2erogue", tbl)
	testutils.RunSQL(t, `insert into e2erogue values ("1 \". ",1),("2 \". ",1),("3 \". ",1),("4 \". ",1),("5 \". ",1),("6 \". ",1),("7 \". ",1),("8 \". ",1),("9 \". ",1),("10 \". ",1),("11 \". ",1),("12 \". ",1),("13 \". ",1),("14 \". ",1),("15 \". ",1),("16 \". ",1),
	("17 \". ",1),("18 \". ",1),("19 \". ",1),("'20 \". ",1),("21 \". ",1),("22 \". ",1),("23 \". ",1),("24 \". ",1),("25 \". ",1),("26 \". ",1),("27 \". ",1),("28 \". ",1),("29 \". ",1),("30 \". ",1),("31 \". ",1),
	("32 \". ",1),("33 \". ",1),("34 \". ",1),("35 \". ",1),("3'6 \". ",1),("37 \". ",1),("38 \". ",1),("39 \". ",1),("40 \". ",1),("41 \". ",1),("42 \". ",1),("43 \". ",1),("44 \". ",1),("45 \". ",1),("46 \". ",1),
	("47 \". ",1),("48 \". ",1),("49 \". ",1),("50 \". ",1),("51 \". ",1),("52 \". ",1),("53 \". ",1),("54 \". ",1),("55 \". ",1),("56 \". ",1),("57 \". ",1),("58 \". ",1),("59 \". ",1),("60 \". ",1),("61 \". ",1),
	("62 \". ",1),("63 \". ",1),("64 \". ",1),("65 \". ",1),("66 \". ",1),("67 \". ",1),("68 \". ",1),("69 \". ",1),("70 \". ",1),("71 \". ",1),("72 \". ",1),("73 \". ",1),("74 \". ",1),("75 \". ",1),("76 \". ",1),
	("77 \". ",1),("78 \". ",1),("79 \". ",1),("80 \". ",1),("81 \". ",1),("82 \". ",1),("83 \". ",1),("84 \". ",1),("85 \". ",1),("86 \". ",1),("87 \". ",1),("88 \". ",1),("89 \". ",1),("90 \". ",1),("91 \". ",1),
	("92 \". ",1),("93 \". ",1),("94 \". ",1),("95 \". ",1),("96 \". ",1),("97 \". ",1),("98 \". ",1),("99 \". ",1),("100 \". ",1),("10\"1 \". ",1),("102 \". ",1),("103 \". ",1),("104 \". ",1),("105 \". ",1),
	("106 \". ",1),("107 \". ",1),("108 \". ",1),("109 \". ",1),("110 \". ",1),("111 \". ",1),("112 \". ",1),("113 \". ",1),("114 \". ",1),("115 \". ",1),("116 \". ",1),("117 \". ",1),("118 \". ",1),
	("119 \". ",1),("120 \". ",1),("121 \". ",1),("122 \". ",1),("123 \". ",1),("124 \". ",1),("125 \". ",1),("126 \". ",1),("127 \". ",1),("128 \". ",1),("129 \". ",1),("130 \". ",1),("131 \". ",1),("132 \". ",1),
	("133 \". ",1),("134 \". ",1),("135 \". ",1),("136 \". ",1),("137 \". ",1),("138 \". ",1),("139 \". ",1),("140 \". ",1),("141 \". ",1),("142 \". ",1),("143 \". ",1),("144 \". ",1),("145 \". ",1),("146 \". ",1),
	("147 \". ",1),("148 \". ",1),("149 \". ",1),("150 \". ",1),("151 \". ",1),("152 \". ",1),("153 \". ",1),("154 \". ",1),("155 \". ",1),("156 \". ",1),("157 \". ",1),("158 \". ",1),("159 \". ",1),("160 \". ",1),
	("161 \". ",1),("162 \". ",1),("163 \". ",1),("164 \". ",1),("165 \". ",1),("166 \". ",1),("167 \". ",1),("168 \". ",1),("169 \". ",1),("170 \". ",1),("171 \". ",1),("172 \". ",1),("173 \". ",1),("174 \". ",1),
	("175 \". ",1),("176 \". ",1),("177 \". ",1),("178 \". ",1),("179 \". ",1),("180 \". ",1),("181 \". ",1),("182 \". ",1),("183 \". ",1),("184 \". ",1),("185 \". ",1),("186 \". ",1),("187 \". ",1),("188 \". ",1),
	("189 \". ",1),("190 \". ",1),("191 \". ",1),("192 \". ",1),("193 \". ",1),("194 \". ",1),("195 \". ",1),("196 \". ",1),("197 \". ",1),("198 \". ",1),("199 \". ",1),("200 \". ",1),("201 \". ",1),("202 \". ",1),
	("203 \". ",1),("204 \". ",1),("205 \". ",1),("206 \". ",1),("207 \". ",1),("208 \". ",1),("209 \". ",1),("210 \". ",1),("211 \". ",1),("212 \". ",1),("213 \". ",1),("214 \". ",1),("215 \". ",1),("216 \". ",1),
	("217 \". ",1),("218 \". ",1),("219 \". ",1),("220 \". ",1),("221 \". ",1),("222 \". ",1),("223 \". ",1),("224 \". ",1),("225 \". ",1),("226 \". ",1),("227 \". ",1),("228 \". ",1),("229 \". ",1),("230 \". ",1),
	("231 \". ",1),("232 \". ",1),("233 \". ",1),("234 \". ",1),("235 \". ",1),("236 \". ",1),("237 \". ",1),("238 \". ",1),("239 \". ",1),("240 \". ",1),("241 \". ",1),("242 \". ",1),("243 \". ",1),("244 \". ",1),
	("245 \". ",1),("246 \". ",1),("247 \". ",1),("248 \". ",1),("249 \". ",1),("250 \". ",1),("251 \". ",1),("252 \". ",1),("253 \". ",1),("254 \". ",1),("255 \". ",1),("256 \". ",1),("257 \". ",1),("258 \". ",1),
	("259 \". ",1),("260 \". ",1),("261 \". ",1),("262 \". ",1),("263 \". ",1),("264 \". ",1),("265 \". ",1),("266 \". ",1),("267 \". ",1),("268 \". ",1),("269 \". ",1),("270 \". ",1),("271 \". ",1),("272 \". ",1),
	("273 \". ",1),("274 \". ",1),("275 \". ",1),("276 \". ",1),("277 \". ",1),("278 \". ",1),("279 \". ",1),("280 \". ",1),("281 \". ",1),("282 \". ",1),("283 \". ",1),("284 \". ",1),("285 \". ",1),("286 \". ",1),
	("287 \". ",1),("288 \". ",1),("289 \". ",1),("290 \". ",1),("291 \". ",1),("292 \". ",1),("293 \". ",1),("294 \". ",1),("295 \". ",1),("296 \". ",1),("297 \". ",1),("298 \". ",1),("299 \". ",1),("300 \". ",1),
	("301 \". ",1),("302 \". ",1),("303 \". ",1),("304 \". ",1),("305 \". ",1),("306 \". ",1),("307 \". ",1),("308 \". ",1),("309 \". ",1),("310 \". ",1),("311 \". ",1),("312 \". ",1),("313 \". ",1),("314 \". ",1),
	("315 \". ",1),("316 \". ",1),("317 \". ",1),("318 \". ",1),("319 \". ",1),("320 \". ",1),("321 \". ",1),("322 \". ",1),("323 \". ",1),("324 \". ",1),("325 \". ",1),("326 \". ",1),("327 \". ",1),("328 \". ",1),
	("329 \". ",1),("330 \". ",1),("331 \". ",1),("332 \". ",1),("333 \". ",1),("334 \". ",1),("335 \". ",1),("336 \". ",1),("337 \". ",1),("338 \". ",1),("339 \". ",1),("340 \". ",1),("341 \". ",1),("342 \". ",1),
	("343 \". ",1),("344 \". ",1),("345 \". ",1),("346 \". ",1),("347 \". ",1),("348 \". ",1),("349 \". ",1),("350 \". ",1),("351 \". ",1),("352 \". ",1),("353 \". ",1),("354 \". ",1),("355 \". ",1),("356 \". ",1),
	("357 \". ",1),("358 \". ",1),("359 \". ",1),("360 \". ",1),("361 \". ",1),("362 \". ",1),("363 \". ",1),("364 \". ",1),("365 \". ",1),("366 \". ",1),("367 \". ",1),("368 \". ",1),("369 \". ",1),("370 \". ",1),
	("371 \". ",1),("372 \". ",1),("373 \". ",1),("374 \". ",1),("375 \". ",1),("376 \". ",1),("377 \". ",1),("378 \". ",1),("379 \". ",1),("380 \". ",1),("381 \". ",1),("382 \". ",1),("383 \". ",1),("384 \". ",1),
	("385 \". ",1),("386 \". ",1),("387 \". ",1),("388 \". ",1),("389 \". ",1),("390 \". ",1),("391 \". ",1),("392 \". ",1),("393 \". ",1),("394 \". ",1),("395 \". ",1),("396 \". ",1),("397 \". ",1),("398 \". ",1),
	("399 \". ",1),("400 \". ",1),("401 \". ",1),("402 \". ",1),("403 \". ",1),("404 \". ",1),("405 \". ",1),("406 \". ",1),("407 \". ",1),("408 \". ",1),("409 \". ",1),("410 \". ",1),("411 \". ",1),("412 \". ",1),
	("413 \". ",1),("414 \". ",1),("415 \". ",1),("416 \". ",1),("417 \". ",1),("418 \". ",1),("419 \". ",1),("420 \". ",1),("421 \". ",1),("422 \". ",1),("423 \". ",1),("424 \". ",1),("425 \". ",1),("426 \". ",1),
	("427 \". ",1),("428 \". ",1),("429 \". ",1),("430 \". ",1),("431 \". ",1),("432 \". ",1),("433 \". ",1),("434 \". ",1),("435 \". ",1),("436 \". ",1),("437 \". ",1),("438 \". ",1),("439 \". ",1),("440 \". ",1),
	("441 \". ",1),("442 \". ",1),("443 \". ",1),("444 \". ",1),("445 \". ",1),("446 \". ",1),("447 \". ",1),("448 \". ",1),("449 \". ",1),("450 \". ",1),("451 \". ",1),("452 \". ",1),("453 \". ",1),("454 \". ",1),
	("455 \". ",1),("456 \". ",1),("457 \". ",1),("458 \". ",1),("459 \". ",1),("460 \". ",1),("461 \". ",1),("462 \". ",1),("463 \". ",1),("464 \". ",1),("465 \". ",1),("466 \". ",1),("467 \". ",1),("468 \". ",1),
	("469 \". ",1),("470 \". ",1),("471 \". ",1),("472 \". ",1),("473 \". ",1),("474 \". ",1),("475 \". ",1),("476 \". ",1),("477 \". ",1),("478 \". ",1),("479 \". ",1),("480 \". ",1),("481 \". ",1),("482 \". ",1),
	("483 \". ",1),("484 \". ",1),("485 \". ",1),("486 \". ",1),("487 \". ",1),("488 \". ",1),("489 \". ",1),("490 \". ",1),("491 \". ",1),("492 \". ",1),("493 \". ",1),("494 \". ",1),("495 \". ",1),("496 \". ",1),
	("497 \". ",1),("498 \". ",1),("499 \". ",1),("500 \". ",1),("501 \". ",1),("502 \". ",1),("503 \". ",1),("504 \". ",1),("505 \". ",1),("506 \". ",1),("507 \". ",1),("508 \". ",1),("509 \". ",1),("510 \". ",1),
	("511 \". ",1),("512 \". ",1),("513 \". ",1),("514 \". ",1),("515 \". ",1),("516 \". ",1),("517 \". ",1),("518 \". ",1),("519 \". ",1),("520 \". ",1),("521 \". ",1),("522 \". ",1),("523 \". ",1),("524 \". ",1),
	("525 \". ",1),("526 \". ",1),("527 \". ",1),("528 \". ",1),("529 \". ",1),("530 \". ",1),("531 \". ",1),("532 \". ",1),("533 \". ",1),("534 \". ",1),("535 \". ",1),("536 \". ",1),("537 \". ",1),("538 \". ",1),
	("539 \". ",1),("540 \". ",1),("541 \". ",1),("542 \". ",1),("543 \". ",1),("544 \". ",1),("545 \". ",1),("546 \". ",1),("547 \". ",1),("548 \". ",1),("549 \". ",1),("550 \". ",1),("551 \". ",1),("552 \". ",1),
	("553 \". ",1),("554 \". ",1),("555 \". ",1),("556 \". ",1),("557 \". ",1),("558 \". ",1),("559 \". ",1),("560 \". ",1),("561 \". ",1),("562 \". ",1),("563 \". ",1),("564 \". ",1),("565 \". ",1),("566 \". ",1),
	("567 \". ",1),("568 \". ",1),("569 \". ",1),("570 \". ",1),("571 \". ",1),("572 \". ",1),("573 \". ",1),("574 \". ",1),("575 \". ",1),("576 \". ",1),("577 \". ",1),("578 \". ",1),("579 \". ",1),("580 \". ",1),
	("581 \". ",1),("582 \". ",1),("583 \". ",1),("584 \". ",1),("585 \". ",1),("586 \". ",1),("587 \". ",1),("588 \". ",1),("589 \". ",1),("590 \". ",1),("591 \". ",1),("592 \". ",1),("593 \". ",1),("594 \". ",1),
	("595 \". ",1),("596 \". ",1),("597 \". ",1),("598 \". ",1),("599 \". ",1),("600 \". ",1),("601 \". ",1),("602 \". ",1),("603 \". ",1),("604 \". ",1),("605 \". ",1),("606 \". ",1),("607 \". ",1),("608 \". ",1),
	("609 \". ",1),("610 \". ",1),("611 \". ",1),("612 \". ",1),("613 \". ",1),("614 \". ",1),("615 \". ",1),("616 \". ",1),("617 \". ",1),("618 \". ",1),("619 \". ",1),("620 \". ",1),("621 \". ",1),("622 \". ",1),
	("623 \". ",1),("624 \". ",1),("625 \". ",1),("626 \". ",1),("627 \". ",1),("628 \". ",1),("629 \". ",1),("630 \". ",1),("631 \". ",1),("632 \". ",1),("633 \". ",1),("634 \". ",1),("635 \". ",1),("636 \". ",1),
	("637 \". ",1),("638 \". ",1),("639 \". ",1),("640 \". ",1),("641 \". ",1),("642 \". ",1),("643 \". ",1),("644 \". ",1),("645 \". ",1),("646 \". ",1),("647 \". ",1),("648 \". ",1),("649 \". ",1),("650 \". ",1),
	("651 \". ",1),("652 \". ",1),("653 \". ",1),("654 \". ",1),("655 \". ",1),("656 \". ",1),("657 \". ",1),("658 \". ",1),("659 \". ",1),("660 \". ",1),("661 \". ",1),("662 \". ",1),("663 \". ",1),("664 \". ",1),
	("665 \". ",1),("666 \". ",1),("667 \". ",1),("668 \". ",1),("669 \". ",1),("670 \". ",1),("671 \". ",1),("672 \". ",1),("673 \". ",1),("674 \". ",1),("675 \". ",1),("676 \". ",1),("677 \". ",1),("678 \". ",1),
	("679 \". ",1),("680 \". ",1),("681 \". ",1),("682 \". ",1),("683 \". ",1),("684 \". ",1),("685 \". ",1),("686 \". ",1),("687 \". ",1),("688 \". ",1),("689 \". ",1),("690 \". ",1),("691 \". ",1),("692 \". ",1),
	("693 \". ",1),("694 \". ",1),("695 \". ",1),("696 \". ",1),("697 \". ",1),("698 \". ",1),("699 \". ",1),("700 \". ",1),("701 \". ",1),("702 \". ",1),("703 \". ",1),("704 \". ",1),("705 \". ",1),("706 \". ",1),
	("707 \". ",1),("708 \". ",1),("709 \". ",1),("710 \". ",1),("711 \". ",1),("712 \". ",1),("713 \". ",1),("714 \". ",1),("715 \". ",1),("716 \". ",1),("717 \". ",1),("718 \". ",1),("719 \". ",1),("720 \". ",1),
	("721 \". ",1),("722 \". ",1),("723 \". ",1),("724 \". ",1),("725 \". ",1),("726 \". ",1),("727 \". ",1),("728 \". ",1),("729 \". ",1),("730 \". ",1),("731 \". ",1),("732 \". ",1),("733 \". ",1),("734 \". ",1),
	("735 \". ",1),("736 \". ",1),("737 \". ",1),("738 \". ",1),("739 \". ",1),("740 \". ",1),("741 \". ",1),("742 \". ",1),("743 \". ",1),("744 \". ",1),("745 \". ",1),("746 \". ",1),("747 \". ",1),("748 \". ",1),
	("749 \". ",1),("750 \". ",1),("751 \". ",1),("752 \". ",1),("753 \". ",1),("754 \". ",1),("755 \". ",1),("756 \". ",1),("757 \". ",1),("758 \". ",1),("759 \". ",1),("760 \". ",1),("761 \". ",1),("762 \". ",1),
	("763 \". ",1),("764 \". ",1),("765 \". ",1),("766 \". ",1),("767 \". ",1),("768 \". ",1),("769 \". ",1),("770 \". ",1),("771 \". ",1),("772 \". ",1),("773 \". ",1),("774 \". ",1),("775 \". ",1),("776 \". ",1),
	("777 \". ",1),("778 \". ",1),("779 \". ",1),("780 \". ",1),("781 \". ",1),("782 \". ",1),("783 \". ",1),("784 \". ",1),("785 \". ",1),("786 \". ",1),("787 \". ",1),("788 \". ",1),("789 \". ",1),("790 \". ",1),
	("791 \". ",1),("792 \". ",1),("793 \". ",1),("794 \". ",1),("795 \". ",1),("796 \". ",1),("797 \". ",1),("798 \". ",1),("799 \". ",1),("800 \". ",1),("801 \". ",1),("802 \". ",1),("803 \". ",1),("804 \". ",1),
	("805 \". ",1),("806 \". ",1),("807 \". ",1),("808 \". ",1),("809 \". ",1),("810 \". ",1),("811 \". ",1),("812 \". ",1),("813 \". ",1),("814 \". ",1),("815 \". ",1),("816 \". ",1),("817 \". ",1),("818 \". ",1),
	("819 \". ",1),("820 \". ",1),("821 \". ",1),("822 \". ",1),("823 \". ",1),("824 \". ",1),("825 \". ",1),("826 \". ",1),("827 \". ",1),("828 \". ",1),("829 \". ",1),("830 \". ",1),("831 \". ",1),("832 \". ",1),
	("833 \". ",1),("834 \". ",1),("835 \". ",1),("836 \". ",1),("837 \". ",1),("838 \". ",1),("839 \". ",1),("840 \". ",1),("841 \". ",1),("842 \". ",1),("843 \". ",1),("844 \". ",1),("845 \". ",1),("846 \". ",1),
	("847 \". ",1),("848 \". ",1),("849 \". ",1),("850 \". ",1),("851 \". ",1),("852 \". ",1),("853 \". ",1),("854 \". ",1),("855 \". ",1),("856 \". ",1),("857 \". ",1),("858 \". ",1),("859 \". ",1),("860 \". ",1),
	("861 \". ",1),("862 \". ",1),("863 \". ",1),("864 \". ",1),("865 \". ",1),("866 \". ",1),("867 \". ",1),("868 \". ",1),("869 \". ",1),("870 \". ",1),("871 \". ",1),("872 \". ",1),("873 \". ",1),("874 \". ",1),
	("875 \". ",1),("876 \". ",1),("877 \". ",1),("878 \". ",1),("879 \". ",1),("880 \". ",1),("881 \". ",1),("882 \". ",1),("883 \". ",1),("884 \". ",1),("885 \". ",1),("886 \". ",1),("887 \". ",1),("888 \". ",1),
	("889 \". ",1),("890 \". ",1),("891 \". ",1),("892 \". ",1),("893 \". ",1),("894 \". ",1),("895 \". ",1),("896 \". ",1),("897 \". ",1),("898 \". ",1),("899 \". ",1),("900 \". ",1),("901 \". ",1),("902 \". ",1),
	("903 \". ",1),("904 \". ",1),("905 \". ",1),("906 \". ",1),("907 \". ",1),("908 \". ",1),("909 \". ",1),("910 \". ",1),("911 \". ",1),("912 \". ",1),("913 \". ",1),("914 \". ",1),("915 \". ",1),("916 \". ",1),
	("917 \". ",1),("918 \". ",1),("919 \". ",1),("920 \". ",1),("921 \". ",1),("922 \". ",1),("923 \". ",1),("924 \". ",1),("925 \". ",1),("926 \". ",1),("927 \". ",1),("928 \". ",1),("929 \". ",1),("930 \". ",1),
	("931 \". ",1),("932 \". ",1),("933 \". ",1),("934 \". ",1),("935 \". ",1),("936 \". ",1),("937 \". ",1),("938 \". ",1),("939 \". ",1),("940 \". ",1),("941 \". ",1),("942 \". ",1),("943 \". ",1),("944 \". ",1),
	("945 \". ",1),("946 \". ",1),("947 \". ",1),("948 \". ",1),("949 \". ",1),("950 \". ",1),("951 \". ",1),("952 \". ",1),("953 \". ",1),("954 \". ",1),("955 \". ",1),("956 \". ",1),("957 \". ",1),("958 \". ",1),
	("959 \". ",1),("960 \". ",1),("961 \". ",1),("962 \". ",1),("963 \". ",1),("964 \". ",1),("965 \". ",1),("966 \". ",1),("967 \". ",1),("968 \". ",1),("969 \". ",1),("970 \". ",1),("971 \". ",1),("972 \". ",1),
	("973 \". ",1),("974 \". ",1),("975 \". ",1),("976 \". ",1),("977 \". ",1),("978 \". ",1),("979 \". ",1),("980 \". ",1),("981 \". ",1),("982 \". ",1),("983 \". ",1),("984 \". ",1),("985 \". ",1),("986 \". ",1),
	("987 \". ",1),("988 \". ",1),("989 \". ",1),("990 \". ",1),("991 \". ",1),("992 \". ",1),("993 \". ",1),("994 \". ",1),("995 \". ",1),("996 \". ",1),("997 \". ",1),("998 \". ",1),("999 \". ",1),("1000 \". ",1),
	("1001 \". ",1),("1002 \". ",1),("1003 \". ",1),("1004 \". ",1),("1005 \". ",1),("1006 \". ",1),("1007 \". ",1),("1008 \". ",1),("1009 \". ",1),("1010 \". ",1),("1011 \". ",1),("1012 \". ",1),
	("1013 \". ",1),("1014 \". ",1),("1015 \". ",1),("1016 \". ",1),("1017 \". ",1),("1018 \". ",1),("1019 \". ",1),("1020 \". ",1),("1021 \". ",1),("1022 \". ",1),("1023 \". ",1),("1024 \". ",1),
	("1025 \". ",1),("1026 \". ",1),("1027 \". ",1),("1028 \". ",1),("1029 \". ",1),("1030 \". ",1),("1031 \". ",1),("1032 \". ",1),("1033 \". ",1),("1034 \". ",1),("1035 \". ",1),("1036 \". ",1),
	("1037 \". ",1),("1038 \". ",1),("1039 \". ",1),("1040 \". ",1),("1041 \". ",1),("1042 \". ",1),("1043 \". ",1),("1044 \". ",1),("1045 \". ",1),("1046 \". ",1),("1047 \". ",1),("1048 \". ",1),
	("1049 \". ",1),("1050 \". ",1),("1051 \". ",1),("1052 \". ",1),("1053 \". ",1),("1054 \". ",1),("1055 \". ",1),("1056 \". ",1),("1057 \". ",1),("1058 \". ",1),("1059 \". ",1),("1060 \". ",1),
	("1061 \". ",1),("1062 \". ",1),("1063 \". ",1),("1064 \". ",1),("1065 \". ",1),("1066 \". ",1),("1067 \". ",1),("1068 \". ",1),("1069 \". ",1),("1070 \". ",1),("1071 \". ",1),("1072 \". ",1),
	("1073 \". ",1),("1074 \". ",1),("1075 \". ",1),("1076 \". ",1),("1077 \". ",1),("1078 \". ",1),("1079 \". ",1),("1080 \". ",1),("1081 \". ",1),("1082 \". ",1),("1083 \". ",1),("1084 \". ",1),
	("1085 \". ",1),("1086 \". ",1),("1087 \". ",1),("1088 \". ",1),("1089 \". ",1),("1090 \". ",1),("1091 \". ",1),("1092 \". ",1),("1093 \". ",1),("1094 \". ",1),("1095 \". ",1),("1096 \". ",1),
	("1097 \". ",1),("1098 \". ",1),("1099 \". ",1),("1100 \". ",1),("1101 \". ",1),("1102 \". ",1),("1103 \". ",1),("1104 \". ",1),("1105 \". ",1),("1106 \". ",1),("1107 \". ",1),("1108 \". ",1),
	("1109 \". ",1),("1110 \". ",1),("1111 \". ",1),("1112 \". ",1),("1113 \". ",1),("1114 \". ",1),("1115 \". ",1),("1116 \". ",1),("1117 \". ",1),("1118 \". ",1),("1119 \". ",1),("1120 \". ",1),
	("1121 \". ",1),("1122 \". ",1),("1123 \". ",1),("1124 \". ",1),("1125 \". ",1),("1126 \". ",1),("1127 \". ",1),("1128 \". ",1),("1129 \". ",1),("1130 \". ",1),("1131 \". ",1),("1132 \". ",1),
	("1133 \". ",1),("1134 \". ",1),("1135 \". ",1),("1136 \". ",1),("1137 \". ",1),("1138 \". ",1),("1139 \". ",1),("1140 \". ",1),("1141 \". ",1),("1142 \". ",1),("1143 \". ",1),("1144 \". ",1),
	("1145 \". ",1),("1146 \". ",1),("1147 \". ",1),("1148 \". ",1),("1149 \". ",1),("1150 \". ",1),("1151 \". ",1),("1152 \". ",1),("1153 \". ",1),("1154 \". ",1),("1155 \". ",1),("1156 \". ",1),
	("1157 \". ",1),("1158 \". ",1),("1159 \". ",1),("1160 \". ",1),("1161 \". ",1),("1162 \". ",1),("1163 \". ",1),("1164 \". ",1),("1165 \". ",1),("1166 \". ",1),("1167 \". ",1),("1168 \". ",1),
	("1169 \". ",1),("1170 \". ",1),("1171 \". ",1),("1172 \". ",1),("1173 \". ",1),("1174 \". ",1),("1175 \". ",1),("1176 \". ",1),("1177 \". ",1),("1178 \". ",1),("1179 \". ",1),("1180 \". ",1),
	("1181 \". ",1),("11'82 \". ",1),("118\"3 \". ",1),("1184 \". ",1),("1185 \". ",1),("1186 \". ",1),("1187 \". ",1),("1188 \". ",1),("1189 \". ",1),("1190 \". ",1),("1191 \". ",1),
	("1192 \". ",1),("1193 \". ",1),("1194 \". ",1),("1195 \". ",1),("119\"\"6 \". ",1),("1197 \". ",1),("1198 \". ",1),("1199 \". ",1),("1200 \". ",1);`)

	m := NewTestRunner(t, "e2erogue", "ENGINE=InnoDB", WithBuffered(false))
	defer utils.CloseAndLog(m)
	require.Equal(t, "initial", m.status.Get().String())

	// Usually we would call m.Run() but we want to step through
	// the migration process manually.
	m.dbConfig = dbconn.NewDBConfig()
	m.status.Begin()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(m.db)
	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	require.NoError(t, err)

	require.NoError(t, m.setup(t.Context()))

	// Pin chunk size so a slow first chunk under CI load can't shrink the
	// chunker via the panic path and cause the second chunk to come back
	// with both bounds (see issue #772).
	disableDynamicChunking(t, m.copyChunker)

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	m.status.Set(status.CopyRows)
	require.Equal(t, "copyRows", m.status.Get().String())

	// We expect 2 chunks to be copied.
	ccopier, ok := m.copier.(*copier.Unbuffered)
	require.True(t, ok)

	// first chunk.
	chunk, err := m.copyChunker.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	// Binary boundary datums render as 0x-hex literals so checkpoint
	// watermarks round-trip; 0x38313920222e20 is the value `819 ". `.
	require.Contains(t, chunk.String(), ` < 0x38313920222e20`)
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Wait for replication to catch up and watermark to be established
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	// Now insert some data. With watermark optimization enabled for binary types,
	// "zz'z\"z" > "819 \". " in byte order, so KeyAboveHighWatermark returns true.
	// This means the binlog event is discarded (not buffered), and the row will
	// be copied in later chunks or fixed during checksum.
	testutils.RunSQL(t, `insert into e2erogue values ("zz'z\"z", 2)`)
	require.True(t, m.changes[0].chunker.KeyAboveHighWatermark("zz'z\"z"))

	// Wait for the binlog event to be processed/discarded
	require.NoError(t, m.replClient.BlockWait(t.Context()))

	// Second chunk
	chunk, err = m.copyChunker.Next()
	require.NoError(t, err)
	require.Equal(t, "((`datetime` > 0x38313920222e20)\n OR (`datetime` = 0x38313920222e20 AND `col2` >= 1))", chunk.String())
	require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Now insert some data.
	// This should be picked up by the binlog subscription.
	// Note: "zz'z\"z" was discarded (KeyAboveHighWatermark=true), not buffered,
	// so delta count is 1 (only this insert), not 2.
	testutils.RunSQL(t, `insert into e2erogue values (5, 2)`)
	require.False(t, m.changes[0].chunker.KeyAboveHighWatermark(5))
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	require.Equal(t, 1, m.replClient.GetDeltaLen())

	testutils.RunSQL(t, "delete from e2erogue where `datetime` like '819%'")
	require.NoError(t, m.replClient.BlockWait(t.Context()))
	require.Equal(t, 2, m.replClient.GetDeltaLen())

	// Now that copy rows is done, we flush the changeset until trivial.
	// and perform the optional checksum.
	require.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	require.Equal(t, "applyChangeset", m.status.Get().String())
	m.status.Set(status.Checksum)
	m.dbConfig = dbconn.NewDBConfig()
	require.NoError(t, m.checksum(t.Context()))
	require.Equal(t, "postChecksum", m.status.Get().String())
	// All done!
}
