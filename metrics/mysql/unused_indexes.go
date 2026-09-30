package mysql

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
)

const unusedIndexesTopN = 20

type indexKey struct {
	schema string
	table  string
	index  string
}

type unusedIndex struct {
	indexKey
	size sql.NullFloat64 // bytes, unknown if mysql.innodb_index_stats is not readable
}

type unusedIndexes struct {
	top       []unusedIndex      // the largest ones
	bySchema  map[string]float64 // number of unused indexes by schema
	sizeKnown bool
}

var systemSchemas = `('mysql', 'performance_schema', 'sys', 'information_schema')`

// Indexes that have never been used since the server start (or since the P_S table was truncated).
var unusedIndexesQuery = `
SELECT OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE INDEX_NAME IS NOT NULL AND INDEX_NAME <> 'PRIMARY' AND COUNT_STAR = 0
	AND OBJECT_SCHEMA NOT IN ` + systemSchemas

// Unique indexes enforce constraints, so they are "used" even if never read.
var uniqueIndexesQuery = `
SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
FROM information_schema.STATISTICS
WHERE NON_UNIQUE = 0 AND TABLE_SCHEMA NOT IN ` + systemSchemas + `
GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME`

// Requires SELECT on mysql.innodb_index_stats; persistent InnoDB statistics only.
const indexSizesQuery = `
SELECT database_name, table_name, index_name, stat_value * @@innodb_page_size
FROM mysql.innodb_index_stats
WHERE stat_name = 'size'`

// partitioned tables are stored as <table>#p#<partition> (or #P# on case-insensitive file systems)
var rePartitionSuffix = regexp.MustCompile(`(?i)#p#.*$`)

// buildUnusedIndexes excludes unique indexes and excluded schemas, counts the unused indexes
// per schema and keeps the n largest ones (then by name, for a stable set when sizes are unknown).
// The three queries are joined here rather than in SQL: they come from different engines with
// different collations, and the optional ones may be unavailable to a restricted user.
func buildUnusedIndexes(unused []indexKey, unique map[indexKey]bool, sizes map[indexKey]float64, exclude map[string]bool, n int) *unusedIndexes {
	res := &unusedIndexes{bySchema: map[string]float64{}, sizeKnown: sizes != nil}
	var all []unusedIndex
	for _, k := range unused {
		if unique[k] || exclude[k.schema] {
			continue
		}
		res.bySchema[k.schema]++
		u := unusedIndex{indexKey: k}
		if size, ok := sizes[k]; ok {
			u.size = sql.NullFloat64{Float64: size, Valid: true}
		}
		all = append(all, u)
	}
	slices.SortFunc(all, func(a, b unusedIndex) int {
		if c := cmp.Compare(b.size.Float64, a.size.Float64); c != 0 {
			return c
		}
		return cmp.Or(cmp.Compare(a.schema, b.schema), cmp.Compare(a.table, b.table), cmp.Compare(a.index, b.index))
	})
	if len(all) > n {
		all = all[:n]
	}
	res.top = all
	return res
}

func queryIndexKeys(ctx context.Context, db *sql.DB, query string) ([]indexKey, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []indexKey
	for rows.Next() {
		var k indexKey
		if err := rows.Scan(&k.schema, &k.table, &k.index); err != nil {
			return nil, err
		}
		res = append(res, k)
	}
	return res, rows.Err()
}

func queryIndexSizes(ctx context.Context, db *sql.DB) (map[indexKey]float64, error) {
	rows, err := db.QueryContext(ctx, indexSizesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[indexKey]float64{}
	for rows.Next() {
		var k indexKey
		var size sql.NullFloat64
		if err := rows.Scan(&k.schema, &k.table, &k.index, &size); err != nil {
			return nil, err
		}
		k.table = rePartitionSuffix.ReplaceAllString(k.table, "")
		res[k] += size.Float64
	}
	return res, rows.Err()
}

// collectUnusedIndexes returns nil if the list of unused indexes is unavailable (performance_schema
// disabled or not readable). Errors of the optional queries are returned along with the result.
func collectUnusedIndexes(ctx context.Context, db *sql.DB, exclude map[string]bool) (*unusedIndexes, []error) {
	unused, err := queryIndexKeys(ctx, db, unusedIndexesQuery)
	if err != nil {
		return nil, []error{fmt.Errorf("unused indexes: %w", err)}
	}
	// without the list of unique indexes they would be reported as unused: report nothing
	keys, err := queryIndexKeys(ctx, db, uniqueIndexesQuery)
	if err != nil {
		return nil, []error{fmt.Errorf("unique indexes: %w", err)}
	}
	unique := make(map[indexKey]bool, len(keys))
	for _, k := range keys {
		unique[k] = true
	}
	var errs []error
	sizes, err := queryIndexSizes(ctx, db)
	if err != nil {
		errs = append(errs, fmt.Errorf("index sizes: %w", err))
		sizes = nil
	}
	return buildUnusedIndexes(unused, unique, sizes, exclude, unusedIndexesTopN), errs

}
