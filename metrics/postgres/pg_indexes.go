package postgres

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
)

const unusedIndexesTopN = 20

// unusedIndexes are the never-scanned non-unique indexes of a database.
type unusedIndexes struct {
	count float64
	bytes float64
	top   []bloatEntry // the largest ones, Bytes = index size
}

type dbIndexStats struct {
	unused     sql.Null[unusedIndexes]
	duplicates sql.Null[float64]
}

// Indexes backing unique/primary key/exclusion constraints are excluded: they are "used"
// by enforcing the constraint even if they are never scanned.
const unusedIndexesQuery = `
SELECT s.schemaname, s.relname, s.indexrelname, pg_relation_size(s.indexrelid),
	count(*) OVER (), sum(pg_relation_size(s.indexrelid)) OVER ()
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0
	AND NOT i.indisunique AND NOT i.indisprimary
	AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = s.indexrelid)
ORDER BY 4 DESC, 1, 2, 3
LIMIT %d`

// Two indexes are duplicates if they have the same table, access method, columns, operator classes,
// collations, expressions and predicate. The query returns the number of redundant indexes
// (a group of N identical indexes contributes N-1).
const duplicateIndexesQuery = `
SELECT COALESCE(sum(cnt - 1), 0) FROM (
	SELECT count(*) AS cnt
	FROM pg_index i
	JOIN pg_class ci ON ci.oid = i.indexrelid
	JOIN pg_class ct ON ct.oid = i.indrelid
	JOIN pg_namespace n ON n.oid = ct.relnamespace
	WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
	GROUP BY i.indrelid, ci.relam, i.indkey::text, i.indclass::text, i.indcollation::text,
		COALESCE(pg_get_expr(i.indexprs, i.indrelid), ''), COALESCE(pg_get_expr(i.indpred, i.indrelid), '')
	HAVING count(*) > 1
) d`

// collectIndexStats queries the database the db handle is connected to. The errors are
// returned for the bounded error-reason bookkeeping; partial results are kept.
func collectIndexStats(ctx context.Context, db *sql.DB) (*dbIndexStats, []error) {
	res := &dbIndexStats{}
	var errs []error

	rows, err := db.QueryContext(ctx, fmt.Sprintf(unusedIndexesQuery, unusedIndexesTopN))
	if err != nil {
		errs = append(errs, fmt.Errorf("unused indexes: %w", err))
	} else {
		u := unusedIndexes{}
		for rows.Next() {
			var e bloatEntry
			if err := rows.Scan(&e.Schema, &e.Table, &e.Index, &e.Bytes, &u.count, &u.bytes); err != nil {
				errs = append(errs, fmt.Errorf("scan unused indexes: %w", err))
				continue
			}
			u.top = append(u.top, e)
		}
		if err := rows.Err(); err != nil {
			errs = append(errs, fmt.Errorf("unused indexes: %w", err))
		} else {
			res.unused = sql.Null[unusedIndexes]{V: u, Valid: true}
		}
		rows.Close()
	}

	if err := db.QueryRowContext(ctx, duplicateIndexesQuery).Scan(&res.duplicates); err != nil {
		res.duplicates = sql.Null[float64]{}
		errs = append(errs, fmt.Errorf("duplicate indexes: %w", err))
	}
	return res, errs
}

// trimUnusedIndexes keeps only the n largest unused indexes across all databases,
// so that the number of pg_index_unused_bytes series doesn't grow with the number of databases.
func trimUnusedIndexes(stats map[string]*dbIndexStats, n int) {
	type item struct {
		db string
		e  bloatEntry
	}
	var all []item
	for db, s := range stats {
		if !s.unused.Valid {
			continue
		}
		for _, e := range s.unused.V.top {
			all = append(all, item{db: db, e: e})
		}
	}
	slices.SortFunc(all, func(a, b item) int {
		if c := cmp.Compare(b.e.Bytes, a.e.Bytes); c != 0 {
			return c
		}
		return cmp.Or(cmp.Compare(a.db, b.db), cmp.Compare(a.e.Schema, b.e.Schema), cmp.Compare(a.e.Table, b.e.Table), cmp.Compare(a.e.Index, b.e.Index))
	})
	if len(all) > n {
		all = all[:n]
	}
	byDB := map[string][]bloatEntry{}
	for _, it := range all {
		byDB[it.db] = append(byDB[it.db], it.e)
	}
	for db, s := range stats {
		if s.unused.Valid {
			s.unused.V.top = byDB[db]
		}
	}
}
