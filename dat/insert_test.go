package dat

import (
	"strings"
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"
)

type missingDbTag struct {
	ID int64
}

type someRecord struct {
	SomethingID int   `db:"something_id"`
	UserID      int64 `db:"user_id"`
	Other       bool  `db:"other"`
}

func BenchmarkInsertValuesSql(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		InsertInto("alpha").Columns("something_id", "user_id", "other").Values(1, 2, true).ToSQL()
	}
}

func BenchmarkInsertRecordsSql(b *testing.B) {
	obj := someRecord{1, 99, false}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		InsertInto("alpha").Columns("something_id", "user_id", "other").Record(obj).ToSQL()
	}
}

// XXX: All the asserts need to have the argument order flipped, to accurately report expected and actual values

func TestInsertSingleToSql(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2)", "b", "c"))
	assert.Equal(t, args, []interface{}{1, 2})
}

func TestDefaultValue(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, DEFAULT).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2)", "b", "c"))
	assert.Equal(t, args, []interface{}{1, DEFAULT})
}

func TestInsertMultipleToSql(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).Values(3, 4).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2),($3,$4)", "b", "c"))
	assert.Equal(t, args, []interface{}{1, 2, 3, 4})
}

func TestInsertRecordsToSql(t *testing.T) {
	objs := []someRecord{{1, 88, false}, {2, 99, true}}
	sql, args, err := InsertInto("a").Columns("something_id", "user_id", "other").Record(objs[0]).Record(objs[1]).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s,%s) VALUES ($1,$2,$3),($4,$5,$6)", "something_id", "user_id", "other"))
	checkSliceEqual(t, args, []interface{}{1, 88, false, 2, 99, true})
}

func TestInsertWhitelist(t *testing.T) {
	objs := []someRecord{{1, 88, false}, {2, 99, true}}
	sql, args, err := InsertInto("a").
		Whitelist("*").
		Record(objs[0]).
		Record(objs[1]).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s,%s) VALUES ($1,$2,$3),($4,$5,$6)", "something_id", "user_id", "other"))
	checkSliceEqual(t, []interface{}{1, 88, false, 2, 99, true}, args)

	_, _, err = InsertInto("a").Whitelist("*").Values("foo").ToSQL()
	assert.Equal(t, `"*" can only be used in conjunction with Record`, err.Error())
}

func TestInsertBlacklist(t *testing.T) {
	objs := []someRecord{{1, 88, false}, {2, 99, true}}
	sql, args, err := InsertInto("a").
		Blacklist("something_id").
		Record(objs[0]).
		Record(objs[1]).
		ToSQL()
	assert.NoError(t, err)
	// order is not guaranteed
	//assert.Equal(t, sql, `INSERT INTO a ("user_id","other") VALUES ($1,$2),($3,$4)`)
	assert.True(t, strings.Contains(sql, `user_id`))
	assert.True(t, strings.Contains(sql, `other`))
	checkSliceEqual(t, args, []interface{}{88, false, 99, true})

	// does not have any columns or record
	_, _, err = InsertInto("a").Blacklist("something_id").Values("foo").ToSQL()
	assert.Error(t, err)
}

func TestInsertDuplicateColumns(t *testing.T) {
	type A struct {
		Status string `db:"status"`
	}

	type B struct {
		Status string `db:"status"`
		A
	}

	b := B{}
	b.Status = "open"
	b.A.Status = "closed"
	sql, args, err := InsertInto("a").Columns("status").Record(&b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO a (status) VALUES ($1)`)
	assert.Equal(t, args, []interface{}{"open"})
}

func TestInsertOnConflictColumnDoNothing(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO NOTHING", "b", "c", "b"))
	assert.Equal(t, args, []interface{}{1, 2})
}

func TestInsertOnConflictConstraintDoNothing(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictConstraint("test_constraint").ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT ON CONSTRAINT test_constraint DO NOTHING", "b", "c"))
	assert.Equal(t, args, []interface{}{1, 2})
}

func TestInsertOnConflictWhereDoNothing(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictWhere("b", "b > 0").ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) WHERE %s DO NOTHING", "b", "c", "b", "b > 0"))
	assert.Equal(t, args, []interface{}{1, 2})
}

func TestInsertOnConflictSet(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", 50).ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = $3", "b", "c", "b", "b"), sql)
	assert.Equal(t, []interface{}{1, 2, 50}, args)
}

func TestInsertOnConflictSetMultiple(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", 50).Set("c", 100).ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = $3, %s = $4", "b", "c", "b", "b", "c"), sql)
	assert.Equal(t, []interface{}{1, 2, 50, 100}, args)
}

func TestInsertOnConflictSetExcluded(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", "EXCLUDED.b").ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = %s", "b", "c", "b", "b", "EXCLUDED.b"), sql)
	assert.Equal(t, []interface{}{1, 2}, args)
}

func TestInsertOnConflictSetExcludedMultiple(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", "EXCLUDED.b").Set("c", "EXCLUDED.c").ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = %s, %s = %s", "b", "c", "b", "b", "EXCLUDED.b", "c", "EXCLUDED.c"), sql)
	assert.Equal(t, []interface{}{1, 2}, args)
}

func TestInsertOnConflictSetMultipleMixed(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", "EXCLUDED.b").Set("c", 50).ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = %s, %s = $3", "b", "c", "b", "b", "EXCLUDED.b", "c"), sql)
	assert.Equal(t, []interface{}{1, 2, 50}, args)
}

func TestInsertOnConflictSetMultipleMixedTwo(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", 50).Set("c", "EXCLUDED.c").ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = $3, %s = %s", "b", "c", "b", "b", "c", "EXCLUDED.c"), sql)
	assert.Equal(t, []interface{}{1, 2, 50}, args)
}

func TestInsertOnConflictSetWhere(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", 50).Where("a.b = $1", 10).ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = $3 WHERE (%s = $4)", "b", "c", "b", "b", "a.b"), sql)
	assert.Equal(t, []interface{}{1, 2, 50, 10}, args)
}

func TestInsertOnConflictSetExcludedWhere(t *testing.T) {
	sql, args, err := InsertInto("a").Columns("b", "c").Values(1, 2).OnConflictColumn("b").Set("b", "EXCLUDED.b").Where("a.b = $1", 10).ToSQL()

	assert.NoError(t, err)

	assert.Equal(t, quoteSQL("INSERT INTO a (%s,%s) VALUES ($1,$2) ON CONFLICT (%s) DO UPDATE SET %s = %s WHERE (%s = $3)", "b", "c", "b", "b", "EXCLUDED.b", "a.b"), sql)
	assert.Equal(t, []interface{}{1, 2, 10}, args)
}
