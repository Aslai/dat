package dat

import (
	"testing"

	"gopkg.in/stretchr/testify.v1/assert"

	"github.com/matcherino/str"
)

func BenchmarkSelectBasicSql(b *testing.B) {
	// Do some allocations outside the loop so they don't affect the results
	argEq := Eq{"a": []int{1, 2, 3}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Select("something_id", "user_id", "other").
			From("some_table").
			Where("d = $1 OR e = $2", 1, "wat").
			Where(argEq).
			OrderBy("id DESC").
			Paginate(1, 20).
			ToSQL()
	}
}

func BenchmarkSelectFullSql(b *testing.B) {
	// Do some allocations outside the loop so they don't affect the results
	argEq1 := Eq{"f": 2, "x": "hi"}
	argEq2 := map[string]interface{}{"g": 3}
	argEq3 := Eq{"h": []int{1, 2, 3}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Select("a", "b", "z", "y", "x").
			Distinct().
			From("c").
			Where("d = $1 OR e = $2", 1, "wat").
			Where(argEq1).
			Where(argEq2).
			Where(argEq3).
			GroupBy("i").
			GroupBy("ii").
			GroupBy("iii").
			Having("j = k").
			Having("jj = $1", 1).
			Having("jjj = $1", 2).
			OrderBy("l").
			OrderBy("l").
			OrderBy("l").
			Limit(7).
			Offset(8).
			ToSQL()
	}
}

func TestSelectBasicToSql(t *testing.T) {
	sql, args, err := Select("a", "b").From("c").Where("id = $1", 1).ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, "SELECT a, b FROM c WHERE (id = $1)")
	assert.Equal(t, args, []interface{}{1})
}

func TestSelectFullToSql(t *testing.T) {
	sql, args, err := Select("a", "b").
		Distinct().
		From("c").
		Where("d = $1 OR e = $2", 1, "wat").
		Where(Eq{"f": 2}).
		Where(map[string]interface{}{"g": 3}).
		Where(Eq{"h": []int{4, 5, 6}}).
		GroupBy("i").
		Having("j = k").
		OrderBy("l").
		Limit(7).
		Offset(8).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("SELECT DISTINCT a, b FROM c WHERE (d = $1 OR e = $2) AND (%s = $3) AND (%s = $4) AND (%s IN $5) GROUP BY i HAVING (j = k) ORDER BY l LIMIT 7 OFFSET 8", "f", "g", "h"))
	assert.Equal(t, args, []interface{}{1, "wat", 2, 3, []int{4, 5, 6}})
}

func TestSelectPaginateOrderDirToSql(t *testing.T) {
	sql, args, err := Select("a", "b").
		From("c").
		Where("d = $1", 1).
		Paginate(1, 20).
		OrderBy("id DESC").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT a, b FROM c WHERE (d = $1) ORDER BY id DESC LIMIT 20 OFFSET 0")
	assert.Equal(t, args, []interface{}{1})

	sql, args, err = Select("a", "b").
		From("c").
		Where("d = $1", 1).
		Paginate(3, 30).
		OrderBy("id").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT a, b FROM c WHERE (d = $1) ORDER BY id LIMIT 30 OFFSET 60")
	assert.Equal(t, args, []interface{}{1})
}

func TestSelectNoWhereSql(t *testing.T) {
	sql, args, err := Select("a", "b").From("c").ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, "SELECT a, b FROM c")
	assert.Equal(t, args, []interface{}(nil))
}

func TestSelectMultiHavingSql(t *testing.T) {
	sql, args, err := Select("a", "b").From("c").Where("p = $1", 1).GroupBy("z").Having("z = $1", 2).Having("y = $1", 3).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT a, b FROM c WHERE (p = $1) GROUP BY z HAVING (z = $2) AND (y = $3)")
	assert.Equal(t, args, []interface{}{1, 2, 3})
}

func TestSelectMultiOrderSql(t *testing.T) {
	sql, args, err := Select("a", "b").From("c").OrderBy("name ASC").OrderBy("id DESC").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT a, b FROM c ORDER BY name ASC, id DESC")
	assert.Equal(t, args, []interface{}(nil))
}

func TestSelectWhereMapSql(t *testing.T) {
	sql, args, err := Select("a").From("b").Where(map[string]interface{}{"a": 1}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s = $1)", "a"))
	assert.Equal(t, args, []interface{}{1})

	sql, args, err = Select("a").From("b").Where(map[string]interface{}{"a": 1, "b": true}).ToSQL()
	assert.NoError(t, err)
	if sql == quoteSQL("SELECT a FROM b WHERE (%s = $1) AND (%s = $2)", "a", "b") {
		assert.Equal(t, args, []interface{}{1, true})
	} else {
		assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s = $1) AND (%s = $2)", "b", "a"))
		assert.Equal(t, args, []interface{}{true, 1})
	}

	sql, args, err = Select("a").From("b").Where(map[string]interface{}{"a": nil}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s IS NULL)", "a"))
	assert.Equal(t, args, []interface{}(nil))

	sql, args, err = Select("a").From("b").Where(map[string]interface{}{"a": []int{1, 2, 3}}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s IN $1)", "a"))
	assert.Equal(t, args, []interface{}{[]int{1, 2, 3}})

	sql, args, err = Select("a").From("b").Where(map[string]interface{}{"a": []int{1}}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s = $1)", "a"))
	assert.Equal(t, args, []interface{}{1})

	// NOTE: a has no valid values, we want a query that returns nothing
	sql, args, err = Select("a").From("b").Where(map[string]interface{}{"a": []int{}}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT a FROM b WHERE (1=0)")
	assert.Equal(t, args, []interface{}(nil))

	var aval []int
	sql, args, err = Select("a").From("b").Where(map[string]interface{}{"a": aval}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s IS NULL)", "a"))
	assert.Equal(t, args, []interface{}(nil))

	sql, args, err = Select("a").From("b").
		Where(map[string]interface{}{"a": []int(nil)}).
		Where(map[string]interface{}{"b": false}).
		ToSQL()
	assert.NoError(t, err)

	assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s IS NULL) AND (%s = $1)", "a", "b"))
	assert.Equal(t, args, []interface{}{false})
}

func TestSelectWhereEqSql(t *testing.T) {
	sql, args, err := Select("a").From("b").Where(Eq{"a": 1, "b": []int64{1, 2, 3}}).ToSQL()
	assert.NoError(t, err)
	if sql == quoteSQL("SELECT a FROM b WHERE (%s = $1) AND (%s IN $2)", "a", "b") {
		assert.Equal(t, args, []interface{}{1, []int64{1, 2, 3}})
	} else {
		assert.Equal(t, sql, quoteSQL("SELECT a FROM b WHERE (%s IN $1) AND (%s = $2)", "b", "a"))
		assert.Equal(t, args, []interface{}{[]int64{1, 2, 3}, 1})
	}
}

func TestSelectWhereExprSql(t *testing.T) {
	expr := Expr("id=$1", 100)
	sql, args, err := Select("a").From("b").Where(expr).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT a FROM b WHERE (id=$1)")
	assert.Exactly(t, args, []interface{}{100})
}

func TestRawSql(t *testing.T) {
	sql, args, err := SQL("SELECT * FROM users WHERE x = 1").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM users WHERE x = 1")
	assert.Equal(t, args, []interface{}(nil))

	sql, args, err = SQL("SELECT * FROM users WHERE x = $1 AND y IN $2", 9, []int{5, 6, 7}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM users WHERE x = $1 AND y IN $2")
	assert.Equal(t, args, []interface{}{9, []int{5, 6, 7}})

	// Doesn't fix shit if it's broken:
	sql, args, err = SQL("wat", 9, []int{5, 6, 7}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "wat")
	assert.Equal(t, args, []interface{}{9, []int{5, 6, 7}})
}

func TestSelectVarieties(t *testing.T) {
	sql, _, err := Select("id, name, email").From("users").ToSQL()
	assert.NoError(t, err)
	sql2, _, err := Select("id", "name", "email").From("users").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, sql2)
}

func TestSelectScope(t *testing.T) {
	scope := NewScope("WHERE :TABLE.id = :id and name = :name", M{"id": 1, "name": "foo"})
	sql, args, err := Select("a").From("b").ScopeMap(scope, M{"name": "mario"}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT a FROM b WHERE ( b.id = $1 and name = $2)`, sql)
	assert.Exactly(t, args, []interface{}{1, "mario"})
}

func TestInnerJoin(t *testing.T) {
	sql, args, err := Select("u.*, p.*").
		From(`
			users u
			INNER JOIN posts p on (p.author_id = u.id)
		`).
		Where(`u.id = $1`, 1).
		ToSQL()
	assert.NoError(t, err)
	sql = str.Clean(sql)
	assert.Equal(t, sql, "SELECT u.*, p.* FROM users u INNER JOIN posts p on (p.author_id = u.id) WHERE (u.id = $1)")
	assert.Exactly(t, args, []interface{}{1})
}

func TestScopeWhere(t *testing.T) {
	published := `
		INNER JOIN posts p on (p.author_id = u.id)
		WHERE
			p.state = $1
	`

	sql, args, err := Select("u.*, p.*").
		From(`users u`).
		Scope(published, "published").
		Where(`u.id = $1`, 1).
		ToSQL()
	assert.NoError(t, err)
	sql = str.Clean(sql)
	assert.Equal(t, "SELECT u.*, p.* FROM users u INNER JOIN posts p on (p.author_id = u.id) WHERE (u.id = $1) AND ( p.state = $2 )", sql)
	assert.Exactly(t, args, []interface{}{1, "published"})
}

func TestScopeJoinOnly(t *testing.T) {
	published := `
		INNER JOIN posts p on (p.author_id = u.id)
	`

	sql, args, err := Select("u.*, p.*").
		From(`users u`).
		Scope(published).
		Where(`u.id = $1`, 1).
		ToSQL()
	assert.NoError(t, err)
	sql = str.Clean(sql)
	assert.Equal(t, "SELECT u.*, p.* FROM users u INNER JOIN posts p on (p.author_id = u.id) WHERE (u.id = $1)", sql)
	assert.Exactly(t, args, []interface{}{1})
}

func TestDistinctOn(t *testing.T) {
	published := `
		INNER JOIN posts p on (p.author_id = u.id)
	`

	sql, args, err := Select("u.*, p.*").
		DistinctOn("foo", "bar").
		From(`users u`).
		Scope(published).
		Where(`u.id = $1`, 1).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT DISTINCT ON (foo, bar) u.*, p.*
		FROM users u
			INNER JOIN posts p on (p.author_id = u.id)
		WHERE (u.id = $1)`), stripWS(sql))
	assert.Exactly(t, args, []interface{}{1})
}

func TestSelectColumns(t *testing.T) {
	sql, args, err := Select("id, user_name").
		From("users").
		Columns("created_at").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT id, user_name, created_at
		FROM users
		`), stripWS(sql))
	assert.Nil(t, args)
}

func TestSelectFor(t *testing.T) {
	sql, args, err := Select("id").
		From("users").
		Where("id > $1", 1000).
		For("UPDATE").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT id FROM users WHERE (id > $1) FOR UPDATE
	`), stripWS(sql))
	assert.Exactly(t, []interface{}{1000}, args)
}

func TestSelectComplexFrom(t *testing.T) {
	sql, args, err := Select("users.id").
		From("users").
		From("admins").
		Where("users.id = admins.id OR $1 = users.id", 1000).
		For("UPDATE").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT users.id FROM users, admins WHERE (users.id = admins.id OR $1 = users.id) FOR UPDATE
	`), stripWS(sql))
	assert.Exactly(t, []interface{}{1000}, args)
}

func TestSelectComplexFromJoin(t *testing.T) {
	sql, args, err := Select("users.id").
		From("users").
		From("admins JOIN perms ON 1 = $1", 1).
		Join("perms AS perms2 ON perms.id = perms2.id").
		LeftJoin("users AS other_admins ON other_admins.role = perms.id OR $1 = 10", 30).
		RightJoin("something ON something.id = users.something_id").
		FullOuterJoin("other_thing ON other_thing.id = users.other_thing_id AND $1 = 1", 5).
		Where("users.id = admins.id").
		For("UPDATE").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT users.id
		FROM users,
		admins JOIN perms ON 1 = $1
		INNER JOIN perms AS perms2 ON perms.id = perms2.id
		LEFT JOIN users AS other_admins ON other_admins.role = perms.id OR $2 = 10
		RIGHT JOIN something ON something.id = users.something_id
		FULL OUTER JOIN other_thing ON other_thing.id = users.other_thing_id AND $3 = 1
		WHERE (users.id = admins.id) FOR UPDATE
	`), stripWS(sql))
	assert.Exactly(t, []interface{}{1, 30, 5}, args)
}
