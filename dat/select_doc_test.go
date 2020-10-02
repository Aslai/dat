package dat

import (
	"testing"
	"time"

	"github.com/mgutz/str"

	"gopkg.in/stretchr/testify.v1/assert"
)

func TestSelectDocSQLNoDocs(t *testing.T) {
	sql, args, err := SelectDoc("b", "c").From("a").Where("d=$1", 4).ToSQL()
	assert.NoError(t, err)

	expected := `
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT b,c
			FROM a
			WHERE (d=$1)
		) as dat__item
	`

	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{4}, args)
}

func TestSelectDocSQLDocs(t *testing.T) {
	sql, args, err := SelectDoc("b", "c").
		Many("f", `SELECT g, h FROM f WHERE id= $1`, 4).
		Many("x", `SELECT id, y, z FROM x`).
		Vector("y", `SELECT id FROM x`).
		Scalar("z", `SELECT id FROM x`).
		From("a").
		Where("d=$1", 4).
		Union(
			SelectDoc("f", "g").
				From("foo").
				Where("1 = 1")).
		UnionAll(
			SelectDoc("l", "m").
				From("bar").
				Where("3 = 3")).
		With("other", SelectDoc("h", "i").
			From("bar").
			Where("2 = 2")).
		ToSQL()
	assert.NoError(t, err)

	expected := `
	WITH other AS (
		SELECT h, i
		FROM bar
		WHERE (2 = 2)
	)
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			b,
			c,
			(SELECT array_agg(dat__f.*) FROM (SELECT g,h FROM f WHERE id=$1) AS dat__f) AS "f",
			(SELECT array_agg(dat__x.*) FROM (SELECT id,y,z FROM x) AS dat__x) AS "x",
			(SELECT array_agg(dat__y.dat__scalar) FROM (SELECT id FROM x) AS dat__y (dat__scalar)) AS "y",
			(SELECT dat__z.dat__scalar FROM (SELECT id FROM x) AS dat__z (dat__scalar) limit 1) AS "z"
		FROM a
		WHERE (d=$2)
		UNION SELECT f, g FROM foo WHERE (1 = 1)
		UNION ALL SELECT l, m FROM bar WHERE (3 = 3)
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{4, 4}, args)
}

func TestSelectDocSQLInnerSQL(t *testing.T) {
	sql, args, err := SelectDoc("b", "c").
		Many("f", `SELECT g, h FROM f WHERE id= $1`, 4).
		Many("x", `SELECT id, y, z FROM x`).
		InnerSQL(`
			FROM a
			WHERE d = $1
		`, 4).
		ToSQL()

	assert.NoError(t, err)

	expected := `
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			b,
			c,
			(SELECT array_agg(dat__f.*) FROM (SELECT g,h FROM f WHERE id=$1) AS dat__f) AS "f",
			(SELECT array_agg(dat__x.*) FROM (SELECT id,y,z FROM x) AS dat__x) AS "x"
		FROM a
		WHERE d=$2
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{4, 4}, args)
}

func TestSelectDocScope(t *testing.T) {
	now := NullTimeFrom(time.Now())

	sql, args, err := SelectDoc("e", "f").
		From("matches m").
		Scope(`
			WHERE m.game_id = $1
				AND (
					m.id > $3
					OR (m.id >= $2 AND m.id <= $3 AND m.updated_at > $4)
				)
		`, 100, 1, 2, now).
		ToSQL()
	assert.NoError(t, err)

	expected := `
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT e, f
			FROM matches m
			WHERE (m.game_id=$1
				AND (
					m.id > $3
					OR (m.id >= $2 AND m.id<=$3 AND m.updated_at>$4)
				))
		) as dat__item
	`

	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{100, 1, 2, now}, args)
}

func TestDocScopeWhere(t *testing.T) {
	published := `
		INNER JOIN posts p on (p.author_id = u.id)
		WHERE
			p.state = $1
	`
	sql, args, err := SelectDoc("u.*, p.*").
		From(`users u`).
		Scope(published, "published").
		Where(`u.id = $1`, 1).
		ToSQL()
	assert.NoError(t, err)
	sql = str.Clean(sql)
	expected := `
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT u.*, p.*
			FROM users u
				INNER JOIN posts p on (p.author_id = u.id)
			WHERE (u.id = $1) AND ( p.state = $2 )
		) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Exactly(t, args, []interface{}{1, "published"})
}

func TestDocDistinctOn(t *testing.T) {
	published := `
		INNER JOIN posts p on (p.author_id = u.id)
		WHERE
			p.state = $1
	`
	sql, args, err := SelectDoc("u.*, p.*").
		DistinctOn("aa", "bb").
		From(`users u`).
		Scope(published, "published").
		Where(`u.id = $1`, 1).
		ToSQL()
	assert.NoError(t, err)
	expected := `
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT DISTINCT ON (aa, bb)
			u.*, p.*
			FROM users u
				INNER JOIN posts p on (p.author_id = u.id)
			WHERE (u.id = $1) AND ( p.state = $2 )
		) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Exactly(t, args, []interface{}{1, "published"})
}

func TestNestedSelecDocWhere(t *testing.T) {
	user := SelectDoc("id", "user_name").
		Many("comments", `SELECT * FROM comments WHERE id = u.id`).
		From("users u").
		Where("u.id = $1", 1)

	sql, args, err := SelectDoc("id").
		One("user", user).
		From(`games`).
		Where(`id = $1`, 10).
		ToSQL()
	assert.NoError(t, err)

	expected := `
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT id,
				(
					SELECT row_to_json(dat__user.*)
					FROM (
						SELECT id, user_name,
							(
								SELECT array_agg(dat__comments.*)
								FROM (SELECT * FROM comments WHERE id = u.id)
								AS dat__comments
							) AS "comments"
						FROM users u
						WHERE (u.id = $1)
					) AS dat__user
				) AS "user"
			FROM games
			WHERE (id = $2)
		) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Exactly(t, args, []interface{}{1, 10})
}

func TestSelectDocColumns(t *testing.T) {
	sql, args, err := SelectDoc("id, user_name").
		From("users").
		Columns("created_at").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT id, user_name, created_at
			FROM users
		) as dat__item`), stripWS(sql))
	assert.Nil(t, args)
}

func TestSelectDocFor(t *testing.T) {
	sql, args, err := SelectDoc("id, user_name").
		From("users").
		Columns("created_at").
		For("UPDATE").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT id, user_name, created_at
			FROM users
			FOR UPDATE
		) as dat__item`), stripWS(sql))
	assert.Nil(t, args)
}

func TestSelectDocComplexFrom(t *testing.T) {
	sql, args, err := SelectDoc("users.id").
		From("users").
		From("admins").
		Where("users.id = admins.id OR $1 = users.id", 1000).
		For("UPDATE").
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, stripWS(`
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT users.id FROM users, admins WHERE (users.id = admins.id OR $1 = users.id) FOR UPDATE
		) as dat__item
	`), stripWS(sql))
	assert.Exactly(t, []interface{}{1000}, args)
}

func TestSelectDocComplexFromJoin(t *testing.T) {
	sql, args, err := SelectDoc("users.id").
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
		SELECT row_to_json(dat__item.*)
		FROM (
			SELECT users.id
			FROM users,
			admins JOIN perms ON 1 = $1
			INNER JOIN perms AS perms2 ON perms.id = perms2.id
			LEFT JOIN users AS other_admins ON other_admins.role = perms.id OR $2 = 10
			RIGHT JOIN something ON something.id = users.something_id
			FULL OUTER JOIN other_thing ON other_thing.id = users.other_thing_id AND $3 = 1
			WHERE (users.id = admins.id) FOR UPDATE
		) as dat__item
	`), stripWS(sql))
	assert.Exactly(t, []interface{}{1, 30, 5}, args)
}

func TestSelectDocWhitelist(t *testing.T) {
	sql, args, err := SelectDoc("b", "c").
		Many("f", `SELECT g, h FROM f WHERE id= $1`, 4).
		Many("x", `SELECT id, y, z FROM x`).
		Vector("y", `SELECT id FROM x`).
		One("foo", `SELECT id FROM x`).
		Scalar("z", `SELECT id FROM x`).
		From("a").
		Whitelist("f*", "x").
		Where("d=$1", 4).
		ToSQL()
	assert.NoError(t, err)

	expected := `
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			b,
			c,
			(SELECT array_agg(dat__f.*) FROM (SELECT g,h FROM f WHERE id=$1) AS dat__f) AS "f",
			(SELECT array_agg(dat__x.*) FROM (SELECT id,y,z FROM x) AS dat__x) AS "x",
			(SELECT row_to_json(dat__foo.*) FROM (SELECT id FROM x) AS dat__foo) AS "foo"
		FROM a
		WHERE (d=$2)
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{4, 4}, args)
}

func TestSelectDocWithScalarArray(t *testing.T) {
	sql, args, err := SelectDoc("data").
		From("foo").
		With("foo", []string{"Mary", "had", "a", "little", "lamb"}).
		Where("$1=9", 9).
		ToSQL()
	assert.NoError(t, err)

	expected := `
	WITH foo AS (
		SELECT UNNEST(ARRAY[$1, $2, $3, $4, $5]::text[]) AS data
	)
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			data
		FROM foo
		WHERE ($6=9)
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{"Mary", "had", "a", "little", "lamb", 9}, args)
}

func TestSelectDocWithStructArray(t *testing.T) {
	type dataType struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	data := []*dataType{
		&dataType{
			ID:   1,
			Name: "John Doe",
		},
		nil,
		&dataType{
			ID:   4,
			Name: "Mary Jane",
		},
		&dataType{
			ID:   19,
			Name: "Curious George",
		},
	}
	sql, args, err := SelectDoc("id, name").
		From("richData").
		With("richData", data).
		Where("$1=9", 9).
		ToSQL()
	assert.NoError(t, err)

	expected := `
	WITH richData AS (
		SELECT
			UNNEST(ARRAY[$1, NULL, $2, $3]::bigint[]) AS "id",
			UNNEST(ARRAY[$4, NULL, $5, $6]::text[]) AS "name"
	)
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			id, name
		FROM richData
		WHERE ($7=9)
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{1, 4, 19, "John Doe", "Mary Jane", "Curious George", 9}, args)
}

func TestSelectDocWithEmptyStructArray(t *testing.T) {
	type dataType struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	data := []*dataType{}
	sql, args, err := SelectDoc("id, name").
		From("richData").
		With("richData", data).
		Where("$1=9", 9).
		ToSQL()
	assert.NoError(t, err)

	expected := `
	WITH richData AS (
		SELECT
			UNNEST(ARRAY[]::bigint[]) AS "id",
			UNNEST(ARRAY[]::text[]) AS "name"
	)
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			id, name
		FROM richData
		WHERE ($1=9)
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{9}, args)
}

// Implements SQLType in the Postgres dialect
type customType int

func (customType) SQLType() string {
	return "numeric(17,2)"
}

func TestSelectDocWithCustomTypeArray(t *testing.T) {
	type dataType struct {
		ID    int        `db:"id"`
		Name  string     `db:"name"`
		Value customType `db:"value"`
	}
	data := []*dataType{
		&dataType{
			ID:    1,
			Name:  "John Doe",
			Value: 82,
		},
		&dataType{
			ID:    4,
			Name:  "Mary Jane",
			Value: 44,
		},
	}
	sql, args, err := SelectDoc("id, name, value").
		From("richData").
		With("richData", data).
		Where("$1=9", 9).
		ToSQL()
	assert.NoError(t, err)

	expected := `
	WITH richData AS (
		SELECT
			UNNEST(ARRAY[$1, $2]::bigint[]) AS "id",
			UNNEST(ARRAY[$3, $4]::text[]) AS "name",
			UNNEST(ARRAY[$5, $6]::numeric(17,2)[]) AS "value"
	)
	SELECT row_to_json(dat__item.*)
	FROM (
		SELECT
			id, name, value
		FROM richData
		WHERE ($7=9)
	) as dat__item
	`
	assert.Equal(t, stripWS(expected), stripWS(sql))
	assert.Equal(t, []interface{}{1, 4, "John Doe", "Mary Jane", customType(82), customType(44), 9}, args)
}
