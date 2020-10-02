package dat

import (
	"fmt"
	"reflect"
)

type subInfo struct {
	*Expression
	alias string
}

// SelectDocBuilder builds SQL that returns a JSON row.
type SelectDocBuilder struct {
	*SelectBuilder
	subQueriesWith   []*subInfo
	subQueriesMany   []*subInfo
	subQueriesOne    []*subInfo
	subQueriesVector []*subInfo
	subQueriesScalar []*subInfo
	innerSQL         *Expression
	union            []*subInfo // alias is used to encode whether or not this is a union or union all - This lets us easily preserve ordering and reuses code
	isParent         bool
	err              error
}

// NewSelectDocBuilder creates an instance of SelectDocBuilder.
func NewSelectDocBuilder(columns ...string) *SelectDocBuilder {
	sb := NewSelectBuilder(columns...)
	return &SelectDocBuilder{SelectBuilder: sb, isParent: true}
}

// InnerSQL sets the SQL after the SELECT (columns...) statement
//
// DEPRECATE this
func (b *SelectDocBuilder) InnerSQL(sql string, a ...interface{}) *SelectDocBuilder {
	b.innerSQL = Expr(sql, a...)
	return b
}

func storeExpr(destination *[]*subInfo, name string, column string, sqlOrBuilder interface{}, a ...interface{}) error {
	var err error
	switch t := sqlOrBuilder.(type) {
	default:
		err = NewError(name + ": sqlOrbuilder accepts only {string, Builder, *SelectDocBuilder} type")
	case *JSQLBuilder:
		t.isParent = false
		sql, args, err := t.ToSQL()
		if err != nil {
			return err
		}
		*destination = append(*destination, &subInfo{Expr(sql, args...), column})
	case *SelectDocBuilder:
		t.isParent = false
		sql, args, err := t.ToSQL()
		if err != nil {
			return err
		}
		*destination = append(*destination, &subInfo{Expr(sql, args...), column})
	case Builder:
		sql, args, err := t.ToSQL()
		if err != nil {
			return err
		}
		*destination = append(*destination, &subInfo{Expr(sql, args...), column})
	case string:
		*destination = append(*destination, &subInfo{Expr(t, a...), column})
	}
	return err
}

// With loads a sub query that will be inserted as a "with" table
func (b *SelectDocBuilder) With(column string, sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	if reflect.TypeOf(sqlOrBuilder).Kind() == reflect.Slice {
		sqlOrBuilder, a, b.err = arrayToTable(sqlOrBuilder)
	}
	if b.err == nil {
		b.err = storeExpr(&b.subQueriesWith, "SelectDocBuilder.With", column, sqlOrBuilder, a...)
	}
	return b
}

// Many loads a sub query resulting in an array of rows as an alias.
func (b *SelectDocBuilder) Many(column string, sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	b.err = storeExpr(&b.subQueriesMany, "SelectDocBuilder.Many", column, sqlOrBuilder, a...)
	return b
}

// Vector loads a sub query resulting in an array of homogeneous scalars as an alias.
func (b *SelectDocBuilder) Vector(column string, sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	b.err = storeExpr(&b.subQueriesVector, "SelectDocBuilder.Vector", column, sqlOrBuilder, a...)
	return b
}

// One loads a query resulting in a single row as an alias.
func (b *SelectDocBuilder) One(column string, sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	b.err = storeExpr(&b.subQueriesOne, "SelectDocBuilder.One", column, sqlOrBuilder, a...)
	return b
}

// Scalar loads a query resulting in a single scalar as an alias and embeds the scalar in the parent object, rather than as a child object
func (b *SelectDocBuilder) Scalar(column string, sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	b.err = storeExpr(&b.subQueriesScalar, "SelectDocBuilder.Scalar", column, sqlOrBuilder, a...)
	return b
}

// Union will add a SQL expression to the query with a UNION directive
func (b *SelectDocBuilder) Union(sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	b.err = storeExpr(&b.union, "SelectDocBuilder.Union", " ", sqlOrBuilder, a...)
	return b
}

// UnionAll will add a SQL expression to the query with a UNION ALL directive
func (b *SelectDocBuilder) UnionAll(sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	b.err = storeExpr(&b.union, "SelectDocBuilder.UnionAll", " ALL ", sqlOrBuilder, a...)
	return b
}

// Whitelist will drop any named columns from the query that are not included in the whitelist. An empty parameter list is a no-op. Columns with a trailing * character are treated as a prefix match instead of whole-word match. This does _not_ affect union queries.
func (b *SelectDocBuilder) Whitelist(columns ...string) *SelectDocBuilder {
	if len(columns) == 0 {
		return b
	}
	matchColumns := make([]string, 0, len(columns))
	eqColumns := make([]string, 0, len(columns))
	for _, c := range columns {
		if len(c) == 0 {
			continue
		}
		if c == "*" {
			return b
		}
		if c[len(c)-1] == '*' {
			matchColumns = append(matchColumns, c[:len(c)-1])
		} else {
			eqColumns = append(eqColumns, c)
		}
	}
	argList := make([]**subInfo, 0, len(b.subQueriesMany)+len(b.subQueriesOne)+len(b.subQueriesVector)+len(b.subQueriesScalar)) // Double pointer so that we can both nullify our entry in this list as well as the list that the argument is from
	for i := range b.subQueriesMany {
		argList = append(argList, &b.subQueriesMany[i])
	}
	for i := range b.subQueriesOne {
		argList = append(argList, &b.subQueriesOne[i])
	}
	for i := range b.subQueriesVector {
		argList = append(argList, &b.subQueriesVector[i])
	}
	for i := range b.subQueriesScalar {
		argList = append(argList, &b.subQueriesScalar[i])
	}
	for i, arg := range argList {
		for _, c := range matchColumns {
			if len(c) <= len((*arg).alias) && (*arg).alias[:len(c)] == c {
				// Prefix matches, remove from the list
				argList[i] = nil
			}
		}
		if argList[i] != nil {
			for _, c := range eqColumns {
				if (*arg).alias == c {
					// Column matches, remove from the list
					argList[i] = nil
				}
			}
		}
		if argList[i] != nil {
			// No match found, do an indirect erase
			*argList[i] = nil
		}
	}
	return b
}

// ToSQL serialized the SelectBuilder to a SQL string
// It returns the string with placeholders and a slice of query arguments
func (b *SelectDocBuilder) ToSQL() (string, []interface{}, error) {
	if b.err != nil {
		return NewDatSQLErr(b.err)
	}

	if len(b.columns)+len(b.subQueriesMany)+len(b.subQueriesOne)+len(b.subQueriesScalar)+len(b.subQueriesVector) == 0 {
		return NewDatSQLError("no columns specified")
	}

	if len(b.tableFragments) == 0 && len(b.joinFragments) > 0 {
		return NewDatSQLError("joins may only be attached if a from target is specified")
	}

	buf := bufPool.Get()
	defer bufPool.Put(buf)
	var args []interface{}
	var placeholderStartPos int64 = 1

	/*
		SELECT
			row_to_json(item.*)
		FROM (
			SELECT 	ID,
				NAME,
				(
					select ARRAY_AGG(dat__1.*)
					from (
						SELECT ID, user_id, title FROM posts WHERE posts.user_id = people.id
					) as dat__1
				) as posts
			FROM
				people
			WHERE
				ID in (1, 2)
		) as item
	*/

	for i, sub := range b.subQueriesWith {
		if i == 0 {
			buf.WriteString("WITH ")
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(sub.alias)
		buf.WriteString(" AS (")
		sub.WriteRelativeArgs(buf, &args, &placeholderStartPos)
		buf.WriteString(") ")
	}

	if b.isParent {
		//buf.WriteString("SELECT convert_to(row_to_json(dat__item.*)::text, 'UTF8') FROM ( SELECT ")
		buf.WriteString("SELECT row_to_json(dat__item.*) FROM ( SELECT ")
	} else {
		buf.WriteString("SELECT ")
	}

	if b.isDistinct {
		if len(b.distinctColumns) == 0 {
			buf.WriteString("DISTINCT ")
		} else {
			buf.WriteString("DISTINCT ON (")
			for i, s := range b.distinctColumns {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(s)
			}
			buf.WriteString(") ")
		}
	}

	for i, s := range b.columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(s)
	}

	/*
		(
			select ARRAY_AGG(dat__1.*)
			from (
				SELECT ID, user_id, title FROM posts WHERE posts.user_id = people.id
			) as dat__1
		) as posts
	*/

	for _, sub := range b.subQueriesMany {
		if sub == nil {
			continue
		}
		buf.WriteString(", (SELECT array_agg(dat__")
		buf.WriteString(sub.alias)
		buf.WriteString(".*) FROM (")
		sub.WriteRelativeArgs(buf, &args, &placeholderStartPos)
		buf.WriteString(") AS dat__")
		buf.WriteString(sub.alias)
		buf.WriteString(") AS ")
		writeQuotedIdentifier(buf, sub.alias)
	}

	for _, sub := range b.subQueriesVector {
		if sub == nil {
			continue
		}
		buf.WriteString(", (SELECT array_agg(dat__")
		buf.WriteString(sub.alias)
		buf.WriteString(".dat__scalar) FROM (")
		sub.WriteRelativeArgs(buf, &args, &placeholderStartPos)
		buf.WriteString(") AS dat__")
		buf.WriteString(sub.alias)
		buf.WriteString("(dat__scalar)) AS ")
		Dialect.WriteIdentifier(buf, sub.alias)
	}

	for _, sub := range b.subQueriesOne {
		if sub == nil {
			continue
		}
		buf.WriteString(", (SELECT row_to_json(dat__")
		buf.WriteString(sub.alias)
		buf.WriteString(".*) FROM (")
		sub.WriteRelativeArgs(buf, &args, &placeholderStartPos)
		buf.WriteString(") AS dat__")
		buf.WriteString(sub.alias)
		buf.WriteString(") AS ")
		writeQuotedIdentifier(buf, sub.alias)
	}

	for _, sub := range b.subQueriesScalar {
		if sub == nil {
			continue
		}
		buf.WriteString(", (SELECT dat__")
		buf.WriteString(sub.alias)
		buf.WriteString(".dat__scalar FROM (")
		sub.WriteRelativeArgs(buf, &args, &placeholderStartPos)
		buf.WriteString(") AS dat__")
		buf.WriteString(sub.alias)
		buf.WriteString("(dat__scalar) limit 1) AS ")
		writeQuotedIdentifier(buf, sub.alias)
	}

	if b.innerSQL != nil {
		b.innerSQL.WriteRelativeArgs(buf, &args, &placeholderStartPos)
	} else {
		whereFragments := b.whereFragments
		from := ""
		fromBuf := bufPool.Get()
		defer bufPool.Put(fromBuf)
		if len(b.tableFragments) > 0 {
			buf.WriteString(" FROM ")
			writeCommaFragmentsToSQL(fromBuf, b.tableFragments, &args, &placeholderStartPos)
			fromBuf.WriteString(" ")
			writeConcatFragmentsToSQL(fromBuf, b.joinFragments, &args, &placeholderStartPos)
			from = fromBuf.String()
			buf.WriteString(from)
		}

		if b.scope != nil {
			var where string
			sql, args2 := b.scope.ToSQL(from)
			sql, where = splitWhere(sql)
			buf.WriteString(sql)
			if where != "" {
				fragment, err := newWhereFragment(where, args2)
				if err != nil {
					return NewDatSQLErr(err)
				}
				whereFragments = append(whereFragments, fragment)
			}
		}

		if len(whereFragments) > 0 {
			buf.WriteString(" WHERE ")
			writeAndFragmentsToSQL(buf, whereFragments, &args, &placeholderStartPos)
		}

		// if b.scope == nil {
		// 	if len(whereFragments) > 0 {
		// 		buf.WriteString(" WHERE ")
		// 		writeWhereFragmentsToSql(buf, whereFragments, &args, &placeholderStartPos)
		// 	}
		// } else {
		// 	whereFragment := newWhereFragment(b.scope.ToSQL(b.table))
		// 	writeScopeCondition(buf, whereFragment, &args, &placeholderStartPos)
		// }

		for _, sub := range b.union {
			if sub == nil {
				continue
			}
			buf.WriteString(" UNION ")
			buf.WriteString(sub.alias)
			buf.WriteString(" ")
			sub.WriteRelativeArgs(buf, &args, &placeholderStartPos)
			buf.WriteString(" ")
		}

		if len(b.groupBys) > 0 {
			buf.WriteString(" GROUP BY ")
			for i, s := range b.groupBys {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(s)
			}
		}

		if len(b.havingFragments) > 0 {
			buf.WriteString(" HAVING ")
			writeAndFragmentsToSQL(buf, b.havingFragments, &args, &placeholderStartPos)
		}

		if len(b.orderBys) > 0 {
			buf.WriteString(" ORDER BY ")
			writeCommaFragmentsToSQL(buf, b.orderBys, &args, &placeholderStartPos)
		}

		if b.limitValid {
			buf.WriteString(" LIMIT ")
			writeUint64(buf, b.limitCount)
		}

		if b.offsetValid {
			buf.WriteString(" OFFSET ")
			writeUint64(buf, b.offsetCount)
		}

		// add FOR clause
		if len(b.fors) > 0 {
			buf.WriteString(" FOR")
			for _, s := range b.fors {
				buf.WriteString(" ")
				buf.WriteString(s)
			}
		}
	}

	if b.isParent {
		buf.WriteString(`) as dat__item`)
	}
	return buf.String(), args, nil
}

//// Override functions from SelectBuilder to return an instance of SelectDocBuilder.

// Columns adds additional select columns to the builder.
func (b *SelectDocBuilder) Columns(columns ...string) *SelectDocBuilder {
	b.SelectBuilder.Columns(columns...)
	return b
}

// Distinct marks the statement as a DISTINCT SELECT
func (b *SelectDocBuilder) Distinct() *SelectDocBuilder {
	b.SelectBuilder.Distinct()
	return b
}

// DistinctOn sets the columns for DISTINCT ON
func (b *SelectDocBuilder) DistinctOn(columns ...string) *SelectDocBuilder {
	b.SelectBuilder.DistinctOn(columns...)
	return b
}

// From sets the table to SELECT FROM. JOINs may also be defined here.
func (b *SelectDocBuilder) From(fromStr string, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.From(fromStr, args...)
	return b
}

// Join appends an inner join to a FROM
func (b *SelectDocBuilder) Join(joinStr string, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.Join(joinStr, args...)
	return b
}

// LeftJoin appends an left outer join to a FROM
func (b *SelectDocBuilder) LeftJoin(joinStr string, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.LeftJoin(joinStr, args...)
	return b
}

// RightJoin appends a right outer join to a FROM
func (b *SelectDocBuilder) RightJoin(joinStr string, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.RightJoin(joinStr, args...)
	return b
}

// FullOuterJoin appends a full outer join to a FROM
func (b *SelectDocBuilder) FullOuterJoin(joinStr string, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.FullOuterJoin(joinStr, args...)
	return b
}

// For adds FOR clause to SELECT.
func (b *SelectDocBuilder) For(options ...string) *SelectDocBuilder {
	b.SelectBuilder.For(options...)
	return b
}

// ScopeMap uses a predefined scope in place of WHERE.
func (b *SelectDocBuilder) ScopeMap(mapScope *MapScope, m M) *SelectDocBuilder {
	b.SelectBuilder.ScopeMap(mapScope, m)
	return b
}

// Scope uses a predefined scope in place of WHERE.
func (b *SelectDocBuilder) Scope(sql string, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.Scope(sql, args...)
	return b
}

// Where appends a WHERE clause to the statement for the given string and args
// or map of column/value pairs
func (b *SelectDocBuilder) Where(whereSQLOrMap interface{}, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.Where(whereSQLOrMap, args...)
	return b
}

// GroupBy appends a column to group the statement
func (b *SelectDocBuilder) GroupBy(group string) *SelectDocBuilder {
	b.SelectBuilder.GroupBy(group)
	return b
}

// Having appends a HAVING clause to the statement
func (b *SelectDocBuilder) Having(whereSQLOrMap interface{}, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.Having(whereSQLOrMap, args...)
	return b
}

// OrderBy appends a column to ORDER the statement by
func (b *SelectDocBuilder) OrderBy(whereSQLOrMap interface{}, args ...interface{}) *SelectDocBuilder {
	b.SelectBuilder.OrderBy(whereSQLOrMap, args...)
	return b
}

// Limit sets a limit for the statement; overrides any existing LIMIT
func (b *SelectDocBuilder) Limit(limit uint64) *SelectDocBuilder {
	b.SelectBuilder.Limit(limit)
	return b
}

// Offset sets an offset for the statement; overrides any existing OFFSET
func (b *SelectDocBuilder) Offset(offset uint64) *SelectDocBuilder {
	b.SelectBuilder.Offset(offset)
	return b
}

// Paginate sets LIMIT/OFFSET for the statement based on the given page/perPage
// Assumes page/perPage are valid. Page and perPage must be >= 1
func (b *SelectDocBuilder) Paginate(page, perPage uint64) *SelectDocBuilder {
	b.SelectBuilder.Paginate(page, perPage)
	return b
}

// arrayToTable accepts an array of structs or scalars and returns a query + args that can be embedded in a sub-table or query. If a struct array is passed,
// then `db` struct tags will inform the aliases for each column. Otherwise, the alias of the column will be `data`.
func arrayToTable(contents interface{}) (string, []interface{}, error) {
	val := reflect.ValueOf(contents)
	typ := val.Type()
	if typ.Kind() != reflect.Slice {
		return "", nil, NewError("arrayToTable can only take slices")
	}
	innerTyp := typ.Elem()
	if innerTyp.Kind() == reflect.Ptr {
		innerTyp = innerTyp.Elem()
	}
	buf := bufPool.Get()
	defer bufPool.Put(buf)
	var args []interface{}
	var placeholderStartPos int64 = 1

	if innerTyp.Kind() != reflect.Struct {
		buf.WriteString("SELECT UNNEST(ARRAY[")
		for i := 0; i < val.Len(); i++ {
			if i != 0 {
				buf.WriteRune(',')
			}
			buf.WriteString(fmt.Sprintf("$%d", placeholderStartPos))
			args = append(args, val.Index(i).Interface())
			placeholderStartPos++
		}
		buf.WriteString("]::")
		Dialect.WriteReflectedType(buf, reflect.SliceOf(innerTyp))
		buf.WriteString(") AS data ")
		return buf.String(), args, nil
	}
	writtenArrays := 0
	buf.WriteString("SELECT ")
	for i := 0; i < innerTyp.NumField(); i++ {
		field := innerTyp.Field(i)
		alias := field.Tag.Get("db")
		if alias != "" {
			switch field.Type.Kind() {
			case reflect.Struct:
				return "", nil, NewError("Temporary tables cannot be built from nested structs")
			}
			if writtenArrays != 0 {
				buf.WriteRune(',')
			}
			writtenArrays++
			buf.WriteString(" UNNEST(ARRAY[")
			for j := 0; j < val.Len(); j++ {
				if j != 0 {
					buf.WriteRune(',')
				}
				value := val.Index(j)
				if value.Kind() == reflect.Ptr {
					if value.IsNil() {
						buf.WriteString("NULL")
						continue
					}
					value = value.Elem()
				}
				buf.WriteString(fmt.Sprintf("$%d", placeholderStartPos))
				placeholderStartPos++
				args = append(args, value.Field(i).Interface())
			}
			buf.WriteString("]::")
			Dialect.WriteReflectedType(buf, reflect.SliceOf(field.Type))
			buf.WriteString(") AS ")
			writeQuotedIdentifier(buf, alias)
		}
	}
	return buf.String(), args, nil
}
