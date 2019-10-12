package dat

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
	union            *Expression
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
	b.err = storeExpr(&b.subQueriesWith, "SelectDocBuilder.With", column, sqlOrBuilder, a...)
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

func (b *SelectDocBuilder) Union(sqlOrBuilder interface{}, a ...interface{}) *SelectDocBuilder {
	switch t := sqlOrBuilder.(type) {
	default:
		b.err = NewError("SelectDocBuilder.Union: sqlOrbuilder accepts only {string, Builder, *SelectDocBuilder} type")
	case *JSQLBuilder:
		t.isParent = false
		sql, args, err := t.ToSQL()
		if err != nil {
			b.err = err
			return b
		}
		b.union = Expr(sql, args...)
	case *SelectDocBuilder:
		t.isParent = false
		sql, args, err := t.ToSQL()
		if err != nil {
			b.err = err
			return b
		}
		b.union = Expr(sql, args...)
	case Builder:
		sql, args, err := t.ToSQL()
		if err != nil {
			b.err = err
			return b
		}
		b.union = Expr(sql, args...)
	case string:
		b.union = Expr(t, a...)
	}
	return b
}

// ToSQL serialized the SelectBuilder to a SQL string
// It returns the string with placeholders and a slice of query arguments
func (b *SelectDocBuilder) ToSQL() (string, []interface{}, error) {
	if b.err != nil {
		return NewDatSQLErr(b.err)
	}

	if len(b.columns) == 0 {
		return NewDatSQLError("no columns specified")
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
			buf.WriteString(" ")
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

	if b.union != nil {
		buf.WriteString(" UNION ")
		b.union.WriteRelativeArgs(buf, &args, &placeholderStartPos)
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
