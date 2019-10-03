package dat

import "errors"

// SelectBuilder contains the clauses for a SELECT statement
type SelectBuilder struct {
	Execer

	isDistinct      bool
	distinctColumns []string
	isInterpolated  bool
	columns         []string
	fors            []string
	tableFragments  []*whereFragment
	joinFragments   []*whereFragment
	whereFragments  []*whereFragment
	groupBys        []string
	havingFragments []*whereFragment
	orderBys        []*whereFragment
	limitCount      uint64
	limitValid      bool
	offsetCount     uint64
	offsetValid     bool
	scope           Scope
	err             error
}

// NewSelectBuilder creates a new SelectBuilder for the given columns
func NewSelectBuilder(columns ...string) *SelectBuilder {
	b := &SelectBuilder{isInterpolated: EnableInterpolation}
	b.Columns(columns...)
	return b
}

// Columns adds additional select columns to the builder.
func (b *SelectBuilder) Columns(columns ...string) *SelectBuilder {
	if len(columns) == 0 || columns[0] == "" {
		b.err = errors.New("Select requires 1 or more columns")
		return nil
	}

	b.columns = append(b.columns, columns...)
	return b
}

// Distinct marks the statement as a DISTINCT SELECT
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.isDistinct = true
	return b
}

// DistinctOn sets the columns for DISTINCT ON
func (b *SelectBuilder) DistinctOn(columns ...string) *SelectBuilder {
	b.isDistinct = true
	b.distinctColumns = columns
	return b
}

// From sets the table to SELECT FROM. JOINs may also be defined here.
func (b *SelectBuilder) From(fromStr string, args ...interface{}) *SelectBuilder {
	fragment, err := newWhereFragment(fromStr, args)
	if err != nil {
		b.err = err
		return b
	}
	b.tableFragments = append(b.tableFragments, fragment)
	return b
}

func (b *SelectBuilder) implJoin(joinStr string, args ...interface{}) *SelectBuilder {
	fragment, err := newWhereFragment(joinStr, args)
	if err != nil {
		b.err = err
		return b
	}
	b.joinFragments = append(b.joinFragments, fragment)
	return b
}

// Join appends an inner join to a FROM
func (b *SelectBuilder) Join(joinStr string, args ...interface{}) *SelectBuilder {
	return b.implJoin("INNER JOIN "+joinStr, args...)
}

// LeftJoin appends an left outer join to a FROM
func (b *SelectBuilder) LeftJoin(joinStr string, args ...interface{}) *SelectBuilder {
	return b.implJoin("LEFT JOIN "+joinStr, args...)
}

// RightJoin appends a right outer join to a FROM
func (b *SelectBuilder) RightJoin(joinStr string, args ...interface{}) *SelectBuilder {
	return b.implJoin("RIGHT JOIN "+joinStr, args...)
}

// FullOuterJoin appends a full outer join to a FROM
func (b *SelectBuilder) FullOuterJoin(joinStr string, args ...interface{}) *SelectBuilder {
	return b.implJoin("FULL OUTER JOIN "+joinStr, args...)
}

// For adds FOR clause to SELECT.
func (b *SelectBuilder) For(options ...string) *SelectBuilder {
	b.fors = options
	return b
}

// ScopeMap uses a predefined scope in place of WHERE.
func (b *SelectBuilder) ScopeMap(mapScope *MapScope, m M) *SelectBuilder {
	b.scope = mapScope.mergeClone(m)
	return b
}

// Scope uses a predefined scope in place of WHERE.
func (b *SelectBuilder) Scope(sql string, args ...interface{}) *SelectBuilder {
	b.scope = ScopeFunc(func(table string) (string, []interface{}) {
		return sql, args
	})
	return b
}

// Where appends a WHERE clause to the statement for the given string and args
// or map of column/value pairs
func (b *SelectBuilder) Where(whereSQLOrMap interface{}, args ...interface{}) *SelectBuilder {
	fragment, err := newWhereFragment(whereSQLOrMap, args)
	if err != nil {
		b.err = err
		return b
	}
	b.whereFragments = append(b.whereFragments, fragment)
	return b
}

// GroupBy appends a column to group the statement
func (b *SelectBuilder) GroupBy(group string) *SelectBuilder {
	b.groupBys = append(b.groupBys, group)
	return b
}

// Having appends a HAVING clause to the statement
func (b *SelectBuilder) Having(whereSQLOrMap interface{}, args ...interface{}) *SelectBuilder {
	fragment, err := newWhereFragment(whereSQLOrMap, args)
	if err != nil {
		b.err = err
	} else {
		b.havingFragments = append(b.havingFragments, fragment)
	}
	return b
}

// OrderBy appends a column to ORDER the statement by
func (b *SelectBuilder) OrderBy(whereSQLOrMap interface{}, args ...interface{}) *SelectBuilder {
	fragment, err := newWhereFragment(whereSQLOrMap, args)
	if err != nil {
		b.err = err
	} else {
		b.orderBys = append(b.orderBys, fragment)
	}
	return b
}

// Limit sets a limit for the statement; overrides any existing LIMIT
func (b *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	b.limitCount = limit
	b.limitValid = true
	return b
}

// Offset sets an offset for the statement; overrides any existing OFFSET
func (b *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	b.offsetCount = offset
	b.offsetValid = true
	return b
}

// Paginate sets LIMIT/OFFSET for the statement based on the given page/perPage
// Assumes page/perPage are valid. Page and perPage must be >= 1
func (b *SelectBuilder) Paginate(page, perPage uint64) *SelectBuilder {
	b.Limit(perPage)
	b.Offset((page - 1) * perPage)
	return b
}

// ToSQL serialized the SelectBuilder to a SQL string
// It returns the string with placeholders and a slice of query arguments
func (b *SelectBuilder) ToSQL() (string, []interface{}, error) {
	if b.err != nil {
		return NewDatSQLErr(b.err)
	}

	if len(b.columns) == 0 {
		return NewDatSQLError("no columns specified")
	}
	if len(b.tableFragments) == 0 && len(b.joinFragments) > 0 {
		return NewDatSQLError("joins may only be attached if a from target is specified")
	}

	buf := bufPool.Get()
	defer bufPool.Put(buf)
	var args []interface{}

	buf.WriteString("SELECT ")

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

	var placeholderStartPos int64 = 1
	from := ""
	fromBuf := bufPool.Get()
	defer bufPool.Put(fromBuf)
	if len(b.tableFragments) > 0 {
		buf.WriteString(" FROM ")
		writeCommaFragmentsToSQL(fromBuf, b.tableFragments, &args, &placeholderStartPos)
		writeConcatFragmentsToSQL(fromBuf, b.joinFragments, &args, &placeholderStartPos)
		from = fromBuf.String()
		buf.WriteString(from)
	}

	whereFragments := b.whereFragments
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

	return buf.String(), args, nil
}
