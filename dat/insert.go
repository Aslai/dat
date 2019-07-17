package dat

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
)

// InsertBuilder contains the clauses for an INSERT statement
type InsertBuilder struct {
	Execer

	isInterpolated   bool
	table            string
	cols             []string
	isBlacklist      bool
	vals             [][]interface{}
	records          []interface{}
	onConflictTarget *onConflictTargetType
	onConflictAction *onConflictActionType
	returnings       []string
	err              error
}

// NewInsertBuilder creates a new InsertBuilder for the given table.
func NewInsertBuilder(table string) *InsertBuilder {
	if table == "" {
		logger.Error("InsertInto requires a table name.")
		return nil
	}
	return &InsertBuilder{table: table, isInterpolated: EnableInterpolation, onConflictTarget: &onConflictTargetType{}, onConflictAction: &onConflictActionType{}}
}

// Columns appends columns to insert in the statement
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	return b.Whitelist(columns...)
}

// Blacklist defines a blacklist of columns and should only be used
// in conjunction with Record.
func (b *InsertBuilder) Blacklist(columns ...string) *InsertBuilder {
	b.isBlacklist = true
	b.cols = columns
	return b
}

// Whitelist defines a whitelist of columns to be inserted. To
// specify all columns of a record use "*".
func (b *InsertBuilder) Whitelist(columns ...string) *InsertBuilder {
	b.cols = columns
	return b
}

// Values appends a set of values to the statement
func (b *InsertBuilder) Values(vals ...interface{}) *InsertBuilder {
	b.vals = append(b.vals, vals)
	return b
}

// Record pulls in values to match Columns from the record
func (b *InsertBuilder) Record(record interface{}) *InsertBuilder {
	b.records = append(b.records, record)
	return b
}

// The ON CONFLICT clause can be used to specify an alternative action to raising a unique constraint or exclusion constraint violation error
//     [ ON CONFLICT [ conflict_target ] conflict_action ]
//		where conflict_target can be one of:
//
//			( { index_column_name | ( index_expression ) } [ COLLATE collation ] [ opclass ] [, ...] ) [ WHERE index_predicate ]
//			ON CONSTRAINT constraint_name

type onConflictTargetType struct {
	column         string
	constraint     string
	indexPredicate string
}

// hasOneConflictTarget returns true if there exists one and only one of the possible conflict targets
func (t *onConflictTargetType) hasOneConflictTarget() bool {
	if t == nil {
		return false
	}

	hasColumn := len(t.column) != 0
	hasConstraint := len(t.constraint) != 0

	return ((hasColumn && !hasConstraint) || (!hasColumn && hasConstraint))
}

// The ON CONFLICT action can DO NOTHING or DO UPDATE SET with an optional WHERE clause
type onConflictActionType struct {
	action         string
	setClauses     []*setClause
	whereFragments []*whereFragment
}

// ON CONFLICT keywords
const updateAction = "UPDATE"
const excludedColumn = "EXCLUDED"

//     [ ON CONFLICT [ conflict_target ] conflict_action ]
//		where conflict_target can be one of:
//
//    ( { index_column_name | ( index_expression ) } [ COLLATE collation ] [ opclass ] [, ...] ) [ WHERE index_predicate ]
//    ON CONSTRAINT constraint_name

// OnConflictColumn is an ON CONFLICT clause with a column conflict_target
func (b *InsertBuilder) OnConflictColumn(column string) *InsertBuilder {
	b.onConflictTarget.column = column
	return b
}

// OnConflictConstraint is an ON CONFLICT clause with a constraint conflict_target
func (b *InsertBuilder) OnConflictConstraint(constraint string) *InsertBuilder {
	b.onConflictTarget.constraint = constraint
	return b
}

// OnConflictWhere is an ON CONFLICT clause with a column and index_predicate conflict_target
func (b *InsertBuilder) OnConflictWhere(column string, indexPredicate string) *InsertBuilder {
	b.onConflictTarget.column = column
	b.onConflictTarget.indexPredicate = indexPredicate
	return b
}

//     [ ON CONFLICT [ conflict_target ] conflict_action ]
//		and conflict_action is one of:
//
//			DO NOTHING
//			DO UPDATE SET { column_name = { expression | DEFAULT } |
//							( column_name [, ...] ) = ( { expression | DEFAULT } [, ...] ) |
//							( column_name [, ...] ) = ( sub-SELECT )
//						  } [, ...]
//					  [ WHERE condition ]

// Set may initiate a DO UPDATE conflict_action and sets a column/value pair
// If never called the conflict_action will default to DO NOTHING
func (b *InsertBuilder) Set(column string, value interface{}) *InsertBuilder {
	if !b.onConflictTarget.hasOneConflictTarget() {
		if b.err == nil {
			b.err = NewError("A conflict_target must be provided for ON CONFLICT DO UPDATE")
		}
		return b
	}

	b.onConflictAction.action = updateAction
	b.onConflictAction.setClauses = append(b.onConflictAction.setClauses, &setClause{column: column, value: value})
	return b
}

// SetMap may initiate a DO UPDATE conflict_action and sets the elements of the map as column/value pairs
func (b *InsertBuilder) SetMap(clauses map[string]interface{}) *InsertBuilder {
	for col, val := range clauses {
		b = b.Set(col, val)
	}
	return b
}

// Where appends a WHERE clause following a conflict_action of DO UPDATE
func (b *InsertBuilder) Where(whereSQLOrMap interface{}, args ...interface{}) *InsertBuilder {
	if b.onConflictAction.action != updateAction {
		if b.err == nil {
			b.err = NewError("conflict_action must be equal to UPDATE")
		}
		return b
	}

	fragment, err := newWhereFragment(whereSQLOrMap, args)
	if err != nil {
		b.err = err
	} else {
		b.onConflictAction.whereFragments = append(b.onConflictAction.whereFragments, fragment)
	}
	return b
}

// Returning sets the columns for the RETURNING clause
func (b *InsertBuilder) Returning(columns ...string) *InsertBuilder {
	b.returnings = columns
	return b
}

// Pair adds a key/value pair to the statement
func (b *InsertBuilder) Pair(column string, value interface{}) *InsertBuilder {
	b.cols = append(b.cols, column)
	lenVals := len(b.vals)
	if lenVals == 0 {
		args := []interface{}{value}
		b.vals = [][]interface{}{args}
	} else if lenVals == 1 {
		b.vals[0] = append(b.vals[0], value)
	} else {
		b.err = errors.New("pair only allows you to specify 1 record to insert")
	}
	return b
}

// ToSQL serialized the InsertBuilder to a SQL string
// It returns the string with placeholders and a slice of query arguments
func (b *InsertBuilder) ToSQL() (string, []interface{}, error) {
	if b.err != nil {
		return "", nil, b.err
	}

	if len(b.table) == 0 {
		return "", nil, NewError("no table specified")
	}
	lenCols := len(b.cols)
	lenRecords := len(b.records)
	if lenCols == 0 {
		return "", nil, NewError("no columns specified")
	}
	if len(b.vals) == 0 && lenRecords == 0 {
		return "", nil, NewError("no values or records specified")
	}

	if lenRecords == 0 && b.cols[0] == "*" {
		return "", nil, NewError(`"*" can only be used in conjunction with Record`)
	}

	if lenRecords == 0 && b.isBlacklist {
		return "", nil, NewError("Blacklist can only be used in conjunction with Record")
	}

	cols := b.cols

	// reflect fields removing blacklisted columns
	if lenRecords > 0 && b.isBlacklist {
		cols = reflectExcludeColumns(b.records[0], cols)
	}
	// reflect all fields
	if lenRecords > 0 && cols[0] == "*" {
		cols = reflectColumns(b.records[0])
	}

	var sql bytes.Buffer
	var args []interface{}

	sql.WriteString("INSERT INTO ")
	sql.WriteString(b.table)
	sql.WriteString(" (")

	for i, c := range cols {
		if i > 0 {
			sql.WriteRune(',')
		}
		writeIdentifier(&sql, c)
	}
	sql.WriteString(") VALUES ")

	start := 1
	// Go thru each value we want to insert. Write the placeholders, and collect args
	for i, row := range b.vals {
		if i > 0 {
			sql.WriteRune(',')
		}
		buildPlaceholders(&sql, start, len(row))

		for _, v := range row {
			args = append(args, v)
			start++
		}
	}
	anyVals := len(b.vals) > 0

	// Go thru the records. Write the placeholders, and do reflection on the records to extract args
	for i, rec := range b.records {
		if i > 0 || anyVals {
			sql.WriteRune(',')
		}

		ind := reflect.Indirect(reflect.ValueOf(rec))
		vals, err := valuesFor(ind.Type(), ind, cols)
		if err != nil {
			return "", nil, err
		}
		buildPlaceholders(&sql, start, len(vals))
		for _, v := range vals {
			args = append(args, v)
			start++
		}
	}

	// On conflict clause
	if b.onConflictTarget.hasOneConflictTarget() {
		sql.WriteString(" ON CONFLICT ")

		// conflict_target
		if len(b.onConflictTarget.column) > 0 {
			sql.WriteString("(" + b.onConflictTarget.column + ")")
			if len(b.onConflictTarget.indexPredicate) > 0 {
				sql.WriteString(" WHERE " + b.onConflictTarget.indexPredicate)
			}
		} else if len(b.onConflictTarget.constraint) > 0 {
			sql.WriteString("ON CONSTRAINT " + b.onConflictTarget.constraint)
		}

		// conflict_action
		if b.onConflictAction.action != updateAction {
			sql.WriteString(" DO NOTHING")
		} else {
			sql.WriteString(" DO UPDATE SET ")

			// Build DO UPDATE SET clause SQL with placeholders and add values to args
			placeholderStartPos := int64(start)
			for i, c := range b.onConflictAction.setClauses {
				if i > 0 {
					sql.WriteString(", ")
				}

				writeIdentifier(&sql, c.column)

				if e, ok := c.value.(*Expression); ok {
					startPos := placeholderStartPos
					sql.WriteString(" = ")
					// map relative $1, $2 placeholders to absolute
					remapPlaceholders(&sql, e.Sql, startPos)
					args = append(args, e.Args...)
					placeholderStartPos += int64(len(e.Args))
				} else if s, ok := c.value.(string); ok && s == excludedColumn+"."+c.column {
					// Leave EXCLUDED.column value unquoted
					sql.WriteString(" = ")
					sql.WriteString(s)
				} else {
					if placeholderStartPos < maxLookup {
						sql.WriteString(equalsPlaceholderTab[placeholderStartPos])
					} else {
						sql.WriteString(" = $")
						sql.WriteString(strconv.FormatInt(placeholderStartPos, 10))
					}
					placeholderStartPos++
					args = append(args, c.value)
				}
			}

			// DO UPDATE SET .. WHERE clause
			if len(b.onConflictAction.whereFragments) > 0 {
				sql.WriteString(" WHERE ")
				writeAndFragmentsToSQL(&sql, b.onConflictAction.whereFragments, &args, &placeholderStartPos)
			}
		}
	}

	// Go thru the returning clauses
	for i, c := range b.returnings {
		if i == 0 {
			sql.WriteString(" RETURNING ")
		} else {
			sql.WriteRune(',')
		}
		writeIdentifier(&sql, c)
	}

	return sql.String(), args, nil
}
