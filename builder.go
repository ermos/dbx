package dbx

import (
	"context"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type QB struct {
	db			*Instance
	object 		interface{}
	fields		map[string]string
	sql 		_sql
	jsonToSql 	map[string]string
	reference	map[string]string
	enums 		[]EnumDefinition
}

type _sql struct {
	Selected 	[]string
	From 		string
	Join		[]_join
	Where		[]_where
	GroupBy		[]string
	OrderBy		[]_order
	Limit 		int
	Offset 		int
}

type _join struct {
	Type		string
	Table 		string
	Query 		string
	Args		[]interface{}
}

type _where struct {
	Column 		string
	Sign 		string
	Value 		[]interface{}
}

type _order struct {
	Column 		string
	Sort 		string
}

func (i *Instance) NewBuilder (object interface{}) *QB {
	var qb QB

	qb.db = i
	qb.object = object

	qb._jsonToSQL()
	qb._reference()
	qb._enums()

	return &qb
}

func (qb *QB) Table (name string) *QB {
	qb.sql.From = name

	return qb
}

func (qb *QB) Select (args ...string) *QB {
	for _, arg := range args {
		qb.sql.Selected = append(qb.sql.Selected, arg)
	}

	return qb
}

func (qb *QB) Join (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("INNER JOIN", table, query, args...)
}

func (qb *QB) LeftJoin (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("LEFT JOIN", table, query, args...)
}

func (qb *QB) RightJoin (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("RIGHT JOIN", table, query, args...)
}

func (qb *QB) FullJoin (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("FULL JOIN", table, query, args...)
}

func (qb *QB) SelfJoin (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("SELF JOIN", table, query, args...)
}

func (qb *QB) NaturalJoin (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("NATURAL JOIN", table, query, args...)
}

func (qb *QB) CrossJoin (table string, query string, args ...interface{}) *QB {
	return qb._joinMethod("CROSS JOIN", table, query, args...)
}

func (qb *QB) _joinMethod (joinType string, table string, query string, args ...interface{}) *QB {
	qb.sql.Join = append(qb.sql.Join, _join{
		Type: joinType,
		Table: table,
		Query: query,
		Args: args,
	})

	return qb
}

func (qb *QB) Where (column string, sign string, value ...interface{}) *QB {
	_checkSign(sign, value...)

	qb.sql.Where = append(qb.sql.Where, _where{
		Column: column,
		Sign: sign,
		Value: value,
	})

	return qb
}

func _checkSign (sign string, value ...interface{}) {
	lenght := len(value)

	switch strings.ToUpper(sign) {
	case "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE":
		if lenght != 1 {
			log.Fatalf("query builder: value's length (%d) and the sign %s isn't compatible", lenght, sign)
		}
	case "IN", "NOT IN":
		if lenght < 1 {
			log.Fatalf("query builder: value's length (%d) and the sign %s isn't compatible", lenght, sign)
		}
	case "BETWEEN", "NOT BETWEEN":
		if lenght != 2 {
			log.Fatalf("query builder: value's length (%d) and the sign %s isn't compatible", lenght, sign)
		}
	case "IS NULL", "IS NOT NULL":
		if lenght != 0 {
			log.Fatalf("query builder: value's length (%d) and the sign %s isn't compatible", lenght, sign)
		}
	default:
		log.Fatalf("query builder: incorrect sign \"%s\"", sign)
	}
}

func (qb *QB) GroupBy (columns ...string) *QB {
	for _, column := range columns {
		qb.sql.GroupBy = append(qb.sql.GroupBy, column)
	}

	return qb
}

func (qb *QB) OrderBy (column string, sort string) *QB {
	switch strings.ToUpper(sort) {
	case "ASC", "DESC":
	default:
		log.Fatalf("query builder: incorrect sort \"%s\"", sort)
	}

	qb.sql.OrderBy = append(qb.sql.OrderBy, _order{
		Column: column,
		Sort: sort,
	})

	return qb
}

func (qb *QB) Limit (count int) *QB {
	qb.sql.Limit = count

	return qb
}

func (qb *QB) Offset (count int) *QB {
	qb.sql.Offset = count

	return qb
}

func (qb *QB) Queries (queries map[string]string) error {
	for json, value := range queries {
		switch json {
		case "limit":
			limit, err := strconv.Atoi(value)
			if err != nil {
				return err
			}

			qb.Limit(limit)
		case "offset":
			offset, err := strconv.Atoi(value)
			if err != nil {
				return err
			}

			qb.Offset(offset)
		default :
			err := qb._addQuery(json, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (qb *QB) _addQuery (head string, value string) error {
	value = strings.ReplaceAll(value, "%20", " ")

	rgx := regexp.MustCompile(`^(.*)%5B(.*)%5D$`)

	indexes := rgx.FindAllStringSubmatch(head, -1)
	if len(indexes) != 1 || len(indexes[0]) != 3 {
		return errors.New("can't parse query name => " + head)
	}

	json := indexes[0][1]
	cond := indexes[0][2]

	if qb.reference[json] != "" {
		json = qb.reference[json]
	}

	switch strings.ToLower(cond) {
	case "equal": // [equal]=23
		qb.Where(qb.jsonToSql[json], "=", value)
	case "not-equal": // [not-equal]=23
		qb.Where(qb.jsonToSql[json], "!=", value)
	case "like": // [like]=23
		qb.Where(qb.jsonToSql[json], "LIKE", "%" + value + "%")
	case "not-like": // [not-like]=23
		qb.Where(qb.jsonToSql[json], "NOT LIKE","%" + value + "%")
	case "in": // [in]={23,14}
		value = strings.TrimRight(strings.TrimLeft(value, "%5B"), "%5D")
		array := strings.Split(value, ",")

		s := make([]interface{}, len(array))
		for i, v := range array {
			s[i] = v
		}

		qb.Where(qb.jsonToSql[json], "IN", s...)
	case "not-in": // [not-in]={23,14}
		value = strings.TrimRight(strings.TrimLeft(value, "%5B"), "%5D")
		array := strings.Split(value, ",")

		s := make([]interface{}, len(array))
		for i, v := range array {
			s[i] = v
		}

		qb.Where(qb.jsonToSql[json], "NOT IN", s...)
	}

	return nil
}

func (qb *QB) _reference () {
	ref := make(map[string]string)

	build := reflect.ValueOf(qb.object).MethodByName("GetReferences")
	if build.IsValid() {
		res := build.Call(make([]reflect.Value, 0))

		if len(res) != 1 {
			log.Fatal("method GetReferences can't return more than one arg")
		}

		ref = res[0].Interface().(map[string]string)
	}

	qb.reference = ref
}

func (qb *QB) _enums () {
	build := reflect.ValueOf(qb.object).MethodByName("GetEnums")
	if build.IsValid() {
		res := build.Call(make([]reflect.Value, 0))

		if len(res) != 1 {
			log.Fatal("method GetEnums can't return more than one arg")
		}

		qb.enums = res[0].Interface().([]EnumDefinition)
	}
}

func (qb *QB) _jsonToSQL () {
	structure := reflect.TypeOf(qb.object)
	fields := make(map[string]string)

	for i := 0; i < structure.NumField(); i++ {
		s := structure.Field(i)
		json := s.Tag.Get("json")
		sql := s.Tag.Get("db")

		fields[json] = sql
	}

	qb.jsonToSql = fields
}

func (qb *QB) Get (ctx context.Context, dest interface{}) error {
	query, args :=  qb._query(false)
	return qb.db.GetAll(ctx, dest, query, args...)
}

func (qb *QB) GetWithMeta (ctx context.Context, dest interface{}) (Result, error) {
	var r Result

	query, args :=  qb._query(true)

	row := qb.db.QueryRow(ctx, query, args...)

	err := row.Scan(&r.Meta.Count)
	if err != nil {
		return r, err
	}

	r.Meta.Header = qb._header()

	query, args =  qb._query(false)

	err = qb.db.GetAll(ctx, dest, query, args...)
	if err != nil {
		return r, err
	}

	r.Body = dest

	return r, nil
}

func (qb *QB) GetScan (ctx context.Context) (*sqlx.Rows, error) {
	query, args :=  qb._query(false)
	return qb.db.Query(ctx, query, args...)
}

func (qb *QB) GetScanWithMeta (ctx context.Context) (*sqlx.Rows, Result, error) {
	var r Result

	query, args :=  qb._query(true)

	row := qb.db.QueryRow(ctx, query, args...)

	err := row.Scan(&r.Meta.Count)
	if err != nil {
		return nil, r, err
	}

	r.Meta.Header = qb._header()

	query, args = qb._query(false)

	rows, err := qb.db.Query(ctx, query, args...)

	return rows, r, err
}

func (qb *QB) _columns () map[string]string {
	columns := make(map[string]string)

	build := reflect.ValueOf(qb.object).MethodByName("GetColumns")
	if build.IsValid() {
		res := build.Call(make([]reflect.Value, 0))

		if len(res) != 1 {
			log.Fatal("method GetColumns can't return more than one arg")
		}

		columns = res[0].Interface().(map[string]string)
	}

	return columns
}

func (qb *QB) _header () map[string]MetaField {
	structure := reflect.TypeOf(qb.object)
	fields := make(map[string]MetaField)
	columns := qb._columns()

	for i := 0; i < structure.NumField(); i++ {
		var field MetaField

		s := structure.Field(i)
		json := s.Tag.Get("json")

		if json == "-" {
			continue
		}

		if columns[json] != "" {
			field.Name = columns[json]
		} else {
			continue
		}

		if qb.reference[json] != "" {
			field.Reference = qb.reference[json]
		}

		if qb.jsonToSql[json] == "" || qb.jsonToSql[json] == "-" {
			field.Virtual = true
		}

		field.Type = _selectType(s.Type.String())

		if field.Type == "enum" {
			for _, enumValue := range qb.enums {
				if enumValue.Key == json {
					field.Value = enumValue.Value
					field.Reference = enumValue.Reference
				}
			}
		}

		fields[json] = field
	}

	return fields
}

func _selectType (value string) string {
	switch value {
	case "int", "*int", "float64", "*float64", "float32", "*float32":
		return "number"
	case "bool", "*bool":
		return "bool"
	case "time.Time", "*time.Time":
		return "date"
	case "string", "*string":
		return "string"
	case "dbx.Money", "*dbx.Money":
		return "money"
	case "dbx.Enum", "*dbx.Enum":
		return "enum"
	default:
		return "object"
	}
}

func (qb *QB) _query (count bool) (query string, args []interface{}) {
	// Select
	query += fmt.Sprintln("SELECT")

	if !count {
		for i, selector := range qb.sql.Selected {
			if i == 0 {
				query += selector
			} else {
				query += fmt.Sprintf(", %s", selector)
			}
		}
	} else {
		query += "count(*)"
	}

	// From
	query += fmt.Sprintf("\nFROM\n%s", qb.sql.From)

	// Join
	for _, join := range qb.sql.Join {
		query += fmt.Sprintf("\n%s\n%s ON %s", join.Type, join.Table, join.Query)

		for _, arg := range join.Args {
			args = append(args, arg)
		}
	}

	// Where
	for i, where := range qb.sql.Where {
		if i == 0 {
			query += "\nWHERE"
			query += _wherePrint(where.Column, where.Sign, len(where.Value))
		} else {
			query += "\nAND"
			query += _wherePrint(where.Column, where.Sign, len(where.Value))
		}



		for _, arg := range where.Value {
			args = append(args, arg)
		}
	}

	// Group By
	for i, groupBy := range qb.sql.GroupBy {
		if i == 0 {
			query += "\nGROUP BY"
			query += fmt.Sprintf("\n%s", groupBy)
		} else {
			query += fmt.Sprintf(",\n%s", groupBy)
		}
	}


	// Order By
	for i, orderBy := range qb.sql.OrderBy {
		if i == 0 {
			query += "\nORDER BY"
			query += fmt.Sprintf("\n%s %s", orderBy.Column, orderBy.Sort)
		} else {
			query += fmt.Sprintf(",\n%s %s", orderBy.Column, orderBy.Sort)
		}
	}

	if qb.sql.Limit != 0 && !count {
		// Limit
		query += fmt.Sprintf("\nLIMIT\n%d", qb.sql.Limit)

		// Offset
		query += fmt.Sprintf("\nOFFSET\n%d", qb.sql.Offset)
	}

	return
}

func _wherePrint (column string, sign string, length int) string {
	var query string

	var joker string
	for i := 0 ; i < length ; i++ {
		if i == 0 {
			joker += "?"
		} else {
			joker += ",?"
		}
	}

	switch strings.ToUpper(sign) {
	case "=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE":
		query += fmt.Sprintf("\n%s %s %s", column, sign, joker)
	case "IN", "NOT IN":
		query += fmt.Sprintf("\n%s %s (%s)", column, sign, joker)
	case "BETWEEN", "NOT BETWEEN":
		query += fmt.Sprintf("\n%s %s ? AND ?", column, sign)
	case "IS NULL", "IS NOT NULL":
		query += fmt.Sprintf("\n%s %s", column, sign)
	}

	return query
}