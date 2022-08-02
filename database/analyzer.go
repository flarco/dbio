package database

import (
	"io/ioutil"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

/*
we have tables and where have no info on primary keys.

1. determine all PKs
- get columns
- get a sample, and iterate on each row. do
select
	count(1) tot_cnt,
	count({field}) {field}_cnt,
	count({field}) {field}_distct_cnt,
	min,
	max,
	min_len,
	max_len
from (
	select * from {schema}.{table}
	limit {limit}
) t

on each row, determine the unique ones and cross-reference on potential join matches and types
*/

type DataAnalyzerOptions struct {
	DbName      string
	SchemaNames []string
}

type DataAnalyzer struct {
	Conn        Connection
	Schemata    Schemata
	ColumnMap   map[string]Column
	RelationMap map[string]map[string]Relation
	Options     DataAnalyzerOptions
}

type Relation string

const RelationOneToOne = "one_to_one"
const RelationOneToMany = "one_to_many"
const RelationManyToOne = "many_to_one"
const RelationManyToMany = "many_to_many"

func NewDataAnalyzer(conn Connection, opts DataAnalyzerOptions) (da *DataAnalyzer, err error) {
	if len(opts.SchemaNames) == 0 {
		err = g.Error("must provide SchemaNames")
		return
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "could not connect to database")
		return
	}

	da = &DataAnalyzer{
		Conn:        conn,
		Options:     opts,
		ColumnMap:   map[string]Column{},
		RelationMap: map[string]map[string]Relation{},
	}

	return
}

func (da *DataAnalyzer) GetSchemata(force bool) (err error) {
	if !(force || da.Schemata.Databases == nil) {
		return nil
	}

	for _, schema := range da.Options.SchemaNames {
		g.Debug("getting schemata for %s", schema)
		da.Schemata, err = da.Conn.GetSchemata(schema, "")
		if err != nil {
			err = g.Error(err, "could not get schemata")
			return
		}
	}

	return
}

var sqlAnalyzeColumns = `
	select {cols_sql}
	from ( select * from "{schema}"."{table}" limit {limit} ) t
`

type StatFieldSQL struct {
	Name        string
	TemplateSQL string
}

// TODO: templatize to other databases: Snowflake, BigQuery
var statsFields = []StatFieldSQL{
	{"row_count", `count(1)`},
	{"value_count", `count("{field}")`},
	{"distinct_count", `count(distinct "{field}")`},
	{"length_minimun", `min(length("{field}"::text))`},
	{"length_maximum", `max(length("{field}"::text))`},
	// {"value_minimun", "min({field}::text)"}, // pulls customer data
	// {"value_maximum", "max({field}::text)"}, // pulls customer data
}

func (da *DataAnalyzer) AnalyzeColumns(sampleSize int) (err error) {
	err = da.GetSchemata(false)
	if err != nil {
		err = g.Error(err, "could not get schemata")
		return
	}

	for _, table := range da.Schemata.Tables() {
		g.Info("analyzing table %s", table.FullName())

		tableColMap := table.ColumnsMap()
		// g.PP(tableColMap)

		// need order to retrieve values
		cols := lo.Filter(lo.Values(tableColMap), func(c Column, i int) bool {
			t := strings.ToLower(c.Type)
			isText := strings.Contains(t, "text") || strings.Contains(t, "char")
			return isText
			// TODO: should be string or number?
			// skip date column for now
			return !(strings.Contains(c.Name, "time") || strings.Contains(c.Name, "date"))
		})

		colsSQL := []string{}
		for _, col := range cols {
			for _, sf := range statsFields {
				colSQL := g.R(
					g.F("%s as {field}_%s", sf.TemplateSQL, sf.Name),
					"field", col.Name,
				)
				colsSQL = append(colsSQL, colSQL)
			}
		}

		sql := g.R(
			sqlAnalyzeColumns,
			"cols_sql", strings.Join(colsSQL, ", "),
			"schema", table.Schema,
			"table", table.Name,
			"limit", cast.ToString(sampleSize),
		)
		data, err := da.Conn.Query(sql)
		if err != nil {
			return g.Error(err, "could not get analysis sql for %s", table.FullName())
		} else if len(data.Rows) == 0 {
			return g.Error("got zero rows for analysis sql for %s", table.FullName())
		}

		// retrieve values, since in order
		row := data.Rows[0]
		i := 0
		for _, col := range cols {
			m := g.M()
			for _, sf := range statsFields {
				m[sf.Name] = row[i]
				i++
			}
			// unmarshal
			err = g.Unmarshal(g.Marshal(m), &col.Stats)
			if err != nil {
				return g.Error(err, "could not get unmarshal sql stats for %s:\n%s", table.FullName(), g.Marshal(m))
			}

			// store in master map
			da.ColumnMap[col.Key()] = col
			if col.IsUnique() {
				g.Info("    %s is unique [%d rows]", col.Key(), col.Stats.RowCount)
			}
		}
	}

	return
}

func (da *DataAnalyzer) ProcessRelations() (err error) {

	// same length text fields, uuid
	lenColMap := map[int][]Column{}
	for _, col := range da.ColumnMap {
		t := strings.ToLower(col.Type)
		isText := strings.Contains(t, "text") || strings.Contains(t, "char")
		if isText && col.Stats.LengthMinimun == col.Stats.LengthMaximum && col.Stats.LengthMinimun > 0 {
			if arr, ok := lenColMap[col.Stats.LengthMinimun]; ok {
				lenColMap[col.Stats.LengthMinimun] = append(arr, col)
			} else {
				lenColMap[col.Stats.LengthMinimun] = []Column{col}
			}
		}
	}

	// iterate over non-unique ones, and grab a value, and try to join to unique ones
	uniqueCols := []Column{}
	nonUniqueCols := []Column{}
	for lenI, cols := range lenColMap {
		g.Info("%d | %d cols", lenI, len(cols))
		if lenI != 36 {
			// only UUID for now
			continue
		}
		for _, col := range cols {
			if col.Stats.RowCount <= 1 {
				continue // skip single row tables for now
			} else if col.IsUnique() {
				uniqueCols = append(uniqueCols, col)
			} else {
				nonUniqueCols = append(nonUniqueCols, col)
			}
		}
	}

	err = da.GetOneToMany(uniqueCols, nonUniqueCols)
	if err != nil {
		return g.Error(err, "could not run GetOneToMany")
	}

	err = da.GetOneToOne(uniqueCols)
	if err != nil {
		return g.Error(err, "could not run GetOneToMany")
	}

	err = da.GetManyToMany(nonUniqueCols)
	if err != nil {
		return g.Error(err, "could not run GetOneToMany")
	}

	// save yaml
	out, err := yaml.Marshal(da.RelationMap)
	if err != nil {
		return g.Error(err, "could not marshal to yaml")
	}

	err = ioutil.WriteFile("output.yaml", out, 0755)
	if err != nil {
		return g.Error(err, "could not write to yaml")
	}

	return
}

func (da *DataAnalyzer) GetOneToMany(uniqueCols, nonUniqueCols []Column) (err error) {
	// build all_non_unique_values
	nonUniqueExpressions := lo.Map(nonUniqueCols, func(col Column, i int) string {
		template := `select * from (select '{schema}.{table}.{field}' as non_unique_column_key, "{field}"::text as val from "{schema}"."{table}" where "{field}" is not null limit 1) t`
		return g.R(
			template,
			"field", col.Name,
			"schema", col.Schema,
			"table", col.Table,
		)
	})
	nonUniqueSQL := strings.Join(nonUniqueExpressions, " union\n    ")

	// get 1-N and N-1
	matchingSQLs := lo.Map(uniqueCols, func(col Column, i int) string {
		template := `select nuv.non_unique_column_key,	'{schema}.{table}.{field}' as unique_column_key	from all_non_unique_values nuv
				inner join "{schema}"."{table}" t on t."{field}"::text = nuv.val`
		return g.R(
			template,
			"field", col.Name,
			"schema", col.Schema,
			"table", col.Table,
		)
	})
	matchingSQL := strings.Join(matchingSQLs, "    union\n    ")

	// run
	sql := g.R(`with all_non_unique_values as (
				{non_unique_sql}
			)
			, matching as (
				{matching_sql}
			)
			select unique_column_key, non_unique_column_key
			from matching
			order by unique_column_key
			`,
		"non_unique_sql", nonUniqueSQL,
		"matching_sql", matchingSQL,
	)
	data, err := da.Conn.Query(sql)
	if err != nil {
		return g.Error(err, "could not get matching columns")
	}

	for _, rec := range data.Records() {
		uniqueColumnKey := cast.ToString(rec["unique_column_key"])
		nonUniqueColumnKey := cast.ToString(rec["non_unique_column_key"])
		// one to many
		if m, ok := da.RelationMap[uniqueColumnKey]; ok {
			m[nonUniqueColumnKey] = RelationOneToMany
		} else {
			da.RelationMap[uniqueColumnKey] = map[string]Relation{nonUniqueColumnKey: RelationOneToMany}
		}

		// many to one
		if m, ok := da.RelationMap[nonUniqueColumnKey]; ok {
			m[uniqueColumnKey] = RelationManyToOne
		} else {
			da.RelationMap[nonUniqueColumnKey] = map[string]Relation{uniqueColumnKey: RelationManyToOne}
		}
	}
	return
}

func (da *DataAnalyzer) GetOneToOne(uniqueCols []Column) (err error) {
	uniqueExpressions := lo.Map(uniqueCols, func(col Column, i int) string {
		template := `select * from (select '{schema}.{table}.{field}' as unique_column_key_1, "{field}"::text as val from "{schema}"."{table}" where "{field}" is not null limit 1) t`
		return g.R(
			template,
			"field", col.Name,
			"schema", col.Schema,
			"table", col.Table,
		)
	})
	nonUniqueSQL := strings.Join(uniqueExpressions, " union\n    ")

	// get 1-1
	matchingSQLs := lo.Map(uniqueCols, func(col Column, i int) string {
		template := `select uv.unique_column_key_1,	'{schema}.{table}.{field}' as unique_column_key_2	from unique_values uv
				inner join "{schema}"."{table}" t on t."{field}"::text = uv.val`
		return g.R(
			template,
			"field", col.Name,
			"schema", col.Schema,
			"table", col.Table,
		)
	})
	matchingSQL := strings.Join(matchingSQLs, "    union\n    ")

	// run
	sql := g.R(`with unique_values as (
				{non_unique_sql}
			)
			, matching as (
				{matching_sql}
			)
			select unique_column_key_1, unique_column_key_2
			from matching
			where unique_column_key_2 != unique_column_key_1
			order by unique_column_key_1
			`,
		"non_unique_sql", nonUniqueSQL,
		"matching_sql", matchingSQL,
	)
	data, err := da.Conn.Query(sql)
	if err != nil {
		return g.Error(err, "could not get matching columns")
	}

	for _, rec := range data.Records() {
		uniqueColumnKey1 := cast.ToString(rec["unique_column_key_1"])
		uniqueColumnKey2 := cast.ToString(rec["unique_column_key_2"])
		// one to many
		if m, ok := da.RelationMap[uniqueColumnKey1]; ok {
			m[uniqueColumnKey2] = RelationOneToOne
		} else {
			da.RelationMap[uniqueColumnKey1] = map[string]Relation{uniqueColumnKey2: RelationOneToOne}
		}

		// many to one
		if m, ok := da.RelationMap[uniqueColumnKey2]; ok {
			m[uniqueColumnKey1] = RelationOneToOne
		} else {
			da.RelationMap[uniqueColumnKey2] = map[string]Relation{uniqueColumnKey1: RelationOneToOne}
		}
	}
	return
}

func (da *DataAnalyzer) GetManyToMany(nonUniqueCols []Column) (err error) {
	return nil
}
