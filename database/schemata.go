package database

import (
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// Column represents a schemata column
type Column struct {
	Position int         `json:"-"`
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	GenType  string      `json:"gen_type"`
	Table    string      `json:"table"`
	Schema   string      `json:"schema"`
	Database string      `json:"database"`
	Stats    ColumnStats `json:"stats"`
}

func (c Column) Key() string {
	return c.Schema + "." + c.Table + "." + c.Name
}

func (c Column) IsUnique() bool {
	if c.Stats.RowCount == 0 {
		return false
	}
	return c.Stats.RowCount == c.Stats.DistinctCount
}

type ColumnStats struct {
	RowCount      int64 `json:"row_count"`
	ValueCount    int64 `json:"value_count"`
	DistinctCount int64 `json:"distinct_count"`
	LengthMinimun int   `json:"length_minimun"`
	LengthMaximum int   `json:"length_maximum"`
	// ValueMinimun  *string `json:"value_minimun"`
	// ValueMaximum  *string `json:"value_maximum"`
}

func (cs *ColumnStats) NullCount() int64 {
	return cs.RowCount - cs.ValueCount
}

func (cs *ColumnStats) NullPercent() float64 {
	val := (cs.RowCount - cs.ValueCount) * 100 / cs.RowCount
	return cast.ToFloat64(val) / 100
}

func (cs *ColumnStats) DistinctPercent() float64 {
	val := (cs.DistinctCount) * 100 / cs.RowCount
	return cast.ToFloat64(val) / 100
}

func (cs *ColumnStats) DuplicateCount() int64 {
	return cs.RowCount - cs.DistinctCount
}

func (cs *ColumnStats) DuplicatePercent() float64 {
	val := (cs.RowCount - cs.DistinctCount) * 100 / cs.RowCount
	return cast.ToFloat64(val) / 100
}

// Table represents a schemata table
type Table struct {
	Name     string `json:"name"`
	Schema   string `json:"schema"`
	Database string `json:"database"`
	IsView   bool   `json:"is_view"` // whether is a view
	Columns  []Column
}

func (t *Table) FullName() string {
	return t.Schema + "." + t.Name
}

func (t *Table) ColumnsMap() map[string]Column {
	columns := map[string]Column{}
	for _, column := range t.Columns {
		key := strings.ToLower(column.Name)
		columns[key] = column
	}
	return columns
}

// Database represents a schemata database
type Database struct {
	Name    string `json:"name"`
	Schemas map[string]Schema
}

func (db *Database) Tables() map[string]Table {
	tables := map[string]Table{}
	for _, schema := range db.Schemas {
		for _, table := range schema.Tables {
			key := strings.ToLower(g.F("%s.%s", schema.Name, table.Name))
			tables[key] = table
		}
	}
	return tables
}

func (db *Database) Columns() map[string]Column {
	columns := map[string]Column{}
	for _, schema := range db.Schemas {
		for _, table := range schema.Tables {
			for _, column := range table.Columns {
				key := strings.ToLower(g.F("%s.%s.%s", schema.Name, table.Name, column.Name))
				columns[key] = column
			}
		}
	}
	return columns
}

// Schema represents a schemata schema
type Schema struct {
	Name   string `json:"name"`
	Tables map[string]Table
}

func (schema *Schema) Columns() map[string]Column {
	columns := map[string]Column{}
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			key := strings.ToLower(g.F("%s.%s", table.Name, column.Name))
			columns[key] = column
		}
	}
	return columns
}

// ToData converts schema objects to tabular format
func (schema *Schema) ToData() (data iop.Dataset) {
	columns := []string{"schema_name", "table_name", "is_view", "column_id", "column_name", "column_type"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns...))

	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			row := []interface{}{schema.Name, table.Name, table.IsView, col.Position, col.Name, col.Type}
			data.Rows = append(data.Rows, row)
		}
	}
	return
}

// Schemata contains the full schema for a connection
type Schemata struct {
	Databases map[string]Database
	conn      Connection
}

// LoadTablesJSON loads from a json string
func (s *Schemata) LoadTablesJSON(payload string) error {
	tables := map[string]Table{}
	err := g.Unmarshal(payload, &tables)
	if err != nil {
		return g.Error(err, "could not unmarshal TablesJSON")
	}

	// reconstruct data
	databases := map[string]Database{}
	for key, table := range tables {
		keyArr := strings.Split(key, ".")
		if len(keyArr) != 3 {
			return g.Error("table key must be formatted as `database.schema.table`, got `%s`", key)
		}

		databaseName := strings.ToLower(keyArr[0])
		schemaName := strings.ToLower(keyArr[1])
		tableName := strings.ToLower(keyArr[2])

		if _, ok := databases[databaseName]; !ok {
			databases[databaseName] = Database{
				Name:    table.Database,
				Schemas: map[string]Schema{},
			}
		}
		database := databases[databaseName]

		if _, ok := database.Schemas[schemaName]; !ok {
			database.Schemas[schemaName] = Schema{
				Name:   table.Schema,
				Tables: map[string]Table{},
			}
		}
		schema := database.Schemas[schemaName]

		// fill in positions
		for i := range table.Columns {
			table.Columns[i].Position = i + 1
		}

		// store data
		schema.Tables[tableName] = table
		database.Schemas[schemaName] = schema
		databases[databaseName] = database
	}

	s.Databases = databases

	return nil
}

// Database returns the first encountered database
func (s *Schemata) Database() Database {
	for _, db := range s.Databases {
		return db
	}
	return Database{}
}

func (s *Schemata) Tables() map[string]Table {
	tables := map[string]Table{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				key := strings.ToLower(g.F("%s.%s.%s", db.Name, schema.Name, table.Name))
				tables[key] = table
			}
		}
	}
	return tables
}

func (s *Schemata) Columns() map[string]Column {
	columns := map[string]Column{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				for _, column := range table.Columns {
					// get general type
					dType := strings.Split(strings.ToLower(column.Type), "(")[0]
					generalType, ok := s.conn.Template().NativeTypeMap[dType]
					if !ok {
						g.Warn(
							"No general type mapping defined for col '%s', with type '%s' for '%s'",
							column.Name,
							dType,
							s.conn.GetType(),
						)
						column.GenType = "string"
					}

					column.GenType = generalType
					key := strings.ToLower(g.F("%s.%s.%s.%s", db.Name, schema.Name, table.Name, column.Name))
					columns[key] = column
				}
			}
		}
	}
	return columns
}
