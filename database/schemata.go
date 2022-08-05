package database

import (
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
)

// Table represents a schemata table
type Table struct {
	Name     string `json:"name"`
	Schema   string `json:"schema"`
	Database string `json:"database"`
	IsView   bool   `json:"is_view"` // whether is a view
	Columns  iop.Columns
}

func (t *Table) FullName() string {
	return t.Schema + "." + t.Name
}

func (t *Table) ColumnsMap() map[string]iop.Column {
	columns := map[string]iop.Column{}
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

func (db *Database) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
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

func (schema *Schema) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
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
			row := []interface{}{schema.Name, table.Name, table.IsView, col.Position, col.Name, col.DbType}
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

func (s *Schemata) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				for _, column := range table.Columns {
					// get general type
					dType := strings.Split(strings.ToLower(column.DbType), "(")[0]
					generalType, ok := s.conn.Template().NativeTypeMap[dType]
					if !ok {
						g.Warn(
							"No general type mapping defined for col '%s', with type '%s' for '%s'",
							column.Name,
							dType,
							s.conn.GetType(),
						)
						column.Type = "string"
					}

					column.Type = iop.ColumnType(generalType)
					key := strings.ToLower(g.F("%s.%s.%s.%s", db.Name, schema.Name, table.Name, column.Name))
					columns[key] = column
				}
			}
		}
	}
	return columns
}