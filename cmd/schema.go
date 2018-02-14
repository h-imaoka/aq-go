package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"strconv"
	"io/ioutil"
	"encoding/json"
)

type Field struct {
	name string
	athenaType string
	nullable bool
}

func (f Field) toMap () map[string]string {
	r := map[string]string{
		"name": f.name, "athenaType": f.athenaType, "nullable": strconv.FormatBool(f.nullable),
	}
	return r
}

type Schema struct {
	Fields []Field
}

func newSchema() *Schema {
	s := new(Schema)
	return s
}

func (s *Schema) convertTypeFromBqToAthena(bqType string) (string, error) {
	/**
	Schema correspondence table
	----------+----------
	BQ        | Athena
	----------+----------
	STRING    | STRING
	BYTES     | x
	INTEGER   | INT
	FLOAT     | DOUBLE
	BOOLEAN   | BOOLEAN
	RECORD    | ARRAY or MAP
	TIMESTAMP | TIMESTAMP
	DATE      | DATE
	TIME      | TIMESTAMP
	DATETIME  | TIMESTAMP
	*/

	bqType = strings.ToUpper(bqType)

	var athenaType string

	switch bqType {
	case "INTEGER":
		athenaType = "BIGINT"
	case "FLOAT":
		athenaType = "DOUBLE"
	case "RECORD":
		return "", errors.New("sorry, `RECORD` is not supported yet in aq")
	case "BYTES":
		return "", errors.New("`BYTES` is not supported in Athena")
	case "TIME", "DATETIME":
		athenaType = "TIMESTAMP"
	default:
		athenaType = bqType
	}

	return athenaType, nil
}

func (s *Schema) appendColumn(name string, bqType string, mode string) error {
	athenaType, err := s.convertTypeFromBqToAthena(bqType)
	if err != nil {
		return err
	}

	f := Field{name, athenaType, strings.ToUpper(mode) != "REQUIRED"}
	s.Fields = append(s.Fields, f)

	return nil
}

func (s *Schema) toString() string {
	var fList []string
	for _, f := range s.Fields {
		fList = append(fList, "`" + f.name + "` " + f.athenaType)
	}
	return strings.Join(fList, ",")
}


type SchemaFileColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}

type SchemaFile []SchemaFileColumn

type SchemaLoader struct {}

func NewSchemaLoader() *SchemaLoader {
	sl := new(SchemaLoader)
	return sl
}

func (sl *SchemaLoader) load(schema string) (*Schema, error) {
	_, err := os.Stat(schema)

	var s *Schema

	if err == nil {
		s, err = sl.loadFromFile(schema)
	} else {
		s, err = sl.loadFromString(schema)
	}

	if err != nil {
		return nil, err
	}

	return s, nil
}

func (sl *SchemaLoader) loadFromFile(schema string) (*Schema, error) {
	filePath, err := filepath.Abs(schema)
	if err != nil {
		return nil, err
	}

	raw, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var sf SchemaFile

	json.Unmarshal(raw, &sf)

	s := newSchema()

	for _, column := range sf {
		s.appendColumn(column.Name, column.Type, column.Mode)
	}

	return s, nil
}

func (sl *SchemaLoader) loadFromString(schema string) (*Schema, error) {
	s := newSchema()

	for _, column := range strings.Split(schema, ",") {
		c := strings.Split(column, ":")
		s.appendColumn(c[0], c[1], "")
	}

	return s, nil
}
