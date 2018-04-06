package cmd

import (
	"fmt"
	"strings"
)

type AthenaQueryBuilder struct {}

func NewAthenaQueryBuilder() *AthenaQueryBuilder {
	a := new(AthenaQueryBuilder)
	return a
}

func (a AthenaQueryBuilder) ls(database string) string {
	var query string
	if database == "" {
		query = "SHOW DATABASES"
	} else {
		query = "SHOW TABLES IN " + database
	}
	return query
}

func (a AthenaQueryBuilder) head(table string, maxRows int) string {
	query := "SELECT * FROM " + table + " LIMIT " + fmt.Sprint(maxRows)
	return query
}

func (a AthenaQueryBuilder) mk(database string) string {
	query := "CREATE DATABASE IF NOT EXISTS " + database
	return query
}

func (a AthenaQueryBuilder) rm(database string, table string) string {
	var query string
	if table == "" {
		query = "DROP DATABASE IF EXISTS " + database
	} else {
		query = "DROP TABLE IF EXISTS " + database + "." + table
	}
	return query
}

func (a AthenaQueryBuilder) load(table string, source string, schema *Schema, patition string) string {
	serde := "org.apache.hive.hcatalog.data.JsonSerDe"

	var patitionState string
	if patition == "" {
		patitionState = ""
	} else {
		patitionState = "PARTITIONED BY (" + strings.Replace(patition,":", " ", -1) + ")"
	}

	query := "CREATE EXTERNAL TABLE IF NOT EXISTS "  + table +
		" (" + schema.toString() + ") " +
		patitionState +
		" ROW FORMAT SERDE '" + serde + "'" +
		" LOCATION '" + source + "'"

	return query
}
