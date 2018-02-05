package cmd

import (
	"fmt"
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
