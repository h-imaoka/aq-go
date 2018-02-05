package cmd

import (
	"strings"

	"github.com/urfave/cli"
	"github.com/sirupsen/logrus"
)

func Ls(c *cli.Context) error {
	var database string
	if c.NArg() > 0 {
		database = c.Args().First()
	} else {
		database = ""
	}

	query := NewAthenaQueryBuilder().ls(database)

	aqr := NewAthenaQueryRunner()
	err := aqr.run(query, c.String("bucket"), c.String("object_prefix"), 0)
	if err != nil {
		logrus.New().Error(err)
		return cli.NewExitError("Query execution is failed.", 1)
	}

	return nil
}

func Head(c *cli.Context) error {
	table := c.Args().First()
	query := NewAthenaQueryBuilder().head(table, c.Int("max_rows"))

	aqr := NewAthenaQueryRunner()
	err := aqr.run(query, c.String("bucket"), c.String("object_prefix"), 0)
	if err != nil {
		logrus.New().Error(err)
		return cli.NewExitError("Query execution is failed.", 1)
	}

	return nil
}

func Mk(c *cli.Context) error {
	database := c.Args().First()
	query := NewAthenaQueryBuilder().mk(database)

	aqr := NewAthenaQueryRunner()
	err := aqr.run(query, c.String("bucket"), c.String("object_prefix"), 0)
	if err != nil {
		logrus.New().Error(err)
		return cli.NewExitError("Query execution is failed.", 1)
	}

	return nil
}

func Rm(c *cli.Context) error {
	name := c.Args().First()

	var database string
	var table string

	if strings.Contains(name, ".") {
		tmp := strings.Split(name, ".")
		database = tmp[0]
		table = tmp[1]
	} else {
		database = name
		table = ""
	}

	query := NewAthenaQueryBuilder().rm(database, table)

	aqr := NewAthenaQueryRunner()
	err := aqr.run(query, c.String("bucket"), c.String("object_prefix"), 0)
	if err != nil {
		logrus.New().Error(err)
		return cli.NewExitError("Query execution is failed.", 1)
	}

	return nil
}

func Query(c *cli.Context) error {
	query := c.Args().First()

	aqr := NewAthenaQueryRunner()
	err := aqr.run(query, c.String("bucket"), c.String("object_prefix"), c.Int("timeout"))
	if err != nil {
		logrus.New().Error(err)
		return cli.NewExitError("Query execution is failed.", 1)
	}

	return nil
}
