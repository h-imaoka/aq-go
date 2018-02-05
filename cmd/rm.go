package cmd

import (
	"github.com/urfave/cli"
	"github.com/sirupsen/logrus"
	"strings"
)

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
