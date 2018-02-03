package cmd

import (
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
