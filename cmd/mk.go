package cmd

import (
	"github.com/urfave/cli"
	"github.com/sirupsen/logrus"
)

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
