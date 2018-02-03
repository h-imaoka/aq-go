package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/mia-0032/aq/cmd"
	"github.com/urfave/cli"
)

var BucketFlag = cli.StringFlag{
	Name: "bucket, b",
	Usage: "S3 bucket where the query result is stored.",
	EnvVar: "AQ_DEFAULT_BUCKET",
}

var ObjectPrefixFlag = cli.StringFlag{
	Name: "object_prefix, o",
	Value: "Unsaved/" + time.Now().Format("2006/01/02"),
	Usage: "S3 object prefix where the query result is stored.",
}

var Commands = []cli.Command{
	{
		Name:   "query",
		Usage:  "Run query",
		Action: cmd.Query,
		ArgsUsage:   "QUERY",
		Flags: []cli.Flag{
			BucketFlag,
			ObjectPrefixFlag,
			cli.IntFlag{
				Name: "timeout, t",
				Value: 0,
				Usage: "Wait for execution of the query for this number of seconds. If this is set to 0, timeout is disabled.",
			},
		},
		Before: func(c *cli.Context) error {
			if c.NArg() == 0 {
				return cli.NewExitError("QUERY must be specified.", 1)
			}
			if c.String("bucket") == "" {
				return cli.NewExitError("bucket must be specified.", 1)
			}
			return nil
		},
	},
	{
		Name:   "ls",
		Usage:  "Show databases or tables in specified database",
		Action: cmd.Ls,
		ArgsUsage:   "[DATABASE]",
		Flags: []cli.Flag{
			BucketFlag,
			ObjectPrefixFlag,
		},
		Before: func(c *cli.Context) error {
			if c.String("bucket") == "" {
				return cli.NewExitError("bucket must be specified.", 1)
			}
			return nil
		},
	},
}

func Run() int {
	app := cli.NewApp()
	app.Name = "aq"
	app.Usage = "Command Line Tool for AWS Athena (bq command like)"
	app.Version = "0.2.0"
	app.EnableBashCompletion = true
	app.Commands = Commands
	app.Action = func (_ *cli.Context) {
		var subcmds []string
		for _, subcmd := range Commands {
			subcmds = append(subcmds, subcmd.Name)
		}
		fmt.Fprintf(os.Stderr, "%s: %s\n", "Subcommands", strings.Join(subcmds, ", "))
	}

	return msg(app.Run(os.Args))
}

func msg(err error) int {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		return 1
	}
	return 0
}
