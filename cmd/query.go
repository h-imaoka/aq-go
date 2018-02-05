package cmd

import (
	"encoding/csv"
	"errors"
	"fmt"
	"time"
	"strings"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws"
)

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


type AthenaQueryRunner struct {
	logger *logrus.Logger
	athenaClient *athena.Athena
	s3Downloader *s3manager.Downloader
}

func NewAthenaQueryRunner() *AthenaQueryRunner {
	a := new(AthenaQueryRunner)
	a.logger = logrus.New()

	sess := session.Must(session.NewSession())

	a.athenaClient = athena.New(sess)
	a.s3Downloader = s3manager.NewDownloader(sess)

	return a
}

func (a AthenaQueryRunner) startQuery(query string, bucket string, objectPrefix string) (string, error) {
	a.logger.Info("Run Query: " + query)

	resultConf := &athena.ResultConfiguration{}
	resultConf.SetOutputLocation("s3://" + bucket + "/" + objectPrefix)

	input := &athena.StartQueryExecutionInput{
		QueryString:         &query,
		ResultConfiguration: resultConf,
	}

	output, err := a.athenaClient.StartQueryExecution(input)

	if err != nil {
		return "", err
	}

	execId := *output.QueryExecutionId

	return execId, nil
}

func (a AthenaQueryRunner) waitQuery(execId string, timeout int) error {
	var timeoutTime time.Time
	if timeout == 0 {
		timeoutTime = time.Now().AddDate(100, 0, 0)
	} else {
		timeoutTime = time.Now().Add(time.Second * time.Duration(timeout))
	}

	executionInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: &execId,
	}

	for time.Now().Before(timeoutTime) {
		a.logger.Debug( "Waiting query finished...")
		executionOutput, err := a.athenaClient.GetQueryExecution(executionInput)
		if err != nil {
			return err
		}
		switch *executionOutput.QueryExecution.Status.State {
			case athena.QueryExecutionStateQueued, athena.QueryExecutionStateRunning:
				time.Sleep(5 * time.Second)
			case athena.QueryExecutionStateSucceeded:
				a.logger.Info("Query succeeded. Result: " + *executionOutput.QueryExecution.ResultConfiguration.OutputLocation)
				return nil
			default:
				a.logger.Error("Query failed. Reason: " + *executionOutput.QueryExecution.Status.StateChangeReason)
				return errors.New(executionOutput.String())
		}
	}

	a.stopQuery(execId)
	return errors.New("Query:" + execId + " is timeout. Stopped query execution.")
}

func (a AthenaQueryRunner) stopQuery(execId string) error {
	executionInput := &athena.StopQueryExecutionInput{
		QueryExecutionId: &execId,
	}
	_, err := a.athenaClient.StopQueryExecution(executionInput)
	if err != nil {
		return err
	}
	return nil
}

func (a AthenaQueryRunner) fetchAllQueryResult(execId string) (string, string, error) {
	executionInput := &athena.GetQueryExecutionInput{
		QueryExecutionId: &execId,
	}
	executionOutput, err := a.athenaClient.GetQueryExecution(executionInput)
	if err != nil {
		return "", "", err
	}

	location := *executionOutput.QueryExecution.ResultConfiguration.OutputLocation
	tmp := strings.SplitN(location, "/", 4)  // s3://hoge/hoge
	bucket := tmp[2]
	key := tmp[3]

	var format string
	if strings.HasSuffix(key, ".csv") {
		format = "csv"
	} else {
		format = "txt"
	}

	f := &aws.WriteAtBuffer{}
	_, err = a.s3Downloader.Download(f, &s3.GetObjectInput{Bucket: &bucket, Key: &key,})
	if err != nil {
		return "", "", err
	}

	result := string(f.Bytes())
	return result, format, nil
}

func (a AthenaQueryRunner) printResult(result string, format string) {
	if format == "csv" {
		reader := csv.NewReader(strings.NewReader(result))
		table, _ := tablewriter.NewCSVReader(os.Stdout, reader, true)
		table.SetAlignment(tablewriter.ALIGN_LEFT)   // Set Alignment
		table.Render()
	} else {
		fmt.Println(result)
	}
}

func (a AthenaQueryRunner) run(query string, bucket string, objectPrefix string, timeout int) error {
	execId, err := a.startQuery(query, bucket, objectPrefix)
	if err != nil {
		return err
	}
	a.logger.Info("QueryExecutionID: " + execId)

	err = a.waitQuery(execId, timeout)
	if err != nil {
		return err
	}

	result, format, err := a.fetchAllQueryResult(execId)
	if err != nil {
		return err
	}
	a.printResult(result, format)

	return nil
}

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
