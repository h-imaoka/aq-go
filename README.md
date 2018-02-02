# aq (Athena Query CLI)

Command Line Tool for AWS Athena (bq command like)

## Installation

```bash
$ go get github.com/mia-0032/aq
```

## Usage

All commands need `--bucket` option because Athena stores query result into S3.
You can specify it by `AQ_DEFAULT_BUCKET` environment variable.

### help

Display command help

```bash
$ aq help
$ aq help [COMMAND]
```

### ls

Show databases or tables in specified database

```bash
$ aq ls
$ aq ls my_database_name
```

### head

Show records in specified table

```bash
$ aq head my_db.my_table
```

### mk

Create database

```bash
$ aq mk my_database_name
```

### load

Create table and load data

```bash
$ aq load my_db.my_table s3://my_bucket/my_object_key/ test/resource/schema.json --partitioning dt:string
```

### rm

Drop database or table

```bash
$ aq rm my_db
$ aq rm my_db.my_table
```

### query

Run query

```bash
$ aq query 'SELECT * FROM "test"."test_logs" limit 10;'
```

## Development

todo: write

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mia-0032/aq
