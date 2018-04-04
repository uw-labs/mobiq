# mobiq

A Mongo -> BigQuery importer with nested document and dynamic schema support.

```
  Usage: mobiq [options]

  Options:

    -V, --version                     output the version number
    -d, --db <dsn>                    mongo DB DSN
    -c, --db-collection <collection>  collection to dump
    --db-query [query]                query for .find()
    --db-limit [limit]                record limit
    --db-skip [skip]                  number of records to skip (default: 0)
    --db-batch [batch]                cursor batch size (default: 1000)
    -p, --bq-project <project>        big query project name
    -s, --bq-dataset <dataset>        big query data set
    -t, --bq-table [table]            big query table, defaults to --db-collection (default: )
    -b, --bq-bucket <bucket>          gc bucket to use for data transfer
    -k, --bq-credentials <file>       gc credentials file (default: )
    --transform-flatten-objects       enables flattening of nested hashes into single dimension ones
    --no-transform-remove-nulls       disables removal of null value keys
    --transform [file]                add transformation stream from file (default: )
    --bq-option [option]              add big query import option key=value (default: [object Object])
    --dump-schema [file]              do not import, just dump schema
    --schema [file]                   use schema from file rather than guessing from files
    --ls-transform                    list available transformations
    -h, --help                        output usage information

```
