# mobiq

Mobiq is a tool for importing mongo collections into google's big query, it supports automatic schema recognition and allows for customer transformations to be plugged in at run time.

It dumps data from mongo using cursor stream api, uploads it to gc storage bucket and runs a big query import from there.

## installation

```
npm i -g --save mobiq
```

## usage

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
    --dump-schema                     do not import, just dump schema
    --ls-transform                    list available transformations
    -h, --help                        output usage information


```

## using transformations

you can list available transformations with `mobiq --ls-transform`, you can also provide your own and point at the relevant file via `--transform` switch.

A transformation is a node transform stream running in object mode which gets each record in a mongo connection.

Example:

```node.js
const { Transform } = require('stream');

module.exports = class TimestampFromMongoId extends Transform {
    constructor() {
        super({objectMode: true})
    }

    _transform(chunk, encoding, callback) {
        chunk.createdOn = new Date(parseInt(chunk.id.substring(0, 8), 16) * 1000);
        this.push(chunk);
        callback()
    }
}
```
