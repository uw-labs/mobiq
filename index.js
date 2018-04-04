const readline = require('readline');
const package = require('./package.json')
const program = require('commander')
const mongo = require('./mongo')
const process = require('process')
const chalk = require('chalk')
const icons = require('log-symbols')
const google = require('./google')
const path = require('path')
let ora = require('ora')
const zlib = require('zlib')
const glob = require('glob')
const fs = require('fs')

if (!process.stdout.isTTY) {
    ora = function(str) {

        const instance = {}
        instance.text = str

        instance.fail = () => {
            logger(icons.error, instance.text)
        }

        instance.succeed = () => {
            logger(icons.success, instance.text)
        }

        return {
            start: () => instance
        }
    }
}

function collect(inVal, memo) {
    const [key, val] = inVal.split('=')
    memo[key]=val
    return memo;
}

function arr(inVal, memo) {
    memo.push(inVal)
    return memo;
}

const logger = (...args) => {
    console.log(...args)
}

const info = (...args) => {
    logger(icons.info, ...args)
}

const label = (label, ...args) => {
    info(chalk.blue(label.padEnd(8, ' ')), ...args)
}

let done
let job

const check = (label) => {
    process.stdout.write(icons.info + ' ' + label)
    done = (newLabel) => {
        if (process.stdout.clearLine) {
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
        } else {
            process.stdout.write('\n')
        }
        logger(icons.success, newLabel || label)
    }
}

function getInt(val) {
    return parseInt(val)
}

program
    .name(package.name)
    .version(package.version)
    .option('-d, --db <dsn>', 'mongo DB DSN')
    .option('-c, --db-collection <collection>', 'collection to dump')
    .option('--db-query [query]', 'query for .find()')
    .option('--db-limit [limit]', 'record limit', parseInt)
    .option('--db-skip [skip]', 'number of records to skip', parseInt, 0)
    .option('--db-batch [batch]', 'cursor batch size', getInt, 1000)
    .option('-p, --bq-project <project>', 'big query project name')
    .option('-s, --bq-dataset <dataset>', 'big query data set')
    .option('-t, --bq-table [table]', 'big query table, defaults to --db-collection', '')
    .option('-b, --bq-bucket <bucket>', 'gc bucket to use for data transfer')
    .option('-k, --bq-credentials <file>', 'gc credentials file', path.resolve, '')
    .option('--transform-flatten-objects', 'enables flattening of nested hashes into single dimension ones')
    .option('--no-transform-remove-nulls', 'disables removal of null value keys')
    .option('--transform [file]', 'add transformation stream from file', arr, [])
    .option('--bq-option [option]', 'add big query import option key=value', collect, {})
    .option('--dump-schema [file]', 'do not import, just dump schema')
    .option('--schema [file]', 'use schema from file rather than guessing from files')
    .option('--ls-transform', 'list available transformations')
    .parse(process.argv);

process.on('uncaughtException', exit);

let stream

function exit(e) {
    if (stream) {
        stream.close()
    }
    if (job && job.cancel) {
        job.cancel()
    }

    logger()
    logger(icons.warning, 'there might be a file left in your bucket that did not clear up')
    logger()
    logger(icons.error, e)
    process.exit(1)
}

function listTransform() {

    info("Available transformations:")

    glob.sync('**/*.js', {cwd: './transform/streams'}).forEach(file => {
        const f = path.parse(file)
        logger(' - '+f.dir + '/' + f.name)
    })

    process.exit(0)
}

function loadTransform(transformations) {
    const streams = []
    transformations.forEach(f => {
        if (!path.parse(f).ext) {
            f = './transform/streams/' + f + '.js'
        }
        const S = require(f)
        streams.push(new S())
    })
    return streams
}

(async () => {
    try {

        logger()
        logger(`${chalk.blue(package.name)} - ${chalk.green(package.version)}`)
        logger()

        const {lsTransform, transform} = program

        if (lsTransform) {
            listTransform()
        }

        const transformStreams = loadTransform(transform)

        const {dbQuery, dbSkip, dbBatch, dbCollection, dbLimit} = program

        check(`checking connection to ${chalk.blue(program.db)}`)
        const db = await mongo(program.db, dbCollection)
        done(`connected to ${chalk.blue(program.db)}.${chalk.blue(dbCollection)}`)

        label('query:', dbQuery || '{}')
        label('skip:', dbSkip)
        label('limit:', dbLimit)
        label('batch:', dbBatch)

        const rowCount = await db.rowCount(dbQuery, dbSkip)
        label('rows:', rowCount)

        logger()

        const {bqBucket, bqCredentials, bqDataset, bqProject} = program
        let bqTable = program.bqTable || dbCollection
        let suppliedSchema
        bqTable = bqTable.replace(/[^a-z0-9]/, '_')


        const gl = google(bqProject, bqCredentials)

        label('project:', bqProject)
        label('dataset:', bqDataset)
        label('table:', bqTable)
        label('user:', gl.user)

        if (program.schema) {
            label('schema:', program.schema)
            suppliedSchema = JSON.parse(fs.readFileSync(program.schema))
        }

        check(`checking bucket ${chalk.blue(bqBucket)}`)
        const bucket = await gl.bucket(bqBucket)
        done()

        check(`creating import file`)
        const file = await gl.file()
        done(`created file ${chalk.blue(file.name)}`)

        logger()

        stream = db.stream(dbQuery, dbSkip, dbBatch, dbLimit)

        await new Promise(r => {

            const {transformFlattenObjects, transformSkipArrays, transformRemoveNulls} = program

            const loader = ora(`loading data into ${chalk.blue(`gs://${bqBucket}/${file.name}`)}`).start();

            let counter = 0;

            stream.on('data', () => {
                counter += 1
                loader.text = `loading data into ${chalk.blue(`gs://${bqBucket}/${file.name}`)} - ${counter}/${rowCount}`
            })

            const writeStream = file.createWriteStream()

            let destination = stream.pipe(
                new (require('./transform/id'))
            ).pipe(
                new (require('./transform/sanitiseNames'))
            )

            if (transformRemoveNulls) {
                destination = destination.pipe(
                    new (require('./transform/denullify'))
                )
            }

            if (transformFlattenObjects) {
                destination = destination.pipe(
                    new (require('./transform/flatten'))(transformSkipArrays)
                )
            }

            transformStreams.forEach(t => {
                destination = destination.pipe(t)
            })

            if (!suppliedSchema) {
                let schemaStream = destination.pipe(
                    gl.schema().fromStream()
                )

                if (program.dumpSchema) {

                    schemaStream.on('finish', () => {
                        fs.writeFileSync(program.dumpSchema, JSON.stringify(gl.schema().get()))
                        logger()
                        info(`schema written to ${chalk.blue(program.dumpSchema)}`)
                        process.exit(0)
                    })

                    schemaStream.on('error', (e) => {
                        loader.fail()
                        throw e
                    })

                }
            }

            destination.pipe(
                new (require('./transform/json'))
            ).pipe(
                zlib.createGzip()
            ).pipe(
                writeStream
            )

            stream.on('error', (e) => {
                loader.fail()
                throw e
            })

            writeStream.on('error', (e) => {
                loader.fail()
                throw e
            })

            writeStream.on('finish', () => {
                loader.succeed()
                r()
            })
        })

        if (program.dumpSchema) {
            fs.writeFileSync(program.dumpSchema, JSON.stringify(gl.schema().get()))
            info(`schema written to ${chalk.blue(program.dumpSchema)}`)
            process.exit(0)
        }

        await new Promise(async (r, x) => {

            const {bqOption} = program

            const ent = Object.entries(bqOption)

            if (ent.length > 0) {
                logger(icons.info, "bq load options:")
                Object.entries(bqOption).forEach(([key, val]) => {
                    logger('   ', `${key}=${val}`)
                })
            }


            const loader = ora(`importing data into ${chalk.blue(`${bqProject}://${bqDataset}/${bqTable}`)}`).start();
            try {
                [job] = await gl.load(bqDataset, bqTable, file, suppliedSchema || gl.schema().get(), bqOption)
            } catch(e) {
                return x(e)
            }

            job.on('error', e => {
                loader.fail()
                return x(e)
            })

            job.on('complete', (metadata) => {

                if(metadata.status.errorResult) {
                    loader.fail()

                    metadata.status.errors.slice(1).forEach(e => {
                        logger(icons.error, e.message)
                    })

                    return x(new Error(metadata.status.errorResult.message))
                }

                loader.succeed()
                r()
            })

        })

        check('deleting import file')
        await file.delete()
        done()

        logger()
        logger(icons.success, 'goodbye!')
        logger()
        process.exit(0)

    } catch (e) {
        exit(e)
    }
})();

