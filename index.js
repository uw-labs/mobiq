const readline = require('readline');
const package = require('./package.json')
const program = require('commander')
const mongo = require('./mongo')
const process = require('process')
const chalk = require('chalk')
const icons = require('log-symbols')
const google = require('./google')
const path = require('path')
const ora = require('ora')
const zlib = require('zlib')

function collect(inVal, memo) {
    const [key, val] = inVal.split('=')
    memo[key]=val
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
        process.stdout.clearLine();
        process.stdout.cursorTo(0);
        logger(icons.success, newLabel || label)
    }
}

program
    .name(package.name)
    .version(package.version)
    .option('-d, --db <dsn>', 'mongo DB DSN')
    .option('-c, --db-collection <collection>', 'collection to dump')
    .option('--db-query [query]', 'query for .find()')
    .option('--db-skip [skip]', 'number of records to skip', parseInt, 0)
    .option('--db-batch [batch]', 'cursor batch size', parseInt, 1000)
    .option('-p, --bq-project <project>', 'big query project name')
    .option('-s, --bq-dataset <dataset>', 'big query data set')
    .option('-t, --bq-table [table]', 'big query table, defaults to --db-collection', '')
    .option('-b, --bq-bucket <bucket>', 'gc bucket to use for data transfer')
    .option('-k, --bq-credentials <file>', 'gc credentials file', path.resolve, '')
    .option('--transform-flatten-objects', 'enables flattening of nested hashes into single dimension ones')
    .option('--no-transform-remove-nulls', 'disables removal of null value keys')
    .option('--transform [file]', 'add transormation stream from file', collect, [])
    .option('--bq-option [option]', 'add big query import option key=value', collect, {})
    .option('--dump-schema', 'do not import, just dump schema')

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

(async () => {
    try {

        logger()
        logger(`${chalk.blue(package.name)} - ${chalk.green(package.version)}`)
        logger()

        const {dbQuery, dbSkip, dbBatch, dbCollection} = program

        check(`checking connection to ${chalk.blue(program.db)}`)
        const db = await mongo(program.db, dbCollection)
        done(`connected to ${chalk.blue(program.db)}.${chalk.blue(dbCollection)}`)

        label('query:', dbQuery || '{}')
        label('skip:', dbSkip)
        label('batch:', dbBatch)

        const rowCount = await db.rowCount(dbQuery, dbSkip)
        label('rows:', rowCount)

        logger()

        const {bqBucket, bqCredentials, bqDataset, bqProject} = program
        let bqTable = program.bqTable || dbCollection
        bqTable = bqTable.replace(/[^a-z0-9]/, '_')


        const gl = google(bqProject, bqCredentials)

        label('project:', bqProject)
        label('dataset:', bqDataset)
        label('table:', bqTable)
        label('user:', gl.user)

        check(`checking bucket ${chalk.blue(bqBucket)}`)
        const bucket = await gl.bucket(bqBucket)
        done()

        check(`creating import file`)
        const file = await gl.file()
        done(`created file ${chalk.blue(file.name)}`)

        logger()

        stream = db.stream(dbQuery, dbSkip, dbBatch)

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

            destination.pipe(
                gl.schema().fromStream()
            )

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
            console.log(require('util').inspect(gl.schema().get(), false, null))
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
                [job] = await gl.load(bqDataset, bqTable, file, gl.schema().get(), bqOption)
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

