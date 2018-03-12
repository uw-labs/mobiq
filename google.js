const gl = require('google-cloud')
const fs = require('fs');
const { Writable } = require('stream')
const schemaGenerator = require('generate-schema').bigquery
const merge = require('deepmerge')
const uniq = require('lodash.uniqwith')
const equal = require('lodash.isequal')
const plain = require('lodash.isplainobject')

class Google {
    constructor(projectId, keyFilename, user) {

        this.projectSettings = {
            projectId,
            keyFilename,
        }

        this.user = user
        this.storage = gl.storage(this.projectSettings)
        this.bq = gl.bigquery(this.projectSettings)
        this._schema = new Schema()
    }

    async bucket(bucketName) {
        const bucket = this.storage.bucket(bucketName)

        const [exists] = await bucket.exists()

        if (!exists) {
            throw new Error(`bucket ${bucketName} does not exist`)
        }

        return this.bucket = bucket
    }

    async file() {

        const time = process.hrtime()
        const suffix = time[0] * 1000000 + time[1]
        //const suffix = 'test'
        const name = `mobiq-import-${suffix}.json.gz`

        return await this.bucket.file(name)
    }

    schema() {
        return this._schema
    }

    async load(dataset, table, file, schema, config = {}) {
        const loadConfiguration = Object.assign({
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_TRUNCATE',
            ignoreUnknownValues: true,
            schema: {
                fields: schema
            }
        }, config)

        return await this.bq.dataset(dataset).table(table).import(file, loadConfiguration)
    }
}

class SchemaStream extends Writable {
    constructor(schema) {
        super({objectMode: true});
        this.schema = schema
    }

    _write(chunk, encoding, callback) {
        this.schema.add(chunk)
        callback()
    }
}

const emptyTarget = value => Array.isArray(value) ? [] : {}
const clone = (value, options) => merge(emptyTarget(value), value, options)

function arrayMerge(target, source, options) {

    let destination = target.slice().concat(source)

    if (!source.some((o) => plain(o))) {
        return destination
    }

    destination = uniq(destination, equal)

    destination = merge.all(destination, {arrayMerge: arrayMerge})

    return [destination]

}

const mapTypes = (inObj) => {
    const obj = Object.assign({}, inObj)
    Object.keys(obj).forEach(key => {
        if (obj[key] && typeof obj[key] === 'object') mapTypes(obj[key]);
        else {
            switch (true) {
                case obj[key] === false || obj[key] === true: return obj[key]
                case obj[key] === null: return obj[key] = null
                case typeof obj[key] === 'number': return obj[key] = 1.1
                default:
                    return obj[key] = '-'
            }
        }
    });
};

class Schema {
    constructor() {
        this.schema = {}
    }
    fromStream() {
        return new SchemaStream(this)
    }

    add(chunk) {

        const chunkCopy = Object.assign({}, chunk)

        mapTypes(chunkCopy)

        this.schema = merge(this.schema, chunkCopy, {arrayMerge: arrayMerge})
    }

    get() {
        return schemaGenerator(this.schema)
    }

}

module.exports = (project, credentialsFile) => {

    const credStore = JSON.parse(fs.readFileSync(credentialsFile, 'utf8'))

    let user

    if (credStore.client_email) {
        user = credStore.client_email
    } else {
        user = credStore.data[0].credential.id_token.email
    }

    return new Google(project, credentialsFile, user)
}