const {Transform} = require('stream');

const sanitiseNames = (obj) => {
    Object.keys(obj).forEach(key => {

        let newKey = key

        if (!Array.isArray(obj)) {
            newKey = key.replace(/[^a-zA-Z0-9]/g, '_').replace(/^([^a-zA-Z])(.+)/, '_$1$2')
        }

        if (newKey !== key) {
            obj[newKey] = obj[key]
            delete obj[key]
        }

        if (obj[newKey] && typeof obj[newKey] === 'object') {
            sanitiseNames(obj[newKey]);
        }
    })
}

module.exports = class FlattenTransform extends Transform {
    constructor() {
        super({objectMode: true})
    }

    _transform(chunk, encoding, callback) {

        sanitiseNames(chunk)

        this.push(chunk);
        callback()
    }
}

