const {Transform} = require('stream');

const sanitiseNames = (obj) => {
    Object.keys(obj).forEach(key => {

            const newKey = key.replace(/[^a-zA-Z0-9]/, '_')

            if (newKey !== key) {
                obj[newKey] = obj[key]
                delete obj[key]
            }

            if (obj[newKey] && typeof obj[newKey] === 'object') {
                sanitiseNames(obj[newKey]);
            }
        }
    )
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

