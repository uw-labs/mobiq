const { Transform } = require('stream');

module.exports = class JsonTransform extends Transform {
    constructor() {
        super({objectMode: true})
    }

    _transform(chunk, encoding, callback) {
        this.push(JSON.stringify(chunk) + '\n');
        callback()
    }
}