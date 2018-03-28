const { Transform } = require('stream');

module.exports = class FlattenTransform extends Transform {
    constructor() {
        super({objectMode: true})
    }

    _transform(chunk, encoding, callback) {
        chunk.createdOn = new Date(parseInt(chunk.id.substring(0, 8), 16) * 1000);
        this.push(chunk);
        callback()
    }
}