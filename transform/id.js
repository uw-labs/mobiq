const { Transform } = require('stream');

module.exports = class FlattenTransform extends Transform {
    constructor() {
        super({objectMode: true})
    }

    _transform(chunk, encoding, callback) {
        chunk.id = chunk._id.toString()
        delete chunk._id
        this.push(chunk);
        callback()
    }
}