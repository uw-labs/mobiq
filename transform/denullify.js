const { Transform } = require('stream');

// https://stackoverflow.com/questions/286141/remove-blank-attributes-from-an-object-in-javascript
const removeEmpty = (obj) => {
    Object.keys(obj).forEach(key => {
        if (obj[key] && typeof obj[key] === 'object') {
            if (!Object.keys(obj[key]).length) {
                delete obj[key];
            } else {
                removeEmpty(obj[key]);
            }
        }
        else if (obj[key] == null) delete obj[key];
    });
};

module.exports = class FlattenTransform extends Transform {
    constructor() {
        super({objectMode: true})
    }

    _transform(chunk, encoding, callback) {

        removeEmpty(chunk)

        this.push(chunk);
        callback()
    }
}

