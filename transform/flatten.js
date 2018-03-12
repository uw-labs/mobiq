const { Transform } = require('stream');

var flattenObject = function(ob, skipArrays) {
    var toReturn = {};

    for (var i in ob) {
        if (!ob.hasOwnProperty(i)) continue;

        if (skipArrays && Array.isArray(ob[i])) continue;

        if ((typeof ob[i]) == 'object') {
            var flatObject = flattenObject(ob[i], skipArrays);
            for (var x in flatObject) {
                if (!flatObject.hasOwnProperty(x)) continue;

                toReturn[i + '.' + x] = flatObject[x];
            }
        } else {
            toReturn[i] = ob[i];
        }
    }
    return toReturn;
};

module.exports = class FlattenTransform extends Transform {
    constructor(skipArrays) {
        super({objectMode: true})
        this.skipArrays = skipArrays
    }

    _transform(chunk, encoding, callback) {
        this.push(flattenObject(chunk, this.skipArrays));
        callback()
    }
}