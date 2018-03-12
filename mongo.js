const {MongoClient} = require('mongodb')

class Mongo {
    async connect(dsn) {
        this.connection = await MongoClient.connect(dsn)
    }

    use(collection) {
        this.collection = this.connection.db().collection(collection)
    }

    rowCount(query, skip) {
        return this.collection.count(query, {skip})
    }

    stream(query, skip, batch) {
        return this.collection.find(query, {skip}).batchSize(batch)
    }
}

module.exports = async (dsn, collection) => {
    const db = new Mongo()

    await db.connect(dsn)

    db.use(collection)

    return db
}