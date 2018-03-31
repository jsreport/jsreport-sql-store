const OdataSql = require('odata-to-sql')
const Promise = require('bluebird')
const uuid = require('uuid').v4

module.exports = (options, dialect, executeQuery) => ({
  load: function (model) {
    this.model = model
    this.odataSql = OdataSql(model, dialect, options.prefix || 'jsreport_')

    return Promise.all(this.odataSql.create().map((q) => executeQuery(q)))
  },
  drop: function () {
    return Promise.all(this.odataSql.drop().map((q) => executeQuery(q)))
  },
  find: async function (entitySet, query, fields, options) {
    if (query.$select && Object.getOwnPropertyNames(query.$select).length > 0) {
      query.$select._id = 1
    }

    const q = this.odataSql.query(entitySet, query)

    const res = await executeQuery(q)
    return res.records.map((r) => this.odataSql.parse(entitySet, r))
  },
  insert: async function (entitySet, doc) {
    doc._id = doc._id || uuid()
    var q = this.odataSql.insert(entitySet, doc)

    await executeQuery(q)
    return doc
  },
  update: async function (entitySet, query, update, options = {}) {
    const q = this.odataSql.update(entitySet, query, update)

    const res = await executeQuery(q)
    if (options.upsert && res.rowsAffected === 0) {
      const insertQ = Object.assign({}, query, update.$set || {}, update.$inc || {})
      return this.insert(entitySet, insertQ)
    }

    return res.rowsAffected
  },
  remove: async function (entitySet, query) {
    const q = this.odataSql.delete(entitySet, query)

    const res = await executeQuery(q)
    return res.rowsAffected
  }
})
