const OdataSql = require('odata-to-sql')
const Promise = require('bluebird')
const uuid = require('uuid').v4

class Cursor {
  constructor (entitySet, odataSql, executeQuery, query, fields) {
    this.entitySet = entitySet
    this.odataSql = odataSql
    this.executeQuery = executeQuery
    this.query = {
      $filter: Object.assign({}, query),
      $select: Object.assign({}, fields)
    }

    if (Object.getOwnPropertyNames(this.query.$select).length > 0) {
      this.query.$select._id = 1
    }
  }

  async toArray () {
    const q = this.odataSql.query(this.entitySet, this.query)

    const res = await this.executeQuery(q)
    return res.records.map((r) => this.odataSql.parse(this.entitySet, r))
  }

  skip (v) {
    this.query.$skip = v
    return this
  }

  limit (v) {
    this.query.$limit = v
    return this
  }

  async count () {
    const q = this.odataSql.count(this.entitySet, this.query)

    const res = await this.executeQuery(q)
    return parseInt(res.records[0]['undefined_count'])
  }

  sort (h) {
    this.query.$sort = Object.assign({}, this.query.$sort, h)
    return this
  }
}

module.exports = (options, dialect, executeQuery) => ({
  load: function (model) {
    this.model = model
    this.odataSql = OdataSql(model, dialect, options.prefix || 'jsreport_')

    return Promise.all(this.odataSql.create().map((q) => executeQuery(q)))
  },
  drop: function () {
    return Promise.all(this.odataSql.drop().map((q) => executeQuery(q)))
  },
  find: function (entitySet, query, fields, options) {
    // $limit, $skip, $sort
    return new Cursor(entitySet, this.odataSql, executeQuery, query, fields)
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
      await this.insert(entitySet, insertQ)
      return 1
    }

    return res.rowsAffected
  },
  remove: async function (entitySet, query) {
    const q = this.odataSql.delete(entitySet, query)

    const res = await executeQuery(q)
    return res.rowsAffected
  }
})
