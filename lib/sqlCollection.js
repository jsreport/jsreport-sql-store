var q = require('q')
var CollectionBase = require('jsreport-core/lib/store/collectionBase')
var util = require('util')
var uuid = require('uuid').v1
var _ = require('lodash')

var SqlCollection = module.exports = function SqlCollection () {
  CollectionBase.apply(this, arguments)
}

util.inherits(SqlCollection, CollectionBase)

SqlCollection.prototype.invokeFind = function (query) {
  return this.executeFind({$filter: query})
}

SqlCollection.prototype.executeFind = function (query) {
  var self = this
  var q = this.provider.odataSql.query(this.name, query)

  return this.provider.executeQuery(q).then(function (res) {
    if (query.$inlinecount) {
      return res.records
    }

    return res.records.map(function (r) {
      return self.provider.odataSql.parse(self.name, r)
    })
  }).then(function (res) {
    if (query.$inlinecount) {
      query = _.extend({}, query)
      delete query.$inlinecount
      return self.executeFind(query).then(function (resNoCount) {
        return {
          count: res[0][Object.keys(res[0])[0]],
          value: resNoCount
        }
      })
    }
    return res
  })
}

SqlCollection.prototype.invokeCount = function (query) {
  return q(0)
}

SqlCollection.prototype.invokeInsert = function (doc) {
  doc._id = uuid()
  var q = this.provider.odataSql.insert(this.name, doc)

  return this.provider.executeQuery(q).then(function () {
    return doc
  })
}

SqlCollection.prototype.invokeUpdate = function (query, update, options) {
  var q = this.provider.odataSql.update(this.name, query, update)

  var self = this

  return this.provider.executeQuery(q).then(function (res) {
    if (options && options.upsert && res.rowsAffected === 0) {
      query = _.extend(query, query.$set || {}, update.$inc || {})
      return self.invokeInsert(query)
    }

    return res.rowsAffected
  })
}

SqlCollection.prototype.invokeRemove = function (query) {
  var q = this.provider.odataSql.delete(this.name, query)

  return this.provider.executeQuery(q).then(function (res) {
    return res.rowsAffected
  })
}

