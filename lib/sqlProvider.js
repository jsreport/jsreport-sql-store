var q = require('q')
var ProviderBase = require('jsreport-core/lib/store/providerBase')
var util = require('util')
var odataSql = require('odata-to-sql')
var SqlCollection = require('./sqlCollection')

var SqlProvider = module.exports = function (reporter, dialect, executeQuery) {
  ProviderBase.call(this, reporter.documentStore.model, reporter.options)
  this.executeQuery = executeQuery
  this.dialect = dialect
}

util.inherits(SqlProvider, ProviderBase)

SqlProvider.prototype.init = function () {
  var self = this
  this.odataSql = odataSql(this._model, this.dialect, this._options.connectionString.prefix || 'jsreport_')

  Object.keys(this._model.entitySets).map(function (key) {
    var entitySet = self._model.entitySets[key]
    var col = new SqlCollection(key, entitySet, self._model.entityTypes[entitySet.entityType.replace('jsreport.', '')], self._options)
    col.provider = self
    self.collections[key] = col
  })

  var promises = self.odataSql.create().map(function (q) {
    return self.executeQuery(q)
  })

  return q.all(promises)
}

SqlProvider.prototype.drop = function () {
  var self = this
  var promises = self.odataSql.drop().map(function (q) {
    return self.executeQuery(q)
  })

  return q.all(promises)
}

SqlProvider.prototype.odata = function (odataServer) {
  var self = this
  odataServer.model(this._model)
    .remove(function (collection, query, cb) {
      self.collections[collection].invokeRemove(query).then(function (res) {
        cb(null, res)
      }).catch(cb)
    })
    .update(function (collection, query, update, cb) {
      self.collections[collection].invokeUpdate(query, update).then(function (res) {
        cb(null, res)
      }).catch(cb)
    })
    .insert(function (collection, doc, cb) {
      self.collections[collection].invokeInsert(doc).then(function (res) {
        cb(null, doc)
      }).catch(cb)
    })
    .query(function (collection, query, cb) {
      self.collections[collection].executeFind(query).then(function (res) {
        cb(null, res)
      }).catch(cb)
    })
}

