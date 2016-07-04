var changes = require('concurrent-couch-follower')
var each = require('async.each')
var http = require('http')
var log = require('pino')()
var normalize = require('normalize-registry-metadata')
var through = require('through2')
var url = require('url')
var uuid = require('uuid')

var level = require('levelup')('.leveldb', {
  db: require('leveldown'),
  valueEncoding: 'json'
})

if (process.env.FOLLOW !== 'false')  {
  changes(function (data, done) {
    var doc = normalize(data.doc)
    var name = doc.name
    var triples = []
    Object.keys(doc.versions).forEach(function (version) {
      var license = doc.versions[version].license
      triples.push([name, version, license])
    })
    each(triples, function (triple, done) {
      recordLicense(triple[0], triple[1], triple[2], done)
    }, done)
  }, {
    db: 'https://replicate.npmjs.com',
    include_docs: true,
    sequence: '.sequence',
    now: false,
    concurrency: 5
  })
}

function recordLicense (package, version, license, callback) {
  log.info({package: package, version: version, license: license})
  var batch = [
    {
      type: 'put',
      key: key('packages', package),
      value: null
    },
    {
      type: 'put',
      key: key('package-versions', package, version),
      value: null
    },
  ]
  if (typeof license === 'string') {
    batch.push({
      type: 'put',
      key: key('package-to-license', package, version),
      value: license
    })
    batch.push({
      type: 'put',
      key: key('license-to-package', license),
      value: {package: package, version: version}
    })
    batch.push({
      type: 'put',
      key: key('licenses', license),
      value: null
    })
  }
  level.batch(batch, callback)
}

function key () {
  return Array.prototype.slice.call(arguments)
    .map(encodeURIComponent)
    .join('/')
}

function parseKey (key) {
  return key.split('/').map(decodeURIComponent)
}

var routes = require('http-hash')()

routes.set('/', function (request, response) {
  
})

routes.set('/licenses', function (request, response) {
  level.createReadStream({
    gt: key('licenses', ''),
    lte: key('licenses', '~'),
    values: false
  })
    .pipe(through.obj(function (key, _, done) {
      var parsed = parseKey(key)
      this.push(parsed[1] + '\n')
      done()
    }))
    .pipe(response)
})

http.createServer()
  .on('request', function (request, response) {
    request.log = log.child({uuid: uuid()})
    request.log.info(request)
    response.once('finish', function () { request.log.info(response) })
    var parsed = url.parse(request.url)
    var route = routes.get(parsed.pathname)
    if (route.handler) {
      route.handler(request, response, route.params)
    } else {
      response.statusCode = 404
      response.end()
    }
  })
  .once('close', function () { level.close() })
  .listen(process.env.PORT || 8080)

function closeServer () {
  server.close(function () { process.exit(0) })
}

process
  .on('SIGQUIT', closeServer)
  .on('SIGINT', closeServer)
  .on('uncaughtException', function (exception) {
    log.error(exception)
    closeServer()
  })
