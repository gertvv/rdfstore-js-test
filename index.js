var rdfstore = require('rdfstore');
var fs = require('fs');
var path = require('path');
var async = require('async');

var inpath = './sparql11-test-suite/';
var outpath = './results.json';

function named(store, uri) {
  return store.rdf.createNamedNode(store.rdf.resolve(uri));
}

function onlySubject(result) {
  if (result.triples.length != 1) throw new Error("expected 1 triple");
  return result.triples[0].subject;
}

function onlyObject(result) {
  if (result.triples.length != 1) throw new Error("expected 1 triple");
  return result.triples[0].object;
}

function optionalObject(result) {
  if (result.triples.length > 1) throw new Error("expected 0 or 1 triple");
  return result.triples.length == 1 ? result.triples[0].object : null;
}

function runSyntaxTest(path, queryFile, shouldParse, callback) {
  var queryFile = path + queryFile.nominalValue;
  fs.readFile(queryFile, { 'encoding': 'utf-8' }, function(error, query) {
    if (error) throw error;
    rdfstore.create(function(testStore) {
      try {
        testStore.execute(query);
      } catch(error) {
        return callback(!shouldParse, error);
      }
      return callback(shouldParse);
    });
  });
}

function runTest(path, store, graph, test, callback) {
  var type = onlyObject(graph.match(test, named(store, 'rdf:type'), null));
  var action = optionalObject(graph.match(test, named(store, 'mf:action'), null));
  var name = optionalObject(graph.match(test, named(store, 'mf:name'), null));
  var testResult = {
    'uri': test.nominalValue,
    'name': name ? name.nominalValue : null,
    'type': type.nominalValue
  };
  function cb(result, error) {
    if (!result && error) {
      testResult.status = 'fail';
      testResult.error = error;
    } else {
      testResult.status = 'pass';
    }
    callback(null, testResult);
  }
  switch (type.nominalValue) {
    case store.rdf.resolve('mf:PositiveSyntaxTest'):
    case store.rdf.resolve('mf:PositiveSyntaxTest11'):
      runSyntaxTest(path, action, true, cb);
      break;
    case store.rdf.resolve('mf:NegativeSyntaxTest'):
    case store.rdf.resolve('mf:NegativeSyntaxTest11'):
      runSyntaxTest(path, action, false, cb);
      break;
    default:
      testResult.status = 'ignored';
      callback(null, testResult);
  }
}

function rdfCollection(store, graph, head, f) {
  var array = [];
  var nil = store.rdf.resolve('rdf:nil');
  while (head.nominalValue != nil) {
    var item = onlyObject(graph.match(head, named(store, 'rdf:first'), null));
    array.push(item);
    head = onlyObject(graph.match(head, named(store, 'rdf:rest'), null));
  }
  return array;
}

function runTests(store, graph, entries) {
}

function runManifest(inpath, manifestName, callback) {
  var manifest = inpath + manifestName;
  rdfstore.create(function(store) {
    fs.readFile(manifest, { 'encoding': 'utf-8' }, function(err, data) {
      if (err) throw err;
      store.load('text/turtle', data, function(success, results) {
        if (!success) throw results;

        // Get the tests from the manifest the hard way, because the SPARQL engine
        // can not be trusted with property paths
        store.rdf.setPrefix('mf', 'http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#');
        store.graph(function(success, graph) {
          if (!success) throw graph;
          var manifest = onlySubject(
            graph.match(null, named(store, 'rdf:type'), named(store, 'mf:Manifest')));
          var entriesNode = graph.match(manifest, named(store, 'mf:entries'), null);
          var includesNode = graph.match(manifest, named(store, 'mf:include'), null);

          function processEntries(callback) {
            if (entriesNode.triples.length == 1) {
              var entries = rdfCollection(store, graph, onlyObject(entriesNode));
              async.map(entries, function(item, callback) {
                runTest(inpath, store, graph, item, callback);
              }, function(err, results) {
                callback(err, results);
              });
            } else {
              callback(null, null);
            }
          }

          function processIncludes(callback) {
            if (includesNode.triples.length == 1) {
              var includes = rdfCollection(store, graph, onlyObject(includesNode));
              async.map(includes, function(item, callback) {
                var dirname = path.dirname(item.nominalValue);
                var basename = path.basename(item.nominalValue);
                runManifest(inpath + dirname + '/', basename, callback);
              }, function(err, results) {
                callback(err, results);
              });
            } else {
              callback(null, null);
            }
          }

          async.parallel([ processEntries, processIncludes ],
              function(err, arr) {
                var result = { 'manifest' : inpath + manifestName };
                if (arr[0]) result.entries = arr[0];
                if (arr[1]) result.includes = arr[1];
                callback(null, result);
              });
        });
      });
    });
  });
}

runManifest(inpath, 'manifest-all.ttl', function(err, results) {
  if (!err) {
    fs.writeFile(outpath, JSON.stringify(results, null, 4));
  }
});
