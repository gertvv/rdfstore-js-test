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

function multipleObject(result) {
  return result.triples.map(function(x) { return x.object });
}

function endsWith(str, suffix) {
  return str.indexOf(suffix, str.length - suffix.length) !== -1;
}

function runSyntaxTest(path, queryFile, shouldParse, callback) {
  var queryFile = path + queryFile.nominalValue;
  fs.readFile(queryFile, { 'encoding': 'utf-8' }, function(error, query) {
    if (error) throw error;
    rdfstore.create(function(testStore) {
      try {
        testStore.execute(query);
      } catch(error) {
        return callback(shouldParse ? 'fail' : 'pass', error);
      }
      return callback(shouldParse ? 'pass' : 'fail');
    });
  });
}

function checkTurtleResults(expectedFile, results, callback) {
  callback('ignored', 'CHECK TURTLE RESULTS HERE');
}

function runQueryEvaluationTest(path, store, graph, action, expected, callback) {
  var queryFile = onlyObject(graph.match(action, named(store, 'qt:query'), null));
  var dataFile = optionalObject(graph.match(action, named(store, 'qt:data'), null));
  var graphDataFiles = multipleObject(graph.match(action, named(store, 'qt:graphData'), null));

  rdfstore.create(function(store) {
    function _loadGraph(name, uri, callback) {
      fs.readFile(path + name, { 'encoding' : 'utf-8' }, function(err, data) {
        if (err) return callback(err);
        
        var type = null;
        if (endsWith(name, '.ttl')) type = 'text/turtle';
        else return callback({ message: "Not parsing " + name });

        function storeCallback(success, results) {
          if (!success) return callback({ message: "Error parsing " + name, cause: results }); // results are the error
          return callback(null, results);
        }

        if (uri) {
          store.load(type, data, uri, storeCallback);
        } else {
          store.load(type, data, storeCallback);
        }
      });
    }

    function loadDefaultGraph(callback) {
      if (!dataFile) return callback(null, null);
      _loadGraph(dataFile.nominalValue, null, callback);
    }

    function loadGraph(node, callback) {
      _loadGraph(node.nominalValue, node.nominalValue, callback);
    }

    function loadQuery(callback) {
      var name = queryFile.nominalValue;
      fs.readFile(path + name, { 'encoding' : 'utf-8' }, callback);
    }

    function loadGraphData(callback) {
      async.mapSeries(graphDataFiles, loadGraph, function(err, results) {
        callback(err, results);
      });
    }

    // in series because they modify the same RDF store
    async.series([ loadQuery, loadDefaultGraph, loadGraphData ],
      function(err, results) {
        if (err) return callback('ignored', err);

        var expectedFile = expected.nominalValue;
        var query = results[0];
        try {
          store.execute(query, function(success, results) {
            if (!success) return callback('fail', results);
            if (endsWith(expected.nominalValue, '.ttl')) {
              checkTurtleResults(expectedFile, results, callback);
            } else {
              callback('ignored', { message: "Not handling expected " + expectedFile });
            }
          });
        } catch (error) {
          callback('fail', error);
        }

      });
  });

}

function runTest(path, store, graph, test, callback) {
  var type = onlyObject(graph.match(test, named(store, 'rdf:type'), null));
  var action = optionalObject(graph.match(test, named(store, 'mf:action'), null));
  var result = optionalObject(graph.match(test, named(store, 'mf:result'), null));
  var name = optionalObject(graph.match(test, named(store, 'mf:name'), null));
  var testResult = {
    'uri': test.nominalValue,
    'name': name ? name.nominalValue : null,
    'type': type.nominalValue
  };
  function cb(result, error) {
    testResult.status = result;
    if (error) {
      testResult.error = error;
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
    case store.rdf.resolve('mf:QueryEvaluationTest'):
      runQueryEvaluationTest(path, store, graph, action, result, cb);
      break;
    default:
      cb('ignored', 'test type not implemented');
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
        store.rdf.setPrefix('qt', 'http://www.w3.org/2001/sw/DataAccess/tests/test-query#');
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
