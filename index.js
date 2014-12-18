var rdfstore = require('rdfstore');
var fs = require('fs');

var manifest = './sparql11-test-suite/syntax-query/manifest.ttl';

function named(store, uri) {
  return store.rdf.createNamedNode(store.rdf.resolve(uri));
}

function onlySubject(result) {
  if (result.triples.length != 1) throw "expected 1 triple";
  return result.triples[0].subject;
}

function onlyObject(result) {
  if (result.triples.length != 1) throw "expected 1 triple";
  return result.triples[0].object;
}

function runTest(store, graph, test) {
  var type = onlyObject(graph.match(test, named(store, 'rdf:type'), null));
  var action = onlyObject(graph.match(test, named(store, 'mf:action'), null));
  console.log(test.nominalValue, type.nominalValue, action.nominalValue);
}

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
        var head = onlyObject(graph.match(manifest, named(store, 'mf:entries'), null));
        var nil = store.rdf.resolve('rdf:nil');
        while (head.nominalValue != nil) {
          var item = onlyObject(graph.match(head, named(store, 'rdf:first'), null));
          runTest(store, graph, item);
          head = onlyObject(graph.match(head, named(store, 'rdf:rest'), null));
        }
      });
    });
  });
});
