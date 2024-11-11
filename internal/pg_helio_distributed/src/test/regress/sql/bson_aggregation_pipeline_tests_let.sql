SET search_path TO helio_core,helio_api,helio_api_catalog,helio_api_internal;

SET citus.next_shard_id TO 413000;
SET helio_api.next_collection_id TO 4130;
SET helio_api.next_collection_index_id TO 4130;


SELECT helio_api.insert_one('db','aggregation_pipeline_let','{"_id":"1", "int": 10, "a" : { "b" : [ "x", 1, 2.0, true ] } }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let','{"_id":"2", "double": 2.0, "a" : { "b" : {"c": 3} } }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let','{"_id":"3", "boolean": false, "a" : "no", "b": "yes", "c": true }', NULL);

-- fetch all rows
SELECT shard_key_value, object_id, document FROM helio_api.collection('db', 'aggregation_pipeline_let') ORDER BY object_id;

-- add newField

-- without let support guc
set helio_api.enableLetSupport to off;
set helio_api.enableLookupLetSupport to off;
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": 20 } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$lt": [ "$_id", "$$varRef" ]} }, "let": { "varRef": 30 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ "$_id", "$$varRef" ]} } } ], "let": { "varRef": "3" } }');

-- with ignore guc does not fail
set helio_api.ignoreLetOnQuerySpec TO true;
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ "$_id", "3" ]} } } ], "let": { "varRef": "3" } }');

set helio_api.ignoreLetOnQuerySpec TO DEFAULT;

-- empty doc without guc works
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : 1 }, "filter": { "_id": { "$lt": 3 }}, "let": { } }');

set helio_api.enableLetSupport to DEFAULT;
set helio_api.enableLookupLetSupport to DEFAULT;
-- with let enabled
EXPLAIN (VERBOSE ON) SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": 20 } }');

SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": 20 } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": { "$avg": [ 20, 40 ] } } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varNotRef" }, "filter": {}, "let": { "varRef": { "$avg": [ 20, 40 ] } } }');

-- let support in $expr
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$lt": [ "$_id", "$$varRef" ]} }, "let": { "varRef": "3" } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ]} }, "let": { "varRef": 3 } }');

SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$lt": [ "$_id", "$$varNotRef" ]} }, "let": { "varRef": "3" } }');

-- let support in $expr with nested $let
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varNotRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
-- same scenario but with aggregation pipeline
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ "$_id", "$$varRef" ]} } } ], "let": { "varRef": "3" } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } } } ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ { "$toInt": "$_id" }, "$$varNotRef" ] } } } ], "let": { "varRef": 3 } }');

-- let support in $expr with nested $let
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varNotRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
-- same scenario but with aggregation pipeline
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$let": { "vars": { "varRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} } } ], "let": { "varRef": 3 } }');

-- find/aggregate with variables referencing other variables on the same let spec should work
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply"  }, "filter": {}, "let": { "varRef": 20, "add": {"$add": ["$$varRef", 2]}, "multiply": {"$multiply": ["$$add", 2]} } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{"$project": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply"  }}], "let": { "varRef": 20, "add": {"$add": ["$$varRef", 2]}, "multiply": {"$multiply": ["$$add", 2]} } }');

-- nested $let should also work
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply", "nestedLet": "$$nestedLet"  }, "filter": {}, "let": { "varRef": 20, "add": {"$add": ["$$varRef", 2]}, "multiply": {"$multiply": ["$$add", 2]}, "nestedLet": {"$let": {"vars": {"add": {"$add": ["$$multiply", 1]}}, "in": "$$add"}} } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{"$project": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply", "nestedLet": "$$nestedLet"  }}], "let": { "varRef": 20, "add": {"$add": ["$$varRef", 2]}, "multiply": {"$multiply": ["$$add", 2]}, "nestedLet": {"$let": {"vars": {"add": {"$add": ["$$multiply", 1]}}, "in": "$$add"}} } }');

-- if we change the order and the variable that we're referencing is defined afterwards we should fail
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply"  }, "filter": {}, "let": { "add": {"$add": ["$$varRef", 2]}, "varRef": 20, "multiply": {"$multiply": ["$$add", 2]} } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{"$project": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply"  }}], "let": { "add": {"$add": ["$$varRef", 2]}, "varRef": 20, "multiply": {"$multiply": ["$$add", 2]} } }');

-- $addFields
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$addFields": { "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$set": { "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');

-- $replaceRoot
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$replaceRoot": { "newRoot": { "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } } }} ], "let": { "varRef": 3 } }');

-- $replaceWith
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$replaceWith": { "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');

-- $group (with simple aggregators)
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" } } } ], "let": { "varRef": 3 } }');

-- $group (with sorted accumulators)
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$addFields": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" }, "first": { "$first" : "$a.b" } } } ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$addFields": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" }, "last": { "$last" : "$a.b" } } } ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$set": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id": "$$varRef", "top": {"$top": {"output": [ "$_id" ], "sortBy": { "_id": 1 }}}}} ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$set": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id": "$$varRef", "bottom": {"$bottom": {"output": [ "$_id" ], "sortBy": { "_id": 1 }}}}} ], "let": { "varRef": 3 } }');

-- $project
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$project": { "_id": 1, "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');

-- $unionWith
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$addFields": { "c": "foo" }}, { "$unionWith": { "coll": "aggregation_pipeline_let", "pipeline": [ { "$addFields": { "bar": "$$varRef" } } ] } } ], "let": { "varRef": 30 }}');

-- $sortByCount
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$sortByCount": { "$lt": [ "$_id", "$$varRef" ] } } ], "let": { "varRef": "2" } }');

-- $facet
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$addFields": { "newField": "myvalue" }}, { "$facet": { "sb1": [ { "$addFields": { "myVar": "$$varRef" }} ], "sb2": [ { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" } } } ] }} ], "let": { "varRef": "2" } }');

-- $graphLookup
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_gl', '{ "_id" : 1, "name" : "Dev" }');
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_gl', '{ "_id" : 2, "name" : "Eliot", "reportsTo" : "Dev" }');
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_gl', '{ "_id" : 3, "name" : "Ron", "reportsTo" : "Eliot" }');
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_gl', '{ "_id" : 4, "name" : "Andrew", "reportsTo" : "Eliot" }');
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_gl', '{ "_id" : 5, "name" : "Asya", "reportsTo" : "Ron" }');
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_gl', '{ "_id" : 6, "name" : "Dan", "reportsTo" : "Andrew" }');

SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let_gl", "pipeline": [ { "$graphLookup": { "from": "aggregation_pipeline_let_gl", "startWith": { "$max": [ "$reportsTo", "$$reportsTo" ] }, "connectFromField": "reportsTo", "connectToField": "name", "as": "reportingHierarchy" } } ], "let": { "reportsTo": "Dev" } }');


-- $inverseMatch
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_inv', '{ "_id" : 1, "policy" : { "name": "Dev" } }');
SELECT helio_api.insert_one('db', 'aggregation_pipeline_let_inv', '{ "_id" : 1, "policy" : { "name": "Elliot" } }');

SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let_inv", "pipeline": [ { "$inverseMatch": { "path": "policy", "from": "aggregation_pipeline_let_gl", "pipeline": [{"$match": {"$expr": { "$eq": [ "$name", "$$varRef"] }} }] }  }], "let": { "varRef": "Dev" } }');


-- $window operators
SELECT helio_api.insert_one('db','aggregation_pipeline_let_setWindowFields','{ "_id": 1, "a": "abc", "cost": 10, "quantity": 501, "date": { "$date": { "$numberLong": "1718841600000" } } }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_setWindowFields','{ "_id": 2, "a": "def", "cost": 8, "quantity": 502, "date": { "$date": { "$numberLong": "1718841605000" } } }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_setWindowFields','{ "_id": 3, "a": "ghi", "cost": 4, "quantity": 503, "date": { "$date": { "$numberLong": "1718841610000" } } }', NULL);

SELECT document FROM helio_api_catalog.bson_aggregation_pipeline('db',
    '{ "aggregate": "aggregation_pipeline_let_setWindowFields", "pipeline":  [{"$setWindowFields": {"partitionBy": { "$concat": ["$a", "$$varRef" ] }, "output": {"total": { "$sum": "$$varRefNum"}}}}], "let": { "varRef": "prefix", "varRefNum": 2 } }');

-- $lookup
SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelinefrom',' {"_id": 1, "name": "American Steak House", "food": ["filet", "sirloin"], "quantity": 100 , "beverages": ["beer", "wine"]}', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelinefrom','{ "_id": 2, "name": "Honest John Pizza", "food": ["cheese pizza", "pepperoni pizza"], "quantity": 120, "beverages": ["soda"]}', NULL);

SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelineto','{ "_id": 1, "item": "filet", "restaurant_name": "American Steak House", "qval": 100 }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelineto','{ "_id": 2, "item": "cheese pizza", "restaurant_name": "Honest John Pizza", "drink": "lemonade", "qval": 120 }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelineto','{ "_id": 3, "item": "cheese pizza", "restaurant_name": "Honest John Pizza", "drink": "soda", "qval": 140 }', NULL);


SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelinefrom_second',' {"_id": 1, "country": "America", "qq": 100 }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_pipelinefrom_second','{ "_id": 2, "country": "Canada", "qq": 120 }', NULL);

-- Add a $lookup with let
SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [ { "$match": { "$expr": { "$eq": [ "$quantity", "$$qval" ] } }}, { "$addFields": { "addedQval": "$$qval" }} ], "as": "matched_docs", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

-- Add a $lookup with let but add a subquery stage
SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [ { "$sort": { "_id": 1 } }, { "$match": { "$expr": { "$eq": [ "$quantity", "$$qval" ] } }} ], "as": "matched_docs", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [ { "$addFields": { "addedQvalBefore": "$$qval" }}, { "$sort": { "_id": 1 } }, { "$match": { "$expr": { "$eq": [ "$quantity", "$$qval" ] } }}, { "$addFields": { "addedQvalAfter": "$$qval" }} ], "as": "matched_docs", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

-- nested $lookup
SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [{ "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom_second", "localField": "qq", "foreignField": "qval", "as": "myExtra", "let": { "secondVar": "$quantity" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$qq", "$$secondVar" ] }  }} ] }} ], "as": "myMatch", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [{ "$addFields": { "addedVal": "$$qval" }}, { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom_second", "localField": "qq", "foreignField": "qval", "as": "myExtra", "let": { "secondVar": "$quantity" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$qq", "$$secondVar" ] }  }} ] }} ], "as": "myMatch", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

-- multiple variable in lookup let

SELECT helio_api.insert('db', '{"insert":"orderscoll", "documents":[
  { "_id": 1, "orderId": "A001", "productId": "P001", "quantity": 10 },
  { "_id": 2, "orderId": "A002", "productId": "P002", "quantity": 5 },
  { "_id": 3, "orderId": "A003", "productId": "P001", "quantity": 2 }
]

}');

SELECT helio_api.insert('db', '{"insert":"products", "documents":[
  { "_id": "P001", "name": "Product 1", "price": 100 },
  { "_id": "P002", "name": "Product 2", "price": 200 }
]

}');

SELECT document from bson_aggregation_pipeline('db', '{ "aggregate": "orderscoll", "pipeline": [ { "$lookup": { "from": "products", "let": { "productId": "$productId", "hello" : "parag" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$_id", "$$productId" ] } } }, { "$project": { "name": 1, "price": 1, "_id": 0 , "field" : "$$hello" } } ], "as": "productDetails" } }, { "$unwind": "$productDetails" } ] , "cursor": {} }');
EXPLAIN (COSTS OFF) SELECT document from bson_aggregation_pipeline('db', '{ "aggregate": "orderscoll", "pipeline": [ { "$lookup": { "from": "products", "let": { "productId": "$productId", "hello" : "parag" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$_id", "$$productId" ] } } }, { "$project": { "name": 1, "price": 1, "_id": 0 , "field" : "$$hello" } } ], "as": "productDetails" } }, { "$unwind": "$productDetails" } ] , "cursor": {} }');

-- only part of the pipeline uses let
SELECT document from bson_aggregation_pipeline('db', '{ "aggregate": "orderscoll", "pipeline": [ { "$lookup": { "from": "products", "let": { "productId": "$productId", "hello" : "parag" }, "pipeline": [ { "$match": { "_id": { "$gt": "P001" } } }, { "$project": { "name": 1, "price": 1, "_id": 0 , "field" : "$$hello" } } ], "as": "productDetails" } }, { "$unwind": "$productDetails" } ] , "cursor": {} }');
EXPLAIN (COSTS OFF) SELECT document from bson_aggregation_pipeline('db', '{ "aggregate": "orderscoll", "pipeline": [ { "$lookup": { "from": "products", "let": { "productId": "$productId", "hello" : "parag" }, "pipeline": [ { "$match": { "_id": { "$gt": "P001" } } }, { "$project": { "name": 1, "price": 1, "_id": 0 , "field" : "$$hello" } } ], "as": "productDetails" } }, { "$unwind": "$productDetails" } ] , "cursor": {} }');


SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [{ "$addFields": { "addedVal": "$$qval" }}, { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom_second", "localField": "qq", "foreignField": "qval", "as": "myExtra", "let": { "secondVar": "$quantity" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$qq", "$$secondVar" ] }  }}, { "$addFields": { "addedVal": "$$qval" }} ] }} ], "as": "myMatch", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$sort": { "_id": 1 }}, { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [{ "$addFields": { "addedVal": "$$qval" }}, { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom_second", "localField": "qq", "foreignField": "qval", "as": "myExtra", "let": { "secondVar": "$quantity" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$qq", "$$secondVar" ] }  }}, { "$addFields": { "addedVal": "$$secondVar" }} ] }} ], "as": "myMatch", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');


-- with an expression that returns empty, the variable should be defined as empty
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "c": "$$c" }, "filter": {}, "let": { "c": {"$getField": {"field": "c", "input": {"a": 1}}}} }');

-- $literal should be treated as literal
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "c": "$$c" }, "filter": {}, "let": { "c": {"$literal": "$$NOW"}} }');

-- these should fail - parse and validate no path, CURRENT or ROOT in let spec.
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": "$a" } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": "$$ROOT" } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": "$$CURRENT" } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": { "$sum": [ "$a", "$a" ] } } }');

-- TODO $$NOW should pass
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": {}, "let": { "varRef": "$$NOW" } }');

-- internal, lookup expression eval merge should wrap variables into $literal and if the variable evaluates to empty, should transform into $$REMOVE
SELECT helio_api_internal.bson_dollar_lookup_expression_eval_merge('{"_id": 1, "b": "$someField"}', '{ "local_b" : "$b", "local_a": "$a" }'::helio_core.bson, '{}'::helio_core.bson);
SELECT helio_api_internal.bson_dollar_lookup_expression_eval_merge('{"_id": 1, "b": "$someField"}', '{ "local_b" : "$b", "local_a": "$a", "local_var1": "$$var1" }'::helio_core.bson, '{"var1": {"$literal": "ABC"}}'::helio_core.bson);

-- lookup with let when a field is missing the path a variable references and when that field is a string in the form of a field expression

SELECT helio_api.insert_one('db','aggregation_pipeline_let_lookup_missing','{"_id":"1", "a": { } }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_lookup_missing','{"_id":"2", "b": 1 }', NULL);
SELECT helio_api.insert_one('db','aggregation_pipeline_let_lookup_missing','{"_id":"3", "a": "$notAFieldPath" }', NULL);

SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let_lookup_missing", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_lookup_missing", "as": "res", "let": {"local_a": "$a"}, "pipeline": [ { "$match": {"$expr": {"$eq": ["$$local_a", "$a"]}}}, {"$project":{"_id": 1}} ] } } ], "cursor": {} }');

/* Shard the collections */
SELECT helio_api.shard_collection('db', 'aggregation_pipeline_let', '{ "_id": "hashed" }', false);

SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$lt": [ "$_id", "$$varNotRef" ]} }, "let": { "varRef": "3" } }');
-- let support in $expr with nested $let
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varNotRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
-- same scenario but with aggregation pipeline
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ "$_id", "$$varRef" ]} } } ], "let": { "varRef": "3" } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } } } ], "let": { "varRef": 3 } }');

-- let support in $expr with nested $let
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "newField" : "$$varRef" }, "filter": { "$expr": { "$let": { "vars": { "varNotRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} }, "let": { "varRef": 3 } }');
-- same scenario but with aggregation pipeline
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$match": { "$expr": { "$let": { "vars": { "varRef": 2 }, "in": { "$lt": [ { "$toInt": "$_id" }, "$$varRef" ] } }} } } ], "let": { "varRef": 3 } }');

-- find/aggregate with variables referencing other variables on the same let spec should work
SELECT document FROM bson_aggregation_find('db', '{ "find": "aggregation_pipeline_let", "projection": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply"  }, "filter": {}, "let": { "varRef": 20, "add": {"$add": ["$$varRef", 2]}, "multiply": {"$multiply": ["$$add", 2]} } }');

-- nested $let should also work
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{"$project": { "varRef" : "$$varRef", "add": "$$add", "multiply": "$$multiply", "nestedLet": "$$nestedLet"  }}], "let": { "varRef": 20, "add": {"$add": ["$$varRef", 2]}, "multiply": {"$multiply": ["$$add", 2]}, "nestedLet": {"$let": {"vars": {"add": {"$add": ["$$multiply", 1]}}, "in": "$$add"}} } }');

-- $addFields
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$addFields": { "newField1" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');

-- $replaceRoot
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$replaceRoot": { "newRoot": { "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } } }} ], "let": { "varRef": 3 } }');

-- $replaceWith
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$replaceWith": { "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');

-- $group (with simple aggregators)
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" } } } ], "let": { "varRef": 3 } }');

-- $group (with sorted accumulators)
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$addFields": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" }, "first": { "$first" : "$a.b" } } } ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$addFields": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" }, "last": { "$last" : "$a.b" } } } ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$set": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id": "$$varRef", "top": {"$top": {"output": [ "$_id" ], "sortBy": { "_id": 1 }}}}} ], "let": { "varRef": 3 } }');
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$set": {"newField": "new"} }, { "$sort": { "_id": 1} }, { "$group": { "_id": "$$varRef", "bottom": {"$bottom": {"output": [ "$_id" ], "sortBy": { "_id": 1 }}}}} ], "let": { "varRef": 3 } }');

-- $project
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$project": { "_id": 1, "newField" : "$$varRef", "c": { "$lt": [ "$_id", "$$varRef" ] } }} ], "let": { "varRef": 3 } }');

-- $unionWith
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [{ "$addFields": { "c": "foo" }}, { "$unionWith": { "coll": "aggregation_pipeline_let", "pipeline": [ { "$addFields": { "bar": "$$varRef" } } ] } } ], "let": { "varRef": 30 }}');

-- $sortByCount
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$sortByCount": { "$lt": [ "$_id", "$$varRef" ] } } ], "let": { "varRef": "2" } }');

-- $facet
SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let", "pipeline": [ { "$addFields": { "newField": "myvalue" }}, { "$facet": { "sb1": [ { "$addFields": { "myVar": "$$varRef" }} ], "sb2": [ { "$group": { "_id" : "$$varRef", "c": { "$sum": "$$varRef" } } } ] }} ], "let": { "varRef": "2" } }');

-- $graphLookup
SELECT helio_api.shard_collection('db', 'aggregation_pipeline_let_gl', '{ "_id": "hashed" }', false);

SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let_gl", "pipeline": [ { "$graphLookup": { "from": "aggregation_pipeline_let_gl", "startWith": { "$max": [ "$reportsTo", "$$reportsTo" ] }, "connectFromField": "reportsTo", "connectToField": "name", "as": "reportingHierarchy" } } ], "let": { "reportsTo": "Dev" } }');

-- $inverseMatch
SELECT helio_api.shard_collection('db', 'aggregation_pipeline_let_inv', '{ "_id": "hashed" }', false);

SELECT document FROM bson_aggregation_pipeline('db', '{ "aggregate": "aggregation_pipeline_let_inv", "pipeline": [ { "$inverseMatch": { "path": "policy", "from": "aggregation_pipeline_let_gl", "pipeline": [{"$match": {"$expr": { "$eq": [ "$name", "$$varRef"] }} }] }  }], "let": { "varRef": "Dev" } }');

-- $window operators
SELECT helio_api.shard_collection('db', 'aggregation_pipeline_let_setWindowFields', '{ "_id": "hashed" }', false);

SELECT document FROM bson_aggregation_pipeline('db',
    '{ "aggregate": "aggregation_pipeline_let_setWindowFields", "pipeline":  [{"$setWindowFields": {"partitionBy": { "$concat": ["$a", "$$varRef" ] }, "output": {"total": { "$sum": "$$varRefNum"}}}}], "let": { "varRef": "prefix", "varRefNum": 2 } }');

-- $lookup
SELECT helio_api.shard_collection('db', 'aggregation_pipeline_let_pipelineto', '{ "_id": "hashed" }', false);

SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [ { "$match": { "$expr": { "$eq": [ "$quantity", "$$qval" ] } }}, { "$addFields": { "addedQval": "$$qval" }} ], "as": "matched_docs", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

-- Add a $lookup with let but add a subquery stage
SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [ { "$sort": { "_id": 1 } }, { "$match": { "$expr": { "$eq": [ "$quantity", "$$qval" ] } }} ], "as": "matched_docs", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [ { "$addFields": { "addedQvalBefore": "$$qval" }}, { "$sort": { "_id": 1 } }, { "$match": { "$expr": { "$eq": [ "$quantity", "$$qval" ] } }}, { "$addFields": { "addedQvalAfter": "$$qval" }} ], "as": "matched_docs", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

-- nested $lookup
SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [{ "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom_second", "localField": "qq", "foreignField": "qval", "as": "myExtra", "let": { "secondVar": "$quantity" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$qq", "$$secondVar" ] }  }} ] }} ], "as": "myMatch", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');

SELECT document from bson_aggregation_pipeline('db', 
  '{ "aggregate": "aggregation_pipeline_let_pipelineto", "pipeline": [ { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom", "pipeline": [{ "$addFields": { "addedVal": "$$qval" }}, { "$lookup": { "from": "aggregation_pipeline_let_pipelinefrom_second", "localField": "qq", "foreignField": "qval", "as": "myExtra", "let": { "secondVar": "$quantity" }, "pipeline": [ { "$match": { "$expr": { "$eq": [ "$qq", "$$secondVar" ] }  }} ] }} ], "as": "myMatch", "localField": "restaurant_name", "foreignField": "name", "let": { "qval": "$qval" } }} ], "cursor": {} }');
