SET search_path TO documentdb_core,documentdb_api,documentdb_api_catalog,documentdb_api_internal;
SET citus.next_shard_id TO 639000;
SET documentdb.next_collection_id TO 6390;
SET documentdb.next_collection_index_id TO 6390;

SET documentdb.EnableVariablesSupportForWriteCommands TO on;

-- Call delete for a non existent collection.
-- Note that this should not report any logs related to collection catalog lookup.
SET citus.log_remote_commands TO ON;
SELECT documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"a":5},{"a":{"$gt":0}}]},"limit":0}]}');
RESET citus.log_remote_commands;

select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":1,"_id":1}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":2,"_id":2}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":3,"_id":3}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":4,"_id":4}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":5,"_id":5}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":6,"_id":6}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":7,"_id":7}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":8,"_id":8}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":9,"_id":9}');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":10,"_id":10}');

-- exercise invalid delete syntax errors
select documentdb_api.delete('db', NULL);
select documentdb_api.delete(NULL, '{"delete":"removeme", "deletes":[{"q":{},"limit":0}]}');
select documentdb_api.delete('db', '{"deletes":[{"q":{},"limit":0}]}');
select documentdb_api.delete('db', '{"delete":"removeme"}');
select documentdb_api.delete('db', '{"delete":["removeme"], "deletes":[{"q":{},"limit":0}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":{"q":{},"limit":0}}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":0}], "extra":1}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{}}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"limit":0}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":[],"limit":0}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":0,"extra":1}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":0}],"ordered":1}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":5}]}');

-- Disallow writes to system.views
select documentdb_api.delete('db', '{"delete":"system.views", "deletes":[{"q":{},"limit":0}]}');

-- delete all
begin;
SET LOCAL search_path TO '';
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":0}]}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete some
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$lte":3}},"limit":0}]}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- arbitrary limit type works in Mongo
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$lte":3}},"limit":{"hello":"world"}}]}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete all from non-existent collection
select documentdb_api.delete('db', '{"delete":"notexists", "deletes":[{"q":{},"limit":0}]}');

-- query syntax errors are added the response
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$ltr":5}},"limit":0}]}');

-- when ordered, expect only first delete to be executed
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":1},"limit":0},{"q":{"$a":2},"limit":0},{"q":{"a":3},"limit":0}]}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":1},"limit":0},{"q":{"$a":2},"limit":0},{"q":{"a":3},"limit":0}],"ordered":true}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- when not ordered, expect first and last delete to be executed
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":1},"limit":0},{"q":{"$a":2},"limit":0},{"q":{"a":3},"limit":0}],"ordered":false}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 without filters is supported for unsharded collections
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 is retryable on unsharded collection (second call is a noop)
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":1}]}', NULL, 'xact-1');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":1}]}', NULL, 'xact-1');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 is supported in the _id case
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":6},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":6}';
rollback;

-- delete 1 is supported in the multiple identical _id case
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"_id":6},{"_id":6}]},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":6}';
rollback;

-- delete 1 is supported in the multiple distinct _id case (but a noop)
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"_id":6},{"_id":5}]},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":6}';
rollback;

-- validate _id extraction
begin;
set local citus.log_remote_commands to on;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":6},"limit":1}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"_id":6},{"_id":5}]},"limit":1}]}');
reset citus.log_remote_commands;
rollback;

-- shard the collection
select documentdb_api.shard_collection('db', 'removeme', '{"a":"hashed"}', false);

-- make sure we get the expected results after sharding a collection
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$lte":3}},"limit":0}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"a":1}';
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"a":10}';
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;


-- delete with oject_id and no shard_key works
BEGIN;
select count(*) from documentdb_api.collection('db', 'removeme');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":{"$eq":5}},"limit":0}], "ordered": false }');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- test pruning logic in delete
begin;
select count(*) from documentdb_api.collection('db', 'removeme');
set local citus.log_remote_commands to on;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$eq":5}},"limit":0}]}');
reset citus.log_remote_commands;
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"a":5}';
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

begin;
select count(*) from documentdb_api.collection('db', 'removeme');
set local citus.log_remote_commands to on;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"a":5},{"a":{"$gt":0}}]},"limit":0}]}');
reset citus.log_remote_commands;
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"a":5}';
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 without filters is unsupported for sharded collections
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":1}]}');

-- delete 1 with shard key filters is supported for sharded collections
begin;
select count(*) from documentdb_api.collection('db', 'removeme');
set local citus.log_remote_commands to on;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$eq":5}},"limit":1}]}');
reset citus.log_remote_commands;
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"a":5}';
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 with shard key filters is retryable
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$eq":5}},"limit":1}]}', NULL, 'xact-2');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$eq":5}},"limit":1}]}', NULL, 'xact-2');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 that does not match any rows is still retryable
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$eq":15}},"limit":1}]}', NULL, 'xact-3');
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":15,"_id":15}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a":{"$eq":15}},"limit":1}]}', NULL, 'xact-3');
rollback;

-- delete 1 is supported in the _id case even on sharded collections
begin;
-- add an additional _id 10
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":11,"_id":10}');
-- delete first row where _id = 10
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":10},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":10}';
-- delete second row where _id = 10
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":10},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":10}';
-- no more row where _id = 10
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":10},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":10}';
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 with with _id filter on a sharded collection is retryable
begin;
-- add an additional _id 10 (total to 11 rows)
select 1 from documentdb_api.insert_one('db', 'removeme', '{"a":11,"_id":10}');
-- delete first row where _id = 10
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":10},"limit":1}]}', NULL, 'xact-4');
-- second time is a noop
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"_id":10},"limit":1}]}', NULL, 'xact-4');
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- delete 1 is supported in the multiple identical _id case
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"_id":6},{"_id":6}]},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":6}';
rollback;

-- delete 1 is unsupported in the multiple distinct _id case
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"_id":6},{"_id":5}]},"limit":1}]}');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"_id":6}';
rollback;

-- validate _id extraction
begin;
set local citus.log_remote_commands to on;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"a": 11, "_id":6},"limit":0}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{"$and":[{"a": 11},{"_id":6},{"_id":5}]},"limit":0}]}');
reset citus.log_remote_commands;
rollback;

-- delete with spec in special section
begin;
select count(*) from documentdb_api.collection('db', 'removeme');
select documentdb_api.delete('db', '{"delete":"removeme"}', '{ "":[{"q":{"a":{"$eq":5}},"limit":1}] }');
select count(*) from documentdb_api.collection('db', 'removeme') where document @@ '{"a":5}';
select count(*) from documentdb_api.collection('db', 'removeme');
rollback;

-- deletes with both specs specified 
begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes": [{"q":{"a":{"$eq":5}},"limit":1}] }', '{ "":[{"q":{"a":{"$eq":5}},"limit":1}] }');
rollback;

-- delete with index hint specified by name and by key object
SELECT documentdb_api_internal.create_indexes_non_concurrently('db', '{ "createIndexes": "removeme", "indexes": [ { "key" : { "a": 1 }, "name": "validIndex"}] }', true);

begin;
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":0,"hint": "validIndex"}]}');
select documentdb_api.delete('db', '{"delete":"removeme", "deletes":[{"q":{},"limit":0,"hint": { "a": 1 }}]}');
rollback;

select documentdb_api.drop_collection('db','removeme');

SELECT 1 FROM documentdb_api.insert_one('delete', 'test_sort_returning', '{"_id": 1,"a":3,"b":7}');
SELECT 1 FROM documentdb_api.insert_one('delete', 'test_sort_returning', '{"_id": 2,"a":2,"b":5}');
SELECT 1 FROM documentdb_api.insert_one('delete', 'test_sort_returning', '{"_id": 3,"a":1,"b":6}');

-- sort in ascending order and project & return deleted document
SELECT collection_id AS test_sort_returning FROM documentdb_api_catalog.collections WHERE database_name = 'delete' AND collection_name = 'test_sort_returning' \gset
SELECT documentdb_api_internal.delete_worker(
    p_collection_id=>:test_sort_returning,
    p_shard_key_value=>:test_sort_returning,
    p_shard_oid => 0,
    p_update_internal_spec => '{ "deleteOne": { "query": { "a": {"$gte": 1} },  "sort": { "b": 1 }, "returnDocument": 1, "returnFields": { "a": 0} } }'::bson,
    p_update_internal_docs=>null::bsonsequence,
    p_transaction_id=>null::text
) FROM documentdb_api.collection('delete', 'test_sort_returning');

-- sort by multiple fields (i) and return deleted document
BEGIN;

SELECT collection_id AS test_sort_returning FROM documentdb_api_catalog.collections WHERE database_name = 'delete' AND collection_name = 'test_sort_returning' \gset
SELECT documentdb_api_internal.delete_worker(
    p_collection_id=>:test_sort_returning,
    p_shard_key_value=>:test_sort_returning,
    p_shard_oid => 0,
    p_update_internal_spec => '{ "deleteOne": { "query": { "a": {"$gte": 1} },  "sort": { "b": -1, "a" : 1 }, "returnDocument": 1, "returnFields": { "a": 0} } }'::bson,
    p_update_internal_docs=>null::bsonsequence,
    p_transaction_id=>null::text
) FROM documentdb_api.collection('delete', 'test_sort_returning');

ROLLBACK;

-- sort by multiple fields (ii) and return deleted document
SELECT collection_id AS test_sort_returning FROM documentdb_api_catalog.collections WHERE database_name = 'delete' AND collection_name = 'test_sort_returning' \gset
SELECT documentdb_api_internal.delete_worker(
    p_collection_id=>:test_sort_returning,
    p_shard_key_value=>:test_sort_returning,
    p_shard_oid => 0,
    p_update_internal_spec => '{ "deleteOne": { "query": { "a": {"$gte": 1} },  "sort": { "a": 1, "b" : -1 }, "returnDocument": 1, "returnFields": { "a": 0} } }'::bson,
    p_update_internal_docs=>null::bsonsequence,
    p_transaction_id=>null::text
) FROM documentdb_api.collection('delete', 'test_sort_returning');

SELECT document FROM documentdb_api.collection('delete', 'test_sort_returning') ORDER BY 1;

-- show that we validate "query" document even if collection doesn't exist
-- i) ordered=true
SELECT documentdb_api.delete(
    'delete',
    '{
        "delete": "dne",
        "deletes": [
            {"q": {"a": 1}, "limit": 0 },
            {"q": {"$b": 1}, "limit": 0 },
            {"q": {"c": 1}, "limit": 0 },
            {"q": {"$d": 1}, "limit": 0 },
            {"q": {"e": 1}, "limit": 0 }
        ],
        "ordered": true
     }'
);
-- ii) ordered=false
SELECT documentdb_api.delete(
    'delete',
    '{
        "delete": "dne",
        "deletes": [
            {"q": {"a": 1}, "limit": 0 },
            {"q": {"$b": 1}, "limit": 0 },
            {"q": {"c": 1}, "limit": 0 },
            {"q": {"$d": 1}, "limit": 0 },
            {"q": {"e": 1}, "limit": 0 }
        ],
        "ordered": false
     }'
);

SELECT documentdb_api.create_collection('delete', 'no_match');

-- show that we validate "query" document even if we can't match any documents
-- i) ordered=true
SELECT documentdb_api.delete(
    'delete',
    '{
        "delete": "no_match",
        "deletes": [
            {"q": {"a": 1}, "limit": 0 },
            {"q": {"$b": 1}, "limit": 0 },
            {"q": {"c": 1}, "limit": 0 },
            {"q": {"$d": 1}, "limit": 0 },
            {"q": {"e": 1}, "limit": 0 }
        ],
        "ordered": true
     }'
);
-- ii) ordered=false
SELECT documentdb_api.delete(
    'delete',
    '{
        "delete": "no_match",
        "deletes": [
            {"q": {"a": 1}, "limit": 0 },
            {"q": {"$b": 1}, "limit": 0 },
            {"q": {"c": 1}, "limit": 0 },
            {"q": {"$d": 1}, "limit": 0 },
            {"q": {"e": 1}, "limit": 0 }
        ],
        "ordered": false
     }'
);

-- _id test using $in opearator with explain plan
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":1,"_id":1}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":2,"_id":2}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":3,"_id":3}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":4,"_id":4}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":5,"_id":5}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":6,"_id":6}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":7,"_id":7}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":8,"_id":8}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":9,"_id":9}');
select 1 from documentdb_api.insert_one('db', 'explainTest', '{"a":10,"_id":10}');


EXPLAIN (COSTS OFF, VERBOSE ON) SELECT documentdb_api.delete(
    'db',
    '{
        "delete": "explainTest",
        "deletes": [
            {"q": {"_id": {"$in" : [2,4,6,8,10] } }, "limit": 0 }
        ],
        "ordered": true
     }'
);

EXPLAIN (COSTS OFF, VERBOSE ON) SELECT documentdb_api.delete(
    'db',
    '{
        "delete": "explainTest",
        "deletes": [
            {"q": {"_id": {"$in" : [2,4,6,8,10] } }, "limit": 0 }
        ],
        "ordered": true
     }'
);

SELECT documentdb_api.delete(
    'db',
    '{
        "delete": "explainTest",
        "deletes": [
            {"q": {"_id": {"$in" : [2,4,6,8,10] } }, "limit": 0 }
        ],
        "ordered": true
     }'
);

select document from documentdb_api.collection('db', 'explainTest') order by 1;


-- let support
SELECT documentdb_api.insert_one('db', 'coll_delete', '{"_id": 1, "a":"dog"}');
SELECT documentdb_api.insert_one('db', 'coll_delete', '{"_id": 2, "a":"cat"}');
SELECT documentdb_api.insert_one('db', 'coll_delete', '{"_id": 3, "a":"$$varRef"}');

-- enableVariablesSupportForWriteCommands GUC off: ignore variableSpec
SET documentdb.enableVariablesSupportForWriteCommands TO off;
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": {"$expr": {"$eq": ["$a", "$$varRef"] } }, "limit": 0}], "let": {"varRef": "cat"} }');

-- enableVariablesSupportForWriteCommands GUC on: user variableSpec
SET documentdb.enableVariablesSupportForWriteCommands TO on;

-- variables accessed outside $expr will not evaluate to let variable value
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": {"_id": "$$varRef" }, "limit": 0}], "let": {"varRef": 2}} ');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": {"_id": "$$varRef" }, "limit": 1}], "let": {"varRef": 2}} ');

BEGIN;
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$eq": ["$_id", "$$varRef"] } }, "limit": 1}], "let": {"varRef": 2} }');
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$eq": ["$_id", "$$varRef"] } }, "limit": 1}], "let": {"varRef": 2} }');
ROLLBACK;

BEGIN;
--- deleteOne (1)
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$eq": ["$_id", "$$varRef"] } }, "limit": 1}], "let": {"varRef": 2} }');
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$eq": ["$_id", "$$varRef"] } }, "limit": 1}], "let": {"varRef": 2} }');
ROLLBACK;

BEGIN;
--- deleteMany (2)
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lte": ["$a", "$$varRef"] } }, "limit": 0}], "let": {"varRef": "zebra"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

BEGIN;
-- deleteMany: when ordered, expect only first delete to be executed
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lt": ["$a", "$$varRef1"] } }, "limit": 0}, { "q": { "$expr": {"$lte": ["$a", "$$varRef2"] } }, "limit": 0}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef1"] } }, "limit": 0}], "ordered": true, "let": {"varRef1": 2, "varRef2": "kangaroo"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

BEGIN;
-- deleteOne: when ordered, expect only first delete to be executed
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lt": ["$_id", "$$varRef1"] } }, "limit": 1}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef2"] } }, "limit": 1}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef1"] } }, "limit": 1}], "ordered": true, "let": {"varRef1": 2, "varRef2": "kangaroo"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

BEGIN;
-- deleteMany: when not ordered, expect first and last delete to be executed
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lt": ["$a", "$$varRef1"] } }, "limit": 0}, { "q": { "$expr": {"$lte": ["$a", "$$varRef2"] } }, "limit": 0}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef1"] } }, "limit": 0}], "ordered": false, "let": {"varRef1": 2, "varRef2": "kangaroo"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

BEGIN;
-- deleteOne: when not ordered, expect first and last delete to be executed
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lt": ["$_id", "$$varRef1"] } }, "limit": 1}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef2"] } }, "limit": 1}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef1"] } }, "limit": 1}], "ordered": false, "let": {"varRef1": 2, "varRef2": "kangaroo"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

-- sharded collection
SELECT documentdb_api.shard_collection('db', 'coll_delete', '{ "a": "hashed" }', false);

BEGIN;
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$eq": ["$a", "$$varRef"] } }, "limit": 1}], "let": {"varRef": 2} }');
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$eq": ["$a", "$$varRef"] } }, "limit": 1}], "let": {"varRef": 2} }');
ROLLBACK;

BEGIN;
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": {"_id": 2, "$expr": {"$eq": ["$a", "$$varRef"] } }, "limit": 1}], "let": {"varRef": "cat"} }');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

BEGIN;
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lte": ["$a", "$$varRef"] } }, "limit": 0}], "let": {"varRef": "zebra"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

BEGIN;
SELECT document from documentdb_api.collection('db', 'coll_delete');
SELECT documentdb_api.delete('db', '{ "delete": "coll_delete", "deletes": [ { "q": { "$expr": {"$lt": ["$a", "$$varRef1"] } }, "limit": 0}, { "q": { "$expr": {"$lte": ["$a", "$$varRef2"] } }, "limit": 0}, { "q": { "$expr": {"$lte": ["$_id", "$$varRef1"] } }, "limit": 0}], "ordered": true, "let": {"varRef1": 2, "varRef2": "kangaroo"}} ');
SELECT document from documentdb_api.collection('db', 'coll_delete');
ROLLBACK;

RESET documentdb.enableVariablesSupportForWriteCommands;