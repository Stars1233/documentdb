
/*
 * processes a documentdb insert wire protocol command.
 */
CREATE OR REPLACE FUNCTION __API_SCHEMA_V2__.insert(
    p_database_name text,
    p_insert __CORE_SCHEMA_V2__.bson,
    p_insert_documents __CORE_SCHEMA_V2__.bsonsequence default NULL,
    p_transaction_id text default NULL,
    p_result OUT __CORE_SCHEMA_V2__.bson,
    p_success OUT boolean)
 RETURNS record
 LANGUAGE C
AS 'MODULE_PATHNAME', $$command_insert$$;
COMMENT ON FUNCTION __API_SCHEMA_V2__.insert(text,__CORE_SCHEMA_V2__.bson,__CORE_SCHEMA_V2__.bsonsequence,text)
    IS 'inserts documents into a documentdb collection for wire protocol command';

/* Command: insert */
CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL_V2__.insert_one(
    p_collection_id bigint,
    p_shard_key_value bigint,
    p_document __CORE_SCHEMA_V2__.bson,
    p_transaction_id text)
 RETURNS bool
 LANGUAGE C
AS 'MODULE_PATHNAME', $$command_insert_one$$;
COMMENT ON FUNCTION __API_SCHEMA_INTERNAL_V2__.insert_one(bigint,bigint,__CORE_SCHEMA_V2__.bson,text)
    IS 'internal command to insert one document into a collection';


/* Command: insert */
CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL_V2__.insert_worker(
    p_collection_id bigint,
    p_shard_key_value bigint,
    p_shard_oid regclass,
    p_insert_internal_spec __CORE_SCHEMA_V2__.bson,
    p_insert_internal_docs __CORE_SCHEMA_V2__.bsonsequence,
    p_transaction_id text)
 RETURNS __CORE_SCHEMA_V2__.bson
 LANGUAGE C
AS 'MODULE_PATHNAME', $$command_insert_worker$$;

/* insert procedure will use when not called inside transaction */
CREATE OR REPLACE PROCEDURE __API_SCHEMA_V2__.insert_bulk(
    IN p_database_name text,
    IN p_insert __CORE_SCHEMA_V2__.bson,
    IN p_insert_documents __CORE_SCHEMA_V2__.bsonsequence default NULL,
    IN p_transaction_id text default NULL,
    INOUT p_result __CORE_SCHEMA_V2__.bson default NULL,
    INOUT p_success boolean default NULL)
 LANGUAGE C
AS 'MODULE_PATHNAME', $$command_insert_bulk$$;
COMMENT ON PROCEDURE __API_SCHEMA_V2__.insert_bulk(text,__CORE_SCHEMA_V2__.bson,__CORE_SCHEMA_V2__.bsonsequence,text,__CORE_SCHEMA_V2__.bson,boolean)
    IS 'inserts documents into a collection for a wire protocol command outside transaction';
