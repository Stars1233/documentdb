
/* Command: update */

DROP FUNCTION IF EXISTS __API_SCHEMA_INTERNAL__.bson_update_document;
CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL__.bson_update_document(
    document __CORE_SCHEMA__.bson,
    updateSpec __CORE_SCHEMA__.bson,
    querySpec __CORE_SCHEMA__.bson,
    arrayFilters __CORE_SCHEMA__.bson DEFAULT NULL,
    buildUpdateDesc bool DEFAULT false,
    variableSpec __CORE_SCHEMA__.bson DEFAULT NULL,
    OUT newDocument __CORE_SCHEMA__.bson,
    OUT updateDesc __CORE_SCHEMA__.bson)
 RETURNS RECORD
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $function$bson_update_document$function$;


DROP FUNCTION IF EXISTS __API_SCHEMA_INTERNAL_V2__.update_bson_document;
CREATE OR REPLACE FUNCTION __API_SCHEMA_INTERNAL_V2__.update_bson_document(
    document __CORE_SCHEMA__.bson,
    updateSpec __CORE_SCHEMA__.bson,
    querySpec __CORE_SCHEMA__.bson,
    arrayFilters __CORE_SCHEMA__.bson DEFAULT NULL,
    variableSpec __CORE_SCHEMA__.bson DEFAULT NULL,
    OUT newDocument __CORE_SCHEMA__.bson)
 RETURNS __CORE_SCHEMA__.bson
 LANGUAGE c
 IMMUTABLE PARALLEL SAFE
AS 'MODULE_PATHNAME', $function$bson_update_document$function$;