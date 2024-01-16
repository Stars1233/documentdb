/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/metadata/metadata_cache.c
 *
 * Implementation of general metadata caching functions.
 *
 *-------------------------------------------------------------------------
 */
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <access/genam.h>
#include <access/table.h>
#include <catalog/pg_extension.h>
#include <catalog/pg_proc.h>
#include <utils/fmgroids.h>

#include "commands/extension.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/version_utils.h"
#include "catalog/pg_am.h"

#include "metadata/metadata_cache.h"
#include "metadata/collection.h"
#include "commands/defrem.h"


#define PG_EXTENSION_NAME_SCAN_NARGS 1

#define POSTGIS_EXTENSION_SCHEMA "postgis_public"
#define RUM_EXTENSION_SCHEMA "public"

/*
 * CacheValidityValue represents the possible states of the cache.
 */
typedef enum CacheValidityValue
{
	/* cache was not succesfully initialized */
	CACHE_INVALID,

	/* extension does not exist, nothing to cache */
	CACHE_VALID_NO_EXTENSION,

	/* extension exist, cache is valid */
	CACHE_VALID
} CacheValidityValue;


static void InvalidateHelioApiCache(Datum argument, Oid relationId);
static Oid GetBinaryOperatorId(Oid *operatorId, Oid leftTypeOid, char *operatorName,
							   Oid rightTypeOid);
static Oid GetCoreBinaryOperatorId(Oid *operatorId, Oid leftTypeOid, char *operatorName,
								   Oid rightTypeOid);
static Oid GetBinaryOperatorFunctionId(Oid *operatorFuncId, char *operatorName,
									   Oid leftTypeOid, Oid rightTypeOid);
static Oid GetBinaryOperatorFunctionIdMissingOk(Oid *operatorFuncId, char *operatorName,
												Oid leftTypeOid, Oid rightTypeOid,
												const char *releaseName);
static Oid GetPostgresInternalFunctionId(Oid *functionId, char *operatorName);
static Oid GetArrayTypeOid(Oid *arrayTypeId, Oid baseElem);

/* Utilities to get function within a schema with variable arguments */
static Oid GetSchemaFunctionIdWithNargs(Oid *functionId, char *schema,
										char *functionName, int nargs,
										Oid *argTypes, bool missingOk);

/* indicates whether the cache is valid, or needs to be reset */
static CacheValidityValue CacheValidity = CACHE_INVALID;

/* session-level memory context in which we keep all cached bytes */
MemoryContext HelioApiMetadataCacheContext = NULL;

char *ApiDataSchemaName = "helio_data";
char *ApiAdminRole = "helio_admin_role";
char *ApiSchemaName = "helio_api";
char *ApiInternalSchemaName = "helio_api_internal";
char *ExtensionObjectPrefix = "helio";
char *FullBsonTypeName = "helio_core.bson";
char *ApiExtensionName = "pg_helio_api";
char *ApiCatalogSchemaName = "helio_api_catalog";

typedef struct HelioApiOidCacheData
{
	/* OID of the <bigint> OPERATOR(pg_catalog.=) <bigint> operator */
	Oid BigintEqualOperatorId;

	/* OID of the <text> OPERATOR(pg_catalog.=) <text> operator */
	Oid TextEqualOperatorId;

	/* OID of the <bson> OPERATOR(ApiCatalogSchemaName.->) <bson> operator */
	Oid BsonArrowOperatorId;

	/* OID of the vector type */
	Oid VectorTypeId;

	/* OID of the index_spec_type */
	Oid IndexSpecTypeId;

	/* OID of the ApiCatalogSchemaName.collections */
	Oid MongoCatalogCollectionsTypeOid;

	/* OID of the <bson> OPERATOR(ApiCatalogSchemaName.=) <bson> operator */
	Oid BsonEqualOperatorId;

	/* OID of the <bson> OPERATOR(ApiCatalogSchemaName.@=) <bson> operator */
	Oid BsonEqualMatchOperatorId;

	/* OID of the ApiSchemaName.bson_query_match() function */
	Oid BsonQueryMatchFunctionId;

	/* OID of the <bson> OPERATOR(ApiCatalogSchemaName.@@) <bson> operator */
	Oid BsonQueryOperatorId;

	/* OID of the bson_true_match function */
	Oid BsonTrueFunctionId;

	/* OID of the bson_empty_data_table function */
	Oid BsonEmptyDataTableFunctionId;

	/* OID of the coll_stats_aggregation function */
	Oid CollStatsAggregationFunctionOid;

	/* OID of the index_stats_aggregation function */
	Oid IndexStatsAggregationFunctionOid;

	/* OID of the current_op aggregation function */
	Oid BsonCurrentOpAggregationFunctionId;

	/* OID of the mongo_api_v1.list_indexes function */
	Oid IndexSpecAsBsonFunctionId;

	/* OID of the sample_collection function */
	Oid ApiSampleCollectionFunctionId;

	/* OID of the TABLESAMPLE SYSTEM_ROWS(n) function */
	Oid ExtensionTableSampleSystemRowsFunctionId;

	/* OID of mongo_api_v1.collection() UDF */
	Oid CollectionFunctionId;

	/* OID of mongo_api_v1.create_indexes() UDF */
	Oid CreateIndexesProcedureId;

	/* OID of mongo_api_v1.re_index() UDF */
	Oid ReindexProcedureId;

	/* OID of ApiCatalogSchemaName.collections table */
	Oid CollectionsTableId;

	/* OID of collections_collection_id_seq sequence */
	Oid CollectionIdSequenceId;

	/* OID of collection_indexes_index_id_seq sequence */
	Oid CollectionIndexIdSequenceId;

	/* OID of ApiCatalogSchemaName schema */
	Oid MongoCatalogNamespaceId;

	/* OID of the current extension */
	Oid HelioApiExtensionId;

	/* OID of the owner of the current extension */
	Oid HelioApiExtensionOwner;

	/* OID of the bson_orderby operator */
	Oid BsonOrderByQueryOperatorId;

	/* OID of the bson_orderby function */
	Oid BsonOrderByFunctionId;

	/* OID of the bson vector search orderby operator */
	Oid VectorOrderByQueryOperatorId;

	/* OID of the pg_vector cosine similarity operator */
	Oid VectorCosineSimilaritySearchOperatorId;

	/* OID of the pg_vector l2 similarity operator */
	Oid VectorL2SimilaritySearchOperatorId;

	/* OID of the pg_vector ip similarity operator */
	Oid VectorIPSimilaritySearchOperatorId;

	/* OID of the pg_vector ivfflat cosine similarity operator */
	Oid VectorIVFFlatCosineSimilarityOperatorFamilyId;

	/* OID of the pg_vector hnsw cosine similarity operator */
	Oid VectorHNSWCosineSimilarityOperatorFamilyId;

	/* OID of the pg_vector ivfflat l2 similarity operator */
	Oid VectorIVFFlatL2SimilarityOperatorFamilyId;

	/* OID of the pg_vector hnsw l2 similarity operator */
	Oid VectorHNSWL2SimilarityOperatorFamilyId;

	/* OID of the pg_vector ivfflat ip similarity operator */
	Oid VectorIVFFlatIPSimilarityOperatorFamilyId;

	/* OID of the pg_vector hnsw ip similarity operator */
	Oid VectorHNSWIPSimilarityOperatorFamilyId;

	/* OID of gin_bson_exclusion_pre_consistent function */
	Oid BsonExclusionPreconsistentFunctionId;

	/* OID of the greater than '>' operator for bson */
	Oid BsonGreaterThanOperatorId;

	/* OID of the less than '>=' operator for bson */
	Oid BsonGreaterThanEqualOperatorId;

	/* OID of the less than '<' operator for bson */
	Oid BsonLessThanOperatorId;

	/* OID of the less than '<=' operator for bson */
	Oid BsonLessThanEqualOperatorId;

	/* OID of the $eq function for bson query */
	Oid BsonEqualMatchRuntimeFunctionId;

	/* Oid of the $eq runtime operator #= */
	Oid BsonEqualMatchRuntimeOperatorId;

	/* OID of the $eq function for bson index */
	Oid BsonEqualMatchIndexFunctionId;

	/* OID of the $gt function for bson query */
	Oid BsonGreaterThanMatchRuntimeFunctionId;

	/* Oid of the $gt runtime operator #> */
	Oid BsonGreaterThanMatchRuntimeOperatorId;

	/* OID of the $gt function for bson index */
	Oid BsonGreaterThanMatchIndexFunctionId;

	/* OID of the $gte function for bson query */
	Oid BsonGreaterThanEqualMatchRuntimeFunctionId;

	/* Oid of the $gte runtime operator #>= */
	Oid BsonGreaterThanEqualMatchRuntimeOperatorId;

	/* OID of the $gte function for bson index */
	Oid BsonGreaterThanEqualMatchIndexFunctionId;

	/* OID of the $lt function for bson query */
	Oid BsonLessThanMatchRuntimeFunctionId;

	/* Oid of the $lt runtime operator #< */
	Oid BsonLessThanMatchRuntimeOperatorId;

	/* OID of the $lt function for bson index */
	Oid BsonLessThanMatchIndexFunctionId;

	/* OID of the $lte function for bson query */
	Oid BsonLessThanEqualMatchRuntimeFunctionId;

	/* Oid of the $lte runtime operator #<= */
	Oid BsonLessThanEqualMatchRuntimeOperatorId;

	/* OID of the $lte function for bson index */
	Oid BsonLessThanEqualMatchIndexFunctionId;

	/* Oid of the bson_dollar_range function */
	Oid BsonRangeMatchFunctionId;

	/* Oid of the $range runtime operator #<> */
	Oid BsonRangeMatchOperatorOid;

	/* OID of the $in function for bson */
	Oid BsonInMatchFunctionId;

	/* OID of the $nin function for bson */
	Oid BsonNinMatchFunctionId;

	/* OID of the $ne function for bson */
	Oid BsonNotEqualMatchFunctionId;

	/* OID of the $all function for bson */
	Oid BsonAllMatchFunctionId;

	/* OID of the $elemMatch function for bson */
	Oid BsonElemMatchMatchFunctionId;

	/* OID of the $regex function for bson */
	Oid BsonRegexMatchFunctionId;

	/* OID of the $mod function for bson */
	Oid BsonModMatchFunctionId;

	/* OID of the $size function for bson */
	Oid BsonSizeMatchFunctionId;

	/* OID of the $type function for bson */
	Oid BsonTypeMatchFunctionId;

	/* OID of the $exists function for bson */
	Oid BsonExistsMatchFunctionId;

	/* OID of the cursor state function */
	Oid CursorStateFunctionId;

	/* OID of the current curor state function */
	Oid CurrentCursorStateFunctionId;

	/* OID of the $bitsAllClear function for bson */
	Oid BsonBitsAllClearFunctionId;

	/* OID of the $bitsAnyClear function for bson */
	Oid BsonBitsAnyClearFunctionId;

	/* OID of the $bitsAllSet function for bson */
	Oid BsonBitsAllSetFunctionId;

	/* OID of the $bitsAnySet function for bson */
	Oid BsonBitsAnySetFunctionId;

	/* OID of the $expr function for bson */
	Oid BsonExprFunctionId;

	/* OID of the $text function for bson */
	Oid BsonTextFunctionId;

	/* OID of the $eq function function for bson_values */
	Oid BsonValueEqualMatchFunctionId;

	/* OID of the $gt function function for bson_values */
	Oid BsonValueGreaterMatchFunctionId;

	/* OID of the $gte function function for bson_values */
	Oid BsonValueGreaterEqualMatchFunctionId;

	/* OID of the $lt function function for bson_values */
	Oid BsonValueLessMatchFunctionId;

	/* OID of the $lte function function for bson_values */
	Oid BsonValueLessEqualMatchFunctionId;

	/* OID of the $size function function for bson_values */
	Oid BsonValueSizeMatchFunctionId;

	/* OID of the $type function function for bson_values */
	Oid BsonValueTypeMatchFunctionId;

	/* OID of the $in function function for bson_values */
	Oid BsonValueInMatchFunctionId;

	/* OID of the $nin function function for bson_values */
	Oid BsonValueNinMatchFunctionId;

	/* OID of the $ne function function for bson_values */
	Oid BsonValueNotEqualMatchFunctionId;

	/* OID of the $exists function function for bson_values */
	Oid BsonValueExistsMatchFunctionId;

	/* OID of the $elemMatch function for bson_values */
	Oid BsonValueElemMatchMatchFunctionId;

	/* OID of the $all function for bson_values */
	Oid BsonValueAllMatchFunctionId;

	/* OID of the $regex function function for bson_values */
	Oid BsonValueRegexMatchFunctionId;

	/* OID of the $mod function for bson_values */
	Oid BsonValueModMatchFunctionId;

	/* OID of the $bitsAllClear function function for bson_values */
	Oid BsonValueBitsAllClearFunctionId;

	/* OID of the $bitsAnyClear function function for bson_values */
	Oid BsonValueBitsAnyClearFunctionId;

	/* OID of the $bitsAllSet function function for bson_values */
	Oid BsonValueBitsAllSetFunctionId;

	/* OID of the $bitsAnySet function function for bson_values */
	Oid BsonValueBitsAnySetFunctionId;

	/* OID of the drandom postgres method which generates a random float number in range [0 - 1) */
	Oid PostgresDrandomFunctionId;

	/* OID of the float8_timestamptz postgres method which generates a timestamp from a unix epoch in seconds */
	Oid PostgresToTimestamptzFunctionId;

	/* OID of the date_part postgres method which get's a specific unit part of a date */
	Oid PostgresDatePartFunctionId;

	/* OID of the timestamptz_zone postgres method which shifts the current timestamp to the specified timezone */
	Oid PostgresTimestampToZoneFunctionId;

	/* OID of Rum Index access methods */
	Oid RumIndexAmId;

	/* OID Of the vector ivfflat index access methods */
	Oid PgVectorIvfFlatIndexAmId;

	/* OID Of the vector hnsw index access methods */
	Oid PgVectorHNSWIndexAmId;

	/* OID of the array_to_vector function. */
	Oid PgDoubleToVectorFunctionOid;

	/* OID of the vector as vector Cast function */
	Oid VectorAsVectorFunctionOid;

	/* OID of the bson_extract_vector function from a document and path */
	Oid ApiCatalogBsonExtractVectorFunctionId;

	/* OID of the bson_search_param function to wrap search parameter. */
	Oid ApiBsonSearchParamFunctionId;

	/* OID of the websearch_to_tsquery function. */
	Oid WebSearchToTsQueryFunctionId;

	/* OID of the websearch_to_tsquery function with regconfig option. */
	Oid WebSearchToTsQueryWithRegConfigFunctionId;

	/* OID of the rum_extract_tsvector function */
	Oid RumExtractTsVectorFunctionId;

	/* OID of the operator class for BSON Text operations with pgmongo_rum */
	Oid BsonRumTextPathOperatorFamily;

	/* OID of the operator class for BSON Single Path operations with pgmongo_rum */
	Oid BsonRumSinglePathOperatorFamily;

	/* OID of the bson_text_meta_qual function ID */
	Oid BsonTextSearchMetaQualFuncId;

	/* OID of the ts_rank function id */
	Oid PostgresTsRankFunctionId;

	/* OID of the tsvector_concat function */
	Oid TsVectorConcatFunctionId;

	/* OID of the ts_match_vq function */
	Oid TsMatchFunctionOid;

	/* OID of the bson_aggregation_pipeline function */
	Oid ApiCatalogAggregationPipelineFunctionId;

	/* OID of the bson_aggregation_find function */
	Oid ApiCatalogAggregationFindFunctionId;

	/* OID of the bson_aggregation_count function */
	Oid ApiCatalogAggregationCountFunctionId;

	/* OID of the bson_aggregation_distinct function */
	Oid ApiCatalogAggregationDistinctFunctionId;

	/* OID of the bson_dollar_add_fields function */
	Oid ApiCatalogBsonDollarAddFieldsFunctionOid;

	/* OID of the bson_dollar_project function */
	Oid ApiCatalogBsonDollarProjectFunctionOid;

	/* OID of the bson_dollar_project_find function */
	Oid ApiCatalogBsonDollarProjectFindFunctionOid;

	/* OID of the bson_dollar_unwind(bson, text) function */
	Oid ApiCatalogBsonDollarUnwindFunctionOid;

	/* OID of the bson_dollar_unwind(bson, bson) function */
	Oid ApiCatalogBsonDollarUnwindWithOptionsFunctionOid;

	/* OID of the bson_dollar_replace_root function */
	Oid ApiCatalogBsonDollarReplaceRootFunctionOid;

	/* OID of the BSONSUM aggregate function */
	Oid ApiCatalogBsonSumAggregateFunctionOid;

	/* OID of the BSONAVERAGE aggregate function */
	Oid ApiCatalogBsonAverageAggregateFunctionOid;

	/* OID of the bson_array_agg function */
	Oid ApiCatalogBsonArrayAggregateFunctionOid;

	/* OID of the mongo bson_distinct_agg function */
	Oid ApiCatalogBsonDistinctAggregateFunctionOid;

	/* OID of the bson_object_agg function */
	Oid ApiCatalogBsonObjectAggregateFunctionOid;

	/* OID of the BSONMAX aggregate function */
	Oid ApiCatalogBsonMaxAggregateFunctionOid;

	/* OID of the BSONMIN aggregate function */
	Oid ApiCatalogBsonMinAggregateFunctionOid;

	/* OID of the BSONFIRSTONSORTED aggregate function */
	Oid ApiCatalogBsonFirstOnSortedAggregateFunctionOid;

	/* OID of the BSONLASTONSORTED aggregate function */
	Oid ApiCatalogBsonLastOnSortedAggregateFunctionOid;

	/* OID of the BSONFIRST aggregate function */
	Oid ApiCatalogBsonFirstAggregateFunctionOid;

	/* OID of the BSONLAST aggregate function */
	Oid ApiCatalogBsonLastAggregateFunctionOid;

	/* OID of the BSONFIRSTNONSORTED aggregate function */
	Oid ApiCatalogBsonFirstNOnSortedAggregateFunctionOid;

	/* OID of the BSONLASTNONSORTED aggregate function */
	Oid ApiCatalogBsonLastNOnSortedAggregateFunctionOid;

	/* OID of the BSONFIRSTN aggregate function */
	Oid ApiCatalogBsonFirstNAggregateFunctionOid;

	/* OID of the BSONLASTN aggregate function */
	Oid ApiCatalogBsonLastNAggregateFunctionOid;

	/* OID of the bson_repath_and_build function */
	Oid ApiCatalogBsonRepathAndBuildFunctionOid;

	/* OID of the row_get_bson function */
	Oid ApiCatalogRowGetBsonFunctionOid;

	/* OID of the bson_expression_get function */
	Oid ApiCatalogBsonExpressionGetFunctionOid;

	/* OID of the bson_expression_map function */
	Oid ApiCatalogBsonExpressionMapFunctionOid;

	/* OID of the pg_catalog.random() function */
	Oid PgRandomFunctionOid;

	/* OID of the bson_dollar_lookup_extract_filter_expression function */
	Oid ApiCatalogBsonLookupExtractFilterExpressionOid;

	/* OID of the bson_lookup_unwind function */
	Oid BsonLookupUnwindFunctionOid;

	/* OID of the bson_distinct_unwind function */
	Oid BsonDistinctUnwindFunctionOid;

	/* Postgis box2df type id */
	Oid Box2dfTypeId;

	/* Postgis geometry type id */
	Oid GeometryTypeId;

	/* Postgis geography type id */
	Oid GeographyTypeId;

	/* Postgis GIDX type id */
	Oid GIDXTypeId;

	/* Postgis geometry array type id */
	Oid GeometryArrayTypeId;

	Oid BsonDollarGeoIntersectsFunctionOid;

	/* Oid of bson_dollar_geowithin function */
	Oid BsonDollarGeowithinFunctionOid;

	/* Oid of bson_extract_geometry function */
	Oid BsonExtractGeometryFunctionId;

	/* Oid of bson_extract_geometry_array function */
	Oid BsonExtractGeometryArrayFunctionId;

	/* Oid of the geometry::geography cast function */
	Oid PostgisGeometryAsGeography;

	/* Oid of the ST_IsValidReason function  */
	Oid PostgisGeometryIsValidDetailFunctionId;

	/* Oid of bson_validate_geometry function */
	Oid BsonValidateGeometryFunctionId;

	/* Oid of bson_validate_geography function */
	Oid BsonValidateGeographyFunctionId;

	/* Oid of the Postgis GIST support function geometry_gist_compress_2d */
	Oid PostgisGeometryGistCompress2dFunctionId;

	/* Oid of the Postgis GIST support function geography_gist_compress */
	Oid PostgisGeographyGistCompressFunctionId;

	/* Oid of the Postgis GIST support function geometry_gist_consistent_2d */
	Oid PostgisGeometryGistConsistent2dFunctionId;

	/* Oid of the Postgis GIST support function geography_gist_consistent */
	Oid PostgisGeographyGistConsistentFunctionId;

	/* Oid of the Box3d Postgis function */
	Oid PostgisMake3dBoxFunctionId;

	/* Oid of the ST_MakeEnvelope Postgis function */
	Oid PostgisMakeEnvelopeFunctionId;

	/* Oid of the ST_MakePoint Postgis function */
	Oid PostgisMakePointFunctionId;

	/* Oid of the ST_buffer Postgis function */
	Oid PostgisBufferFunctionId;

	/* Oid of the ST_Collect Postgis function */
	Oid PostgisCollectFunctionId;

	/* Oid of the ST_Area Postgis function */
	Oid PostgisGeometryAreaFunctionId;

	/* Oid of the st_geomfromwkb Postgis function */
	Oid PostgisGeometryFromEWKBFunctionId;

	/* Oid of the ST_MakePolygon Postgis function */
	Oid PostgisMakePolygonFunctionId;

	/* Oid of the ST_MakeLine Postgis function */
	Oid PostgisMakeLineFunctionId;

	/* Oid of the ST_geogfromwkb function */
	Oid PostgisGeographyFromWKBFunctionId;

	/* Oid of ST_Covers (geography) Postgis function */
	Oid PostgisGeographyCoversFunctionId;

	/* Oid of overlaps_2d(box2df, geometry) postgis function */
	Oid PostgisBox2dfGeometryOverlapsFunctionId;

	/* Oid of overlaps_geog(gidx, geography) Postgis function */
	Oid PostgisGIDXGeographyOverlapsFunctionId;

	/* Oid of the ST_Covers (geometry) Postgis function */
	Oid PostgisGeometryCoversFunctionId;

	/* Oid of the ST_Intersects Postgis function */
	Oid PostgisGeographyIntersectsFunctionId;

	/* Oid of the ST_SetSRID Postgis function */
	Oid PostgisSetSRIDFunctionId;

	/* Oid of the ApiInternalSchemaName.index_build_is_in_progress function */
	Oid IndexBuildIsInProgressFunctionId;

	/* Oid of the ApiDataSchemaName namespace */
	Oid ApiDataNamespaceOid;
} HelioApiOidCacheData;

static HelioApiOidCacheData Cache;

/*
 * InitializeHelioApiExtensionCache (re)initializes the cache.
 *
 * This function either completes and sets CacheValidity to valid, or throws
 * an OOM and leaves CacheValidity as invalid. In the latter case, any allocated
 * memory will be reset on the next invocation.
 */
void
InitializeHelioApiExtensionCache(void)
{
	if (CacheValidity == CACHE_VALID)
	{
		return;
	}

	/* we create a memory context and register the invalidation handler once */
	if (HelioApiMetadataCacheContext == NULL)
	{
		/* postgres does not always initialize CacheMemoryContext */
		CreateCacheMemoryContext();

		HelioApiMetadataCacheContext = AllocSetContextCreate(CacheMemoryContext,
															 "HelioApiMetadataCacheContext ",
															 ALLOCSET_DEFAULT_SIZES);

		CacheRegisterRelcacheCallback(InvalidateHelioApiCache, (Datum) 0);
	}

	/* reset any previously allocated memory. Code below is sensitive to OOMs */
	MemoryContextReset(HelioApiMetadataCacheContext);

	/* clear the cache data */
	memset(&Cache, 0, sizeof(Cache));

	/*
	 * Check whether the extension exists and is not still be created or
	 * altered.
	 */
	bool missingOK = true;
	Cache.HelioApiExtensionId = get_extension_oid(ApiExtensionName, missingOK);
	if (Cache.HelioApiExtensionId == InvalidOid ||
		(CurrentExtensionObject == Cache.HelioApiExtensionId && creating_extension))
	{
		CacheValidity = CACHE_VALID_NO_EXTENSION;

		return;
	}

	/* since the extension exists, we expect ApiCatalogSchemaName to exist too */
	missingOK = false;
	Cache.MongoCatalogNamespaceId = get_namespace_oid(ApiCatalogSchemaName, missingOK);

	/* look up the ApiCatalogSchemaName.collections OID to catch invalidations */
	Cache.CollectionsTableId = get_relname_relid("collections",
												 Cache.MongoCatalogNamespaceId);

	/* after cache reset (e.g. drop+create extension), also reset collections cache */
	ResetCollectionsCache();

	/* we made it here without out of memory errors */
	CacheValidity = CACHE_VALID;
}


/* Invalidates the collections cache using the collections table oid.
 * this is used to be able to invalidate the cache via the version cache
 * so that the lifetime of both are tight together.
 */
void
InvalidateCollectionsCache()
{
	if (Cache.CollectionsTableId != InvalidOid)
	{
		InvalidateHelioApiCache((Datum) 0, Cache.CollectionsTableId);
	}
}


/*
 * InvalidateHelioApiCache is called when receiving invalidations from other
 * backends.
 *
 * This can happen any time postgres code calls AcceptInvalidationMessages(), e.g
 * after obtaining a relation lock. We remove entries from the cache. They will
 * still be temporarily usable until new entries are added to the cache.
 */
static void
InvalidateHelioApiCache(Datum argument, Oid relationId)
{
	if (relationId == InvalidOid || relationId == Cache.CollectionsTableId)
	{
		/*
		 * Invalidations of ApiCatalogSchemaName.collections typically indicate
		 * CREATE/ALTER/DROP EXTENSION. Reset the whole cache.
		 */
		CacheValidity = CACHE_INVALID;
		ResetCollectionsCache();
		InvalidateVersionCache();
	}
	else
	{
		/* got an invalidation for a specific relation */

		if (CacheValidity == CACHE_VALID)
		{
			/* free the collection cache entry for the given relation */
			InvalidateCollectionByRelationId(relationId);
		}
		else
		{
			/*
			 * If the cache is not valid, we'll reset the collections
			 * cache on the next call to InitializeHelioApiExtensionCache.
			 */
		}
	}
}


/*
 * IsHelioApiExtensionActive returns whether the current extension exists and is
 * usable (not being altered, no pg_upgrade in progress).
 */
bool
IsHelioApiExtensionActive(void)
{
	InitializeHelioApiExtensionCache();

	return CacheValidity == CACHE_VALID && !IsBinaryUpgrade &&
		   !(creating_extension && CurrentExtensionObject == Cache.HelioApiExtensionId);
}


/*
 * HelioApiExtensionOwner returns OID of the owner of current extension.
 */
Oid
HelioApiExtensionOwner(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.HelioApiExtensionOwner != InvalidOid)
	{
		return Cache.HelioApiExtensionOwner;
	}

	bool useIndex = true;
	Snapshot scanSnapshot = NULL;

	ScanKeyData scanKey[PG_EXTENSION_NAME_SCAN_NARGS];
	ScanKeyInit(&scanKey[0], Anum_pg_extension_extname, BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(ApiExtensionName));

	Relation relation = table_open(ExtensionRelationId, AccessShareLock);
	SysScanDesc scandesc = systable_beginscan(relation, ExtensionNameIndexId, useIndex,
											  scanSnapshot, PG_EXTENSION_NAME_SCAN_NARGS,
											  scanKey);

	/* there can be at most one matching tuple */
	HeapTuple extensionTuple = systable_getnext(scandesc);
	if (!HeapTupleIsValid(extensionTuple))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("helio extension has not been loaded")));
	}

	Form_pg_extension extensionForm = (Form_pg_extension) GETSTRUCT(extensionTuple);
	Cache.HelioApiExtensionOwner = extensionForm->extowner;

	systable_endscan(scandesc);
	table_close(relation, AccessShareLock);

	return Cache.HelioApiExtensionOwner;
}


/*
 * ApiCollectionFunctionId returns the OID of the mongo_api_v1.collection()
 * function.
 */
Oid
ApiCollectionFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CollectionFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("collection"));
		Oid paramOids[2] = { TEXTOID, TEXTOID };
		bool missingOK = false;

		Cache.CollectionFunctionId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.CollectionFunctionId;
}


/*
 * BigintEqualOperatorId returns the OID of the <bigint> = <bigint> operator.
 */
Oid
BigintEqualOperatorId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BigintEqualOperatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString("pg_catalog"),
											makeString("="));

		Cache.BigintEqualOperatorId =
			OpernameGetOprid(operatorNameList, INT8OID, INT8OID);
	}

	return Cache.BigintEqualOperatorId;
}


Oid
TextEqualOperatorId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.TextEqualOperatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString("pg_catalog"),
											makeString("="));

		Cache.TextEqualOperatorId =
			OpernameGetOprid(operatorNameList, TEXTOID, TEXTOID);
	}

	return Cache.TextEqualOperatorId;
}


/*
 * ApiCreateIndexesProcedureId returns the OID of the
 * mongo_api_v1.create_indexes() procedure.
 */
Oid
ApiCreateIndexesProcedureId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CreateIndexesProcedureId == InvalidOid)
	{
		ObjectWithArgs *objectWithArgs = makeNode(ObjectWithArgs);
		objectWithArgs->objname = list_make2(makeString(ApiSchemaName),
											 makeString("create_indexes"));

		objectWithArgs->objargs = list_make4(typeStringToTypeName("text"),
											 typeStringToTypeName(FullBsonTypeName),
											 typeStringToTypeName(FullBsonTypeName),
											 typeStringToTypeName("boolean"));

		FunctionParameter *inDatabaseNameParam = makeNode(FunctionParameter);
		inDatabaseNameParam->name = "p_database_name";
		inDatabaseNameParam->argType = typeStringToTypeName("text");
		inDatabaseNameParam->mode = FUNC_PARAM_IN;

		FunctionParameter *inBsonArgParam = makeNode(FunctionParameter);
		inBsonArgParam->name = "p_arg";
		inBsonArgParam->argType = typeStringToTypeName(FullBsonTypeName);
		inBsonArgParam->mode = FUNC_PARAM_IN;

		FunctionParameter *outBsonResultParam = makeNode(FunctionParameter);
		outBsonResultParam->name = "retval";
		outBsonResultParam->argType = typeStringToTypeName(FullBsonTypeName);
		outBsonResultParam->mode = FUNC_PARAM_INOUT;

		FunctionParameter *outOkResultParam = makeNode(FunctionParameter);
		outOkResultParam->name = "ok";
		outOkResultParam->argType = typeStringToTypeName("boolean");
		outOkResultParam->mode = FUNC_PARAM_INOUT;

		objectWithArgs->objfuncargs = list_make4(inDatabaseNameParam, inBsonArgParam,
												 outBsonResultParam,
												 outOkResultParam);

		bool missingOk = false;
		Cache.CreateIndexesProcedureId =
			LookupFuncWithArgs(OBJECT_PROCEDURE, objectWithArgs, missingOk);
	}

	return Cache.CreateIndexesProcedureId;
}


/*
 * ApiReIndexProcedureId returns the OID of the
 * mongo_api_v1.re_index() procedure.
 */
Oid
ApiReIndexProcedureId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ReindexProcedureId == InvalidOid)
	{
		ObjectWithArgs *objectWithArgs = makeNode(ObjectWithArgs);
		objectWithArgs->objname = list_make2(makeString(ApiSchemaName),
											 makeString("re_index"));

		objectWithArgs->objargs = list_make4(typeStringToTypeName("text"),
											 typeStringToTypeName("text"),
											 typeStringToTypeName(FullBsonTypeName),
											 typeStringToTypeName("boolean"));

		FunctionParameter *inDatabaseNameParam = makeNode(FunctionParameter);
		inDatabaseNameParam->name = "p_database_name";
		inDatabaseNameParam->argType = typeStringToTypeName("text");
		inDatabaseNameParam->mode = FUNC_PARAM_IN;

		FunctionParameter *inBsonArgParam = makeNode(FunctionParameter);
		inBsonArgParam->name = "p_collection_name";
		inBsonArgParam->argType = typeStringToTypeName("text");
		inBsonArgParam->mode = FUNC_PARAM_IN;

		FunctionParameter *outBsonResultParam = makeNode(FunctionParameter);
		outBsonResultParam->name = "retval";
		outBsonResultParam->argType = typeStringToTypeName(FullBsonTypeName);
		outBsonResultParam->mode = FUNC_PARAM_INOUT;

		FunctionParameter *outOkResultParam = makeNode(FunctionParameter);
		outOkResultParam->name = "ok";
		outOkResultParam->argType = typeStringToTypeName("boolean");
		outOkResultParam->mode = FUNC_PARAM_INOUT;

		objectWithArgs->objfuncargs = list_make4(inDatabaseNameParam, inBsonArgParam,
												 outBsonResultParam,
												 outOkResultParam);

		bool missingOk = false;
		Cache.ReindexProcedureId =
			LookupFuncWithArgs(OBJECT_PROCEDURE, objectWithArgs, missingOk);
	}

	return Cache.ReindexProcedureId;
}


/*
 * BsonQueryMatchFunctionId returns the OID of ApiCatalogSchemaName.bson_query_match function.
 */
Oid
BsonQueryMatchFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonQueryMatchFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString("bson_query_match"));
		Oid bsonTypeId = BsonTypeId();
		Oid paramOids[2] = { bsonTypeId, bsonTypeId };
		bool missingOK = false;

		Cache.BsonQueryMatchFunctionId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.BsonQueryMatchFunctionId;
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_eq function.
 */
Oid
BsonEqualMatchRuntimeFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonEqualMatchRuntimeFunctionId,
									   "bson_dollar_eq", BsonTypeId(),
									   GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_eq Runtime operator #=.
 */
Oid
BsonEqualMatchRuntimeOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonEqualMatchRuntimeOperatorId,
							   BsonTypeId(), "#=", GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_eq function for index.
 */
Oid
BsonEqualMatchIndexFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonEqualMatchIndexFunctionId,
									   "bson_dollar_eq", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_gt function.
 */
Oid
BsonGreaterThanMatchRuntimeFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonGreaterThanMatchRuntimeFunctionId,
									   "bson_dollar_gt", BsonTypeId(),
									   GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_gt Runtime operator #>.
 */
Oid
BsonGreaterThanMatchRuntimeOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonGreaterThanMatchRuntimeOperatorId,
							   BsonTypeId(), "#>", GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_gt function for index.
 */
Oid
BsonGreaterThanMatchIndexFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonGreaterThanMatchIndexFunctionId,
									   "bson_dollar_gt", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_gte function.
 */
Oid
BsonGreaterThanEqualMatchRuntimeFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonGreaterThanEqualMatchRuntimeFunctionId,
									   "bson_dollar_gte", BsonTypeId(),
									   GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_gte Runtime operator #>=.
 */
Oid
BsonGreaterThanEqualMatchRuntimeOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonGreaterThanEqualMatchRuntimeOperatorId,
							   BsonTypeId(), "#>=", GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_gte function for index.
 */
Oid
BsonGreaterThanEqualMatchIndexFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonGreaterThanEqualMatchIndexFunctionId,
									   "bson_dollar_gte", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_lt function.
 */
Oid
BsonLessThanMatchRuntimeFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonLessThanMatchRuntimeFunctionId,
									   "bson_dollar_lt", BsonTypeId(),
									   GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_lt Runtime operator #<.
 */
Oid
BsonLessThanMatchRuntimeOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonLessThanMatchRuntimeOperatorId,
							   BsonTypeId(), "#<", GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_lt function.
 */
Oid
BsonLessThanMatchIndexFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonLessThanMatchIndexFunctionId,
									   "bson_dollar_lt", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_lte function.
 */
Oid
BsonLessThanEqualMatchRuntimeFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonLessThanEqualMatchRuntimeFunctionId,
									   "bson_dollar_lte", BsonTypeId(),
									   GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_lte Runtime operator #<=.
 */
Oid
BsonLessThanEqualMatchRuntimeOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonLessThanEqualMatchRuntimeOperatorId,
							   BsonTypeId(), "#<=", GetClusterBsonQueryTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_lte function for index.
 */
Oid
BsonLessThanEqualMatchIndexFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonLessThanEqualMatchIndexFunctionId,
									   "bson_dollar_lte", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_range function.
 */
Oid
BsonRangeMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonRangeMatchFunctionId,
									   "bson_dollar_range", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_range Runtime operator #<>.
 */
Oid
BsonRangeMatchOperatorOid(void)
{
	return GetBinaryOperatorId(&Cache.BsonRangeMatchOperatorOid,
							   BsonTypeId(), "@<>", BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_in function.
 */
Oid
BsonInMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonInMatchFunctionId,
									   "bson_dollar_in", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_nin function.
 */
Oid
BsonNinMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonNinMatchFunctionId,
									   "bson_dollar_nin", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_ne function.
 */
Oid
BsonNotEqualMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonNotEqualMatchFunctionId,
									   "bson_dollar_ne", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_all function.
 */
Oid
BsonAllMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonAllMatchFunctionId,
									   "bson_dollar_all", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_elemmatch function.
 */
Oid
BsonElemMatchMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonElemMatchMatchFunctionId,
									   "bson_dollar_elemmatch", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_bits_all_clear function.
 */
Oid
BsonBitsAllClearFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonBitsAllClearFunctionId,
									   "bson_dollar_bits_all_clear", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_bits_all_clear function.
 */
Oid
BsonBitsAnyClearFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonBitsAnyClearFunctionId,
									   "bson_dollar_bits_any_clear", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_bits_all_set function.
 */
Oid
BsonBitsAllSetFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonBitsAllSetFunctionId,
									   "bson_dollar_bits_all_set", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_bits_any_set function.
 */
Oid
BsonBitsAnySetFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonBitsAnySetFunctionId,
									   "bson_dollar_bits_any_set", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_expr function.
 */
Oid
BsonExprFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonExprFunctionId,
									   "bson_dollar_expr", BsonTypeId(),
									   BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_text function.
 */
Oid
BsonTextFunctionId(void)
{
	return GetBinaryOperatorFunctionIdMissingOk(
		&Cache.BsonTextFunctionId,
		"bson_dollar_text",
		BsonTypeId(), BsonTypeId(),
		"1.6");
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_regex function.
 */
Oid
BsonRegexMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonRegexMatchFunctionId,
									   "bson_dollar_regex", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_mod function.
 */
Oid
BsonModMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonModMatchFunctionId,
									   "bson_dollar_mod", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_size function.
 */
Oid
BsonSizeMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonSizeMatchFunctionId,
									   "bson_dollar_size", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_type function.
 */
Oid
BsonTypeMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonTypeMatchFunctionId,
									   "bson_dollar_type", BsonTypeId(), BsonTypeId());
}


/*
 * Returns the OID of ApiCatalogSchemaName.bson_dollar_exists function.
 */
Oid
BsonExistsMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonExistsMatchFunctionId,
									   "bson_dollar_exists", BsonTypeId(), BsonTypeId());
}


/*
 * BsonEqualMatchOperatorId returns the OID of the <bson> @= <bson> operator.
 */
Oid
BsonEqualMatchOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonEqualMatchOperatorId,
							   BsonTypeId(), "@=", BsonTypeId());
}


/*
 * BsonEqualOperatorId returns the OID of the <bson> = <bson> operator.
 */
Oid
BsonEqualOperatorId(void)
{
	return GetCoreBinaryOperatorId(&Cache.BsonEqualOperatorId,
								   BsonTypeId(), "=", BsonTypeId());
}


/*
 * BsonQueryOperatorId returns the OID of the <bson> @@ <bson> operator.
 */
Oid
BsonQueryOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonQueryOperatorId,
							   BsonTypeId(), "@@", BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $eq <bson> function.
 */
Oid
BsonValueEqualMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueEqualMatchFunctionId,
									   "bson_value_dollar_eq", INTERNALOID, BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $gt <bson> function.
 */
Oid
BsonValueGreaterThanMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueGreaterMatchFunctionId,
									   "bson_value_dollar_gt", INTERNALOID, BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $gte <bson> function.
 */
Oid
BsonValueGreaterThanEqualMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueGreaterEqualMatchFunctionId,
									   "bson_value_dollar_gte", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $lt <bson> function.
 */
Oid
BsonValueLessThanMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueLessMatchFunctionId,
									   "bson_value_dollar_lt", INTERNALOID, BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $lte <bson> function.
 */
Oid
BsonValueLessThanEqualMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueLessEqualMatchFunctionId,
									   "bson_value_dollar_lte", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $size <bson> function.
 */
Oid
BsonValueSizeMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueSizeMatchFunctionId,
									   "bson_value_dollar_size", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $type <bson> function.
 */
Oid
BsonValueTypeMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueTypeMatchFunctionId,
									   "bson_value_dollar_type", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $in <bson> function.
 */
Oid
BsonValueInMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueInMatchFunctionId,
									   "bson_value_dollar_in", INTERNALOID, BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $nin <bson> function.
 */
Oid
BsonValueNinMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueNinMatchFunctionId,
									   "bson_value_dollar_nin", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $ne <bson> function.
 */
Oid
BsonValueNotEqualMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueNotEqualMatchFunctionId,
									   "bson_value_dollar_ne", INTERNALOID, BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $exists <bson> function.
 */
Oid
BsonValueExistsMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueExistsMatchFunctionId,
									   "bson_value_dollar_exists", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $elemMatch <bson> function.
 */
Oid
BsonValueElemMatchMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueElemMatchMatchFunctionId,
									   "bson_value_dollar_elemmatch", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $all <bson> function.
 */
Oid
BsonValueAllMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueAllMatchFunctionId,
									   "bson_value_dollar_all", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $regex <bson> function.
 */
Oid
BsonValueRegexMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueRegexMatchFunctionId,
									   "bson_value_dollar_regex", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $mod <bson> function.
 */
Oid
BsonValueModMatchFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueModMatchFunctionId,
									   "bson_value_dollar_mod", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $bitsAllClear <bson> function.
 */
Oid
BsonValueBitsAllClearFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueBitsAllClearFunctionId,
									   "bson_value_dollar_bits_all_clear", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $bitsAnyClear <bson> function.
 */
Oid
BsonValueBitsAnyClearFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueBitsAnyClearFunctionId,
									   "bson_value_dollar_bits_any_clear", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $bitsAllClear <bson> function.
 */
Oid
BsonValueBitsAllSetFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueBitsAllSetFunctionId,
									   "bson_value_dollar_bits_all_set", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the <bson_value_t> $bitsAnyClear <bson> function.
 */
Oid
BsonValueBitsAnySetFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonValueBitsAnySetFunctionId,
									   "bson_value_dollar_bits_any_set", INTERNALOID,
									   BsonTypeId());
}


/*
 * Returns the OID of the "drandom" internal postgres method
 */
Oid
PostgresDrandomFunctionId(void)
{
	return GetPostgresInternalFunctionId(&Cache.PostgresDrandomFunctionId, "drandom");
}


/*
 * Returns the OID of the "float8_timestamptz" internal postgres method
 */
Oid
PostgresToTimestamptzFunctionId(void)
{
	return GetPostgresInternalFunctionId(&Cache.PostgresToTimestamptzFunctionId,
										 "float8_timestamptz");
}


/*
 * Returns the OID of the "date_part" internal postgres method
 */
Oid
PostgresDatePartFunctionId(void)
{
	return GetPostgresInternalFunctionId(&Cache.PostgresDatePartFunctionId,
										 "timestamp_part");
}


/*
 * Returns the OID of the "timestamptz_zone" internal postgres method
 */
Oid
PostgresTimestampToZoneFunctionId(void)
{
	return GetPostgresInternalFunctionId(&Cache.PostgresTimestampToZoneFunctionId,
										 "timestamptz_zone");
}


/* Returns the OID of Rum Index Access method.
 */
Oid
RumIndexAmId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.RumIndexAmId == InvalidOid)
	{
		const char *extensionRumAccess = psprintf("%s_rum", ExtensionObjectPrefix);
		HeapTuple tuple = SearchSysCache1(AMNAME, CStringGetDatum(extensionRumAccess));
		if (!HeapTupleIsValid(tuple))
		{
			ereport(ERROR,
					(errmsg("Access method \"%s\" not supported.", extensionRumAccess)));
		}
		Form_pg_am accessMethodForm = (Form_pg_am) GETSTRUCT(tuple);
		Cache.RumIndexAmId = accessMethodForm->oid;
		ReleaseSysCache(tuple);
	}

	return Cache.RumIndexAmId;
}


/* Returns the OID of vector ivfflat Index Access method.
 */
Oid
PgVectorIvfFlatIndexAmId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.PgVectorIvfFlatIndexAmId == InvalidOid)
	{
		HeapTuple tuple = SearchSysCache1(AMNAME, CStringGetDatum("ivfflat"));
		if (!HeapTupleIsValid(tuple))
		{
			ereport(NOTICE,
					(errmsg(
						 "Access method \"ivfflat\" not supported.")));
		}
		Form_pg_am accessMethodForm = (Form_pg_am) GETSTRUCT(tuple);
		Cache.PgVectorIvfFlatIndexAmId = accessMethodForm->oid;
		ReleaseSysCache(tuple);
	}

	return Cache.PgVectorIvfFlatIndexAmId;
}


/* Returns the OID of vector hnsw Index Access method.
 */
Oid
PgVectorHNSWIndexAmId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.PgVectorHNSWIndexAmId == InvalidOid)
	{
		HeapTuple tuple = SearchSysCache1(AMNAME, CStringGetDatum("hnsw"));
		if (!HeapTupleIsValid(tuple))
		{
			ereport(NOTICE,
					(errmsg(
						 "Access method \"hnsw\" not supported.")));
		}
		Form_pg_am accessMethodForm = (Form_pg_am) GETSTRUCT(tuple);
		Cache.PgVectorHNSWIndexAmId = accessMethodForm->oid;
		ReleaseSysCache(tuple);
	}

	return Cache.PgVectorHNSWIndexAmId;
}


/*
 * Returns the function Oid for converting a double[] to a vector
 * specifically the array_to_vector function.
 */
Oid
PgDoubleToVectorFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.PgDoubleToVectorFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("public"),
											makeString("array_to_vector"));

		Oid paramOids[3] = { FLOAT8ARRAYOID, INT4OID, BOOLOID };
		bool missingOK = false;
		Cache.PgDoubleToVectorFunctionOid =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.PgDoubleToVectorFunctionOid;
}


/*
 * VectorAsVectorFunctionOid returns the OID of the vector as vector cast function.
 */
Oid
VectorAsVectorFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorAsVectorFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("public"),
											makeString("vector"));

		Oid paramOids[3] = { VectorTypeId(), INT4OID, BOOLOID };
		bool missingOK = false;
		Cache.VectorAsVectorFunctionOid =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.VectorAsVectorFunctionOid;
}


/*
 * BsonTrueFunctionId returns the OID of the bson_true_match function.
 */
Oid
BsonTrueFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonTrueFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString("bson_true_match"));
		Oid paramOids[1] = { BsonTypeId() };
		bool missingOK = false;

		Cache.BsonTrueFunctionId =
			LookupFuncName(functionNameList, 1, paramOids, missingOK);
	}

	return Cache.BsonTrueFunctionId;
}


/*
 * Returns the OID of the mongo_api_v1.cursor_state function.
 */
Oid
ApiCursorStateFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CursorStateFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("cursor_state"));
		Oid paramOids[2] = { BsonTypeId(), BsonTypeId() };
		bool missingOK = false;

		Cache.CursorStateFunctionId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.CursorStateFunctionId;
}


/*
 * Returns the OID of the mongo_api_v1.current_cursor_state function.
 */
Oid
ApiCurrentCursorStateFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CurrentCursorStateFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("current_cursor_state"));
		Oid paramOids[1] = { BsonTypeId() };
		bool missingOK = false;

		Cache.CurrentCursorStateFunctionId =
			LookupFuncName(functionNameList, 1, paramOids, missingOK);
	}

	return Cache.CurrentCursorStateFunctionId;
}


/*
 * BsonEmptyDataTableFunctionId returns the OID of the empty_data_table function.
 */
Oid
BsonEmptyDataTableFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonEmptyDataTableFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("empty_data_table"));
		Oid paramOids[0] = { };
		bool missingOK = false;

		Cache.BsonEmptyDataTableFunctionId =
			LookupFuncName(functionNameList, 0, paramOids, missingOK);
	}

	return Cache.BsonEmptyDataTableFunctionId;
}


/*
 * ApiCollStatsAggregationFunctionOid returns the OID of the coll_stats_aggregation function.
 */
Oid
ApiCollStatsAggregationFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CollStatsAggregationFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("coll_stats_aggregation"));
		Oid paramOids[3] = { TEXTOID, TEXTOID, BsonTypeId() };
		bool missingOK = false;

		Cache.CollStatsAggregationFunctionOid =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.CollStatsAggregationFunctionOid;
}


/*
 * ApiIndexStatsAggregationFunctionOid returns the OID of the index_stats_aggregation function.
 */
Oid
ApiIndexStatsAggregationFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.IndexStatsAggregationFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("index_stats_aggregation"));
		Oid paramOids[2] = { TEXTOID, TEXTOID };
		bool missingOK = false;

		Cache.IndexStatsAggregationFunctionOid =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.IndexStatsAggregationFunctionOid;
}


Oid
BsonCurrentOpAggregationFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonCurrentOpAggregationFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("current_op_aggregation"));
		Oid paramOids[1] = { BsonTypeId() };
		bool missingOK = false;

		Cache.BsonCurrentOpAggregationFunctionId =
			LookupFuncName(functionNameList, 1, paramOids, missingOK);
	}

	return Cache.BsonCurrentOpAggregationFunctionId;
}


/*
 * IndexSpecAsBsonFunctionId returns the OID of the ApiInternalSchemaName.index_spec_as_bson function.
 */
Oid
IndexSpecAsBsonFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.IndexSpecAsBsonFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiInternalSchemaName),
											makeString("index_spec_as_bson"));
		Oid paramOids[3] = { IndexSpecTypeId(), BOOLOID, TEXTOID };
		bool missingOK = false;

		Cache.IndexSpecAsBsonFunctionId =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.IndexSpecAsBsonFunctionId;
}


/*
 * ApiSampleCollectionFunctionId returns the OID of the sample_collection function.
 */
Oid
ApiSampleCollectionFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiSampleCollectionFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiSchemaName),
											makeString("sample_collection"));
		Oid paramOids[3] = { TEXTOID, TEXTOID, INT8OID };
		bool missingOK = false;

		Cache.ApiSampleCollectionFunctionId =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.ApiSampleCollectionFunctionId;
}


/*
 * ExtensionTableSampleSystemRowsFunctionId returns the OID of the tsm system_rows function.
 */
Oid
ExtensionTableSampleSystemRowsFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ExtensionTableSampleSystemRowsFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("public"),
											makeString("system_rows"));
		Oid paramOids[1] = { INTERNALOID };
		bool missingOK = false;

		Cache.ExtensionTableSampleSystemRowsFunctionId =
			LookupFuncName(functionNameList, 1, paramOids, missingOK);
	}

	return Cache.ExtensionTableSampleSystemRowsFunctionId;
}


Oid
ApiCatalogAggregationPipelineFunctionId(void)
{
	return GetBinaryOperatorFunctionIdMissingOk(
		&Cache.ApiCatalogAggregationPipelineFunctionId,
		"bson_aggregation_pipeline",
		TEXTOID, BsonTypeId(),
		"1.7");
}


Oid
ApiCatalogAggregationFindFunctionId(void)
{
	return GetBinaryOperatorFunctionIdMissingOk(
		&Cache.ApiCatalogAggregationFindFunctionId,
		"bson_aggregation_find",
		TEXTOID, BsonTypeId(),
		"1.7");
}


Oid
ApiCatalogAggregationCountFunctionId(void)
{
	return GetBinaryOperatorFunctionIdMissingOk(
		&Cache.ApiCatalogAggregationCountFunctionId,
		"bson_aggregation_count",
		TEXTOID, BsonTypeId(),
		"1.7");
}


Oid
ApiCatalogAggregationDistinctFunctionId(void)
{
	return GetBinaryOperatorFunctionIdMissingOk(
		&Cache.ApiCatalogAggregationDistinctFunctionId,
		"bson_aggregation_distinct",
		TEXTOID, BsonTypeId(),
		"1.7");
}


Oid
BsonDollarAddFieldsFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.ApiCatalogBsonDollarAddFieldsFunctionOid,
									   "bson_dollar_add_fields", BsonTypeId(),
									   BsonTypeId());
}


Oid
BsonDollarProjectFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.ApiCatalogBsonDollarProjectFunctionOid,
									   "bson_dollar_project", BsonTypeId(), BsonTypeId());
}


Oid
BsonDollarProjectFindFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiCatalogBsonDollarProjectFindFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString(
												"bson_dollar_project_find"));
		Oid paramOids[3] = { BsonTypeId(), BsonTypeId(), BsonTypeId() };
		bool missingOK = false;

		Cache.ApiCatalogBsonDollarProjectFindFunctionOid =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.ApiCatalogBsonDollarProjectFindFunctionOid;
}


Oid
BsonDollarUnwindFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.ApiCatalogBsonDollarUnwindFunctionOid,
									   "bson_dollar_unwind", BsonTypeId(), TEXTOID);
}


Oid
BsonDollarUnwindWithOptionsFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(
		&Cache.ApiCatalogBsonDollarUnwindWithOptionsFunctionOid,
		"bson_dollar_unwind", BsonTypeId(), BsonTypeId());
}


Oid
BsonDollarReplaceRootFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.ApiCatalogBsonDollarReplaceRootFunctionOid,
									   "bson_dollar_replace_root", BsonTypeId(),
									   BsonTypeId());
}


static Oid
GetAggregateFunctionByName(Oid *function, char *namespaceName, char *name)
{
	InitializeHelioApiExtensionCache();

	if (*function == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(namespaceName),
											makeString(name));
		bool missingOK = false;
		ObjectWithArgs args = { 0 };
		args.args_unspecified = true;
		args.objname = functionNameList;

		*function = LookupFuncWithArgs(OBJECT_AGGREGATE, &args, missingOK);
	}

	return *function;
}


Oid
BsonSumAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonSumAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonsum");
}


Oid
BsonAvgAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonAverageAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonaverage");
}


Oid
BsonArrayAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonArrayAggregateFunctionOid,
									  ApiCatalogSchemaName, "bson_array_agg");
}


Oid
BsonDistinctAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonDistinctAggregateFunctionOid,
									  ApiCatalogSchemaName, "bson_distinct_agg");
}


Oid
BsonObjectAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonObjectAggregateFunctionOid,
									  ApiCatalogSchemaName, "bson_object_agg");
}


Oid
BsonMaxAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonMaxAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonmax");
}


Oid
BsonMinAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonMinAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonmin");
}


Oid
BsonFirstOnSortedAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(
		&Cache.ApiCatalogBsonFirstOnSortedAggregateFunctionOid,
		ApiCatalogSchemaName, "bsonfirstonsorted");
}


Oid
BsonFirstAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonFirstAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonfirst");
}


Oid
BsonLastAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonLastAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonlast");
}


Oid
BsonLastOnSortedAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(
		&Cache.ApiCatalogBsonLastOnSortedAggregateFunctionOid,
		ApiCatalogSchemaName, "bsonlastonsorted");
}


Oid
BsonFirstNAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonFirstNAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonfirstn");
}


Oid
BsonFirstNOnSortedAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(
		&Cache.ApiCatalogBsonFirstNOnSortedAggregateFunctionOid,
		ApiCatalogSchemaName, "bsonfirstnonsorted");
}


Oid
BsonLastNAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(&Cache.ApiCatalogBsonLastNAggregateFunctionOid,
									  ApiCatalogSchemaName, "bsonlastn");
}


Oid
BsonLastNOnSortedAggregateFunctionOid(void)
{
	return GetAggregateFunctionByName(
		&Cache.ApiCatalogBsonLastNOnSortedAggregateFunctionOid,
		ApiCatalogSchemaName, "bsonlastnonsorted");
}


Oid
PgRandomFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.PgRandomFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("random"));
		Oid *paramOids = NULL;
		bool missingOK = false;

		Cache.PgRandomFunctionOid =
			LookupFuncName(functionNameList, 0, paramOids, missingOK);
	}

	return Cache.PgRandomFunctionOid;
}


Oid
BsonLookupExtractFilterExpressionFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(
		&Cache.ApiCatalogBsonLookupExtractFilterExpressionOid,
		"bson_dollar_lookup_extract_filter_expression",
		BsonTypeId(), BsonTypeId());
}


Oid
BsonLookupUnwindFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonLookupUnwindFunctionOid,
									   "bson_lookup_unwind",
									   BsonTypeId(), TEXTOID);
}


Oid
BsonDistinctUnwindFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonDistinctUnwindFunctionOid,
									   "bson_distinct_unwind",
									   BsonTypeId(), TEXTOID);
}


Oid
BsonRepathAndBuildFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiCatalogBsonRepathAndBuildFunctionOid == InvalidOid)
	{
		/* Given it's a variadic function, we just look it up by name */
		List *functionNameList = list_make2(makeString(CoreSchemaName),
											makeString("bson_repath_and_build"));
		bool missingOK = false;
		ObjectWithArgs args = { 0 };
		args.args_unspecified = true;
		args.objname = functionNameList;

		Cache.ApiCatalogBsonRepathAndBuildFunctionOid =
			LookupFuncWithArgs(OBJECT_FUNCTION, &args, missingOK);
	}

	return Cache.ApiCatalogBsonRepathAndBuildFunctionOid;
}


Oid
RowGetBsonFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiCatalogRowGetBsonFunctionOid == InvalidOid)
	{
		/* Given it's a variadic function, we just look it up by name */
		List *functionNameList = list_make2(makeString(CoreSchemaName),
											makeString("row_get_bson"));
		bool missingOK = false;
		ObjectWithArgs args = { 0 };
		args.args_unspecified = true;
		args.objname = functionNameList;

		Cache.ApiCatalogRowGetBsonFunctionOid =
			LookupFuncWithArgs(OBJECT_FUNCTION, &args, missingOK);
	}

	return Cache.ApiCatalogRowGetBsonFunctionOid;
}


Oid
BsonExpressionGetFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiCatalogBsonExpressionGetFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString("bson_expression_get"));
		Oid paramOids[3] = { BsonTypeId(), BsonTypeId(), BOOLOID };
		bool missingOK = false;

		Cache.ApiCatalogBsonExpressionGetFunctionOid =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.ApiCatalogBsonExpressionGetFunctionOid;
}


Oid
BsonExpressionMapFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiCatalogBsonExpressionMapFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString("bson_expression_map"));
		Oid paramOids[4] = { BsonTypeId(), TEXTOID, BsonTypeId(), BOOLOID };
		bool missingOK = false;

		Cache.ApiCatalogBsonExpressionMapFunctionOid =
			LookupFuncName(functionNameList, 4, paramOids, missingOK);
	}

	return Cache.ApiCatalogBsonExpressionMapFunctionOid;
}


/*
 * BsonArrowOperatorId returns the OID of the <bson> -> <text> operator.
 */
Oid
BsonArrowOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonQueryOperatorId,
							   BsonTypeId(), "->", TEXTOID);
}


/*
 * GeometryTypeId returns the OID of the postgis_public.geometry type.
 */
Oid
GeometryTypeId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.GeometryTypeId == InvalidOid)
	{
		List *geometryTypeNameList = list_make2(makeString(POSTGIS_EXTENSION_SCHEMA),
												makeString(
													"geometry"));
		TypeName *geometryTypeName = makeTypeNameFromNameList(geometryTypeNameList);
		Cache.GeometryTypeId = typenameTypeId(NULL, geometryTypeName);
	}

	return Cache.GeometryTypeId;
}


/*
 * Box2df returns postgis box2df type id
 */
Oid
Box2dfTypeId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.Box2dfTypeId == InvalidOid)
	{
		List *typeNameList = list_make2(makeString(POSTGIS_EXTENSION_SCHEMA),
										makeString("box2df"));
		TypeName *typeName = makeTypeNameFromNameList(typeNameList);
		Cache.Box2dfTypeId = typenameTypeId(NULL, typeName);
	}

	return Cache.Box2dfTypeId;
}


/*
 * GeographyTypeId returns the OID of the postgis_public.geography type.
 */
Oid
GeographyTypeId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.GeographyTypeId == InvalidOid)
	{
		List *geographyTypeNameList = list_make2(makeString(POSTGIS_EXTENSION_SCHEMA),
												 makeString(
													 "geography"));
		TypeName *geographyTypeName = makeTypeNameFromNameList(geographyTypeNameList);
		Cache.GeographyTypeId = typenameTypeId(NULL, geographyTypeName);
	}

	return Cache.GeographyTypeId;
}


/*
 * GIDXTypeId returns postgis gidx type id
 */
Oid
GIDXTypeId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.GIDXTypeId == InvalidOid)
	{
		List *typeNameList = list_make2(makeString(POSTGIS_EXTENSION_SCHEMA),
										makeString("gidx"));
		TypeName *typeName = makeTypeNameFromNameList(typeNameList);
		Cache.GIDXTypeId = typenameTypeId(NULL, typeName);
	}

	return Cache.GIDXTypeId;
}


/*
 * GeometryArrayTypeId returns the array type id of postgis_public.geometry type
 */
Oid
GeometryArrayTypeId(void)
{
	return GetArrayTypeOid(&Cache.GeometryArrayTypeId, GeometryTypeId());
}


/*
 * VectorTypeId returns the OID of the vector type.
 */
Oid
VectorTypeId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorTypeId == InvalidOid)
	{
		List *vectorTypeNameList = list_make2(makeString("public"), makeString("vector"));
		TypeName *vectorTypeName = makeTypeNameFromNameList(vectorTypeNameList);
		Cache.VectorTypeId = typenameTypeId(NULL, vectorTypeName);
	}

	return Cache.VectorTypeId;
}


/*
 * IndexSpecTypeId returns the OID of the ApiCatalogSchemaName.index_spec_type.
 */
Oid
IndexSpecTypeId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.IndexSpecTypeId == InvalidOid)
	{
		List *typeNameList = list_make2(makeString(ApiCatalogSchemaName),
										makeString("index_spec_type"));
		TypeName *typeName = makeTypeNameFromNameList(typeNameList);
		Cache.IndexSpecTypeId = typenameTypeId(NULL, typeName);
	}

	return Cache.IndexSpecTypeId;
}


Oid
MongoCatalogCollectionsTypeOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.MongoCatalogCollectionsTypeOid == InvalidOid)
	{
		List *typeNameList = list_make2(makeString(ApiCatalogSchemaName),
										makeString("collections"));
		TypeName *typeName = makeTypeNameFromNameList(typeNameList);
		Cache.MongoCatalogCollectionsTypeOid = typenameTypeId(NULL, typeName);
	}

	return Cache.MongoCatalogCollectionsTypeOid;
}


/*
 * BsonOrderByQueryOperatorId returns the OID of the <bson> |-<> <bson> operator.
 */
Oid
BsonOrderByQueryOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.BsonOrderByQueryOperatorId,
							   BsonTypeId(), "|-<>", BsonTypeId());
}


/*
 * BsonOrderByFunctionId returns the OID of the bson_orderby(<bson>, <bson>) function.
 */
Oid
BsonOrderByFunctionOid(void)
{
	return GetBinaryOperatorFunctionId(&Cache.BsonOrderByFunctionId,
									   "bson_orderby", BsonTypeId(), BsonTypeId());
}


Oid
ApiCatalogBsonExtractVectorFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.ApiCatalogBsonExtractVectorFunctionId,
									   "bson_extract_vector", BsonTypeId(), TEXTOID);
}


/*
 * Returns the OID of the ApiSchemaName.bson_search_param function.
 */
Oid
ApiBsonSearchParamFunctionId(void)
{
	return GetBinaryOperatorFunctionId(&Cache.ApiBsonSearchParamFunctionId,
									   "bson_search_param", BsonTypeId(), BsonTypeId());
}


/*
 * BsonDollarGeowithinFunctionOid returns the OID of ApiCatalogSchemaName.bson_dollar_geowithin
 */
Oid
BsonDollarGeowithinFunctionOid(void)
{
	int nargs = 2;
	Oid argTypes[2] = { BsonTypeId(), BsonTypeId() };
	bool missingOk = true;
	return GetSchemaFunctionIdWithNargs(
		&Cache.BsonDollarGeowithinFunctionOid,
		ApiCatalogSchemaName, "bson_dollar_geowithin", nargs,
		argTypes, missingOk);
}


Oid
BsonDollarGeoIntersectsFunctionOid(void)
{
	int nargs = 2;
	Oid argTypes[2] = { BsonTypeId(), BsonTypeId() };
	bool missingOk = true;
	return GetSchemaFunctionIdWithNargs(
		&Cache.BsonDollarGeoIntersectsFunctionOid,
		ApiCatalogSchemaName, "bson_dollar_geointersects", nargs,
		argTypes, missingOk);
}


/*
 * BsonValidateGeometryFunctionId returns the OID of the ApiCatalogSchemaName.bson_validate_geometry
 */
Oid
BsonValidateGeometryFunctionId(void)
{
	int nargs = 2;
	Oid argTypes[2] = { BsonTypeId(), TEXTOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.BsonValidateGeometryFunctionId,
		ApiCatalogSchemaName, "bson_validate_geometry", nargs,
		argTypes, missingOk);
}


/*
 * BsonValidateGeographyFunctionId returns the OID of the ApiCatalogSchemaName.bson_validate_geography
 */
Oid
BsonValidateGeographyFunctionId(void)
{
	int nargs = 2;
	Oid argTypes[2] = { BsonTypeId(), TEXTOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.BsonValidateGeographyFunctionId,
		ApiCatalogSchemaName, "bson_validate_geography", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryGistCompress2dFunctionId returns OID of postgis_public.geometry_gist_compress_2d
 */
Oid
PostgisGeometryGistCompress2dFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { INTERNALOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryGistCompress2dFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "geometry_gist_compress_2d", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeographyGistCompressFunctionId returns OID of postgis_public.geography_gist_compress
 */
Oid
PostgisGeographyGistCompressFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { INTERNALOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeographyGistCompressFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "geography_gist_compress", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryGistConsistent2dFunctionId returns OID of postgis_public.geometry_gist_consistent_2d
 */
Oid
PostgisGeometryGistConsistent2dFunctionId(void)
{
	int nargs = 3;
	Oid argTypes[3] = { INTERNALOID, GeometryTypeId(), INT4OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryGistConsistent2dFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "geometry_gist_consistent_2d", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeographyGistConsistentFunctionId returns OID of postgis_public.geography_gist_consistent
 */
Oid
PostgisGeographyGistConsistentFunctionId(void)
{
	int nargs = 3;
	Oid argTypes[3] = { INTERNALOID, GeographyTypeId(), INT4OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeographyGistConsistentFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "geography_gist_consistent", nargs,
		argTypes, missingOk);
}


/*
 * PostgisMakeEnvelopeFunctionId returns the OID of the postgis_public.st_makeenvelope function.
 */
Oid
PostgisMakeEnvelopeFunctionId(void)
{
	int nargs = 5;
	Oid argTypes[5] = { FLOAT8OID, FLOAT8OID, FLOAT8OID, FLOAT8OID, INT4OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisMakeEnvelopeFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_makeenvelope", nargs,
		argTypes, missingOk);
}


/*
 * PostgisMakePointFunctionId returns the OID of the postgis_public.st_makepoint function.
 */
Oid
PostgisMakePointFunctionId(void)
{
	int nargs = 2;
	Oid argTypes[2] = { FLOAT8OID, FLOAT8OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisMakePointFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_makepoint", nargs,
		argTypes, missingOk);
}


/*
 * PostgisBufferFunctionId returns the OID of the postgis_public.st_buffer function.
 */
Oid
PostgisBufferFunctionId(void)
{
	int nargs = 3;
	Oid argTypes[3] = { GeometryTypeId(), FLOAT8OID, TEXTOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisBufferFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_buffer", nargs,
		argTypes, missingOk);
}


/*
 * PostgisMakeLineFunctionId returns the OID of the postgis_public.st_makepolygon function.
 */
Oid
PostgisMakePolygonFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { GeometryTypeId() };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisMakePolygonFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_makepolygon", nargs,
		argTypes, missingOk);
}


/*
 * PostgisMakeLineFunctionId returns the OID of the postgis_public.st_makeline function.
 */
Oid
PostgisMakeLineFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { GeometryArrayTypeId() };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisMakeLineFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_makeline", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeographyFromWKBFunctionId returns the OID of the postgis_public.st_geogfromwkb function.
 * which converts the WKB to a geography
 */
Oid
PostgisGeographyFromWKBFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { BYTEAOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeographyFromWKBFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_geogfromwkb", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeographyCoversFunctionId returns the OID of postgis_public.st_covers function.
 * Note this variant is only used for geographies
 */
Oid
PostgisGeographyCoversFunctionId(void)
{
	int nargs = 2;
	Oid geographyTypeId = GeographyTypeId();
	Oid argTypes[2] = { geographyTypeId, geographyTypeId };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeographyCoversFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_covers", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGIDXGeographyOverlapsFunctionId returns the OID of postgis_public.overlaps_geog function.
 * which check gidx overlap between (gidx, geography)
 */
Oid
PostgisGIDXGeographyOverlapsFunctionId(void)
{
	int nargs = 2;
	Oid geographyTypeId = GeographyTypeId();
	Oid argTypes[2] = { GIDXTypeId(), geographyTypeId };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGIDXGeographyOverlapsFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "overlaps_geog", nargs,
		argTypes, missingOk);
}


/*
 * PostgisBox2dfGeometryOverlapsFunctionId returns the OID of postgis_public.overlaps_2d function.
 * which check box2df overlap between (box2df, geometry)
 */
Oid
PostgisBox2dfGeometryOverlapsFunctionId(void)
{
	int nargs = 2;
	Oid argTypes[2] = { Box2dfTypeId(), GeometryTypeId() };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisBox2dfGeometryOverlapsFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "overlaps_2d", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryCoversFunctionId returns the OID of postgis_public.st_covers function.
 * Note this variant is only used for geometries
 */
Oid
PostgisGeometryCoversFunctionId(void)
{
	int nargs = 2;
	Oid geometryOid = GeometryTypeId();
	Oid argTypes[2] = { geometryOid, geometryOid };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryCoversFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_covers", nargs,
		argTypes, missingOk);
}


/*
 * PostgisIntersectsFunctionId returns the OID of postgis_public.st_intersects function.
 * Note this variant is only used for geographies
 */
Oid
PostgisGeographyIntersectsFunctionId(void)
{
	int nargs = 2;
	Oid geographyOid = GeographyTypeId();
	Oid argTypes[2] = { geographyOid, geographyOid };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeographyIntersectsFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_intersects", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryAreaFunctionId returns the OID of the postgis_public.st_area function.
 */
Oid
PostgisGeometryAreaFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { GeometryTypeId() };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryAreaFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_area", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryFromEWKBFunctionId returns the OID of the postgis_public.st_geomfromewkb function.
 * which converts the EWKB to a geometry
 */
Oid
PostgisGeometryFromEWKBFunctionId(void)
{
	int nargs = 1;
	Oid argTypes[1] = { BYTEAOID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryFromEWKBFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_geomfromewkb", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryAsGeography returns the OID of the geometry::geography Cast function
 * postgis_public.geography(geometry).
 */
Oid
PostgisGeometryAsGeography(void)
{
	int nargs = 1;
	Oid argTypes[1] = { GeometryTypeId() };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryAsGeography,
		POSTGIS_EXTENSION_SCHEMA, "geography", nargs,
		argTypes, missingOk);
}


/*
 * PostgisGeometryIsValidDetailFunctionId returns the OID of the postgis_public.st_isvaliddetail function.
 */
Oid
PostgisGeometryIsValidDetailFunctionId(void)
{
	int nargs = 2;
	Oid argTypes[2] = { GeometryTypeId(), INT4OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisGeometryIsValidDetailFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_isvaliddetail", nargs,
		argTypes, missingOk);
}


/*
 * PostgisSetSRIDFunctionId returns the OID of the postgis_public.st_setsrid function.
 */
Oid
PostgisSetSRIDFunctionId(void)
{
	int nargs = 2;
	Oid geometryOid = GeometryTypeId();
	Oid argTypes[2] = { geometryOid, INT4OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(
		&Cache.PostgisSetSRIDFunctionId,
		POSTGIS_EXTENSION_SCHEMA, "st_setsrid", nargs,
		argTypes, missingOk);
}


/*
 * VectorOrderByQueryOperatorId returns the OID of the <bson> |-<> <bson> operator.
 */
Oid
VectorOrderByQueryOperatorId(void)
{
	return GetBinaryOperatorId(&Cache.VectorOrderByQueryOperatorId,
							   BsonTypeId(), "|=<>|", BsonTypeId());
}


/*
 * VectorCosineSimilaritySearchOperatorId returns the OID of the <vector> <=> <vector> operator.
 */
Oid
VectorCosineSimilaritySearchOperatorId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorCosineSimilaritySearchOperatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString("public"), makeString("<=>"));

		Cache.VectorCosineSimilaritySearchOperatorId =
			OpernameGetOprid(operatorNameList, VectorTypeId(), VectorTypeId());
	}

	return Cache.VectorCosineSimilaritySearchOperatorId;
}


/*
 * VectorL2SimilaritySearchOperatorId returns the OID of the <vector> <-> <vector> operator.
 */
Oid
VectorL2SimilaritySearchOperatorId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorL2SimilaritySearchOperatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString("public"), makeString("<->"));

		Cache.VectorL2SimilaritySearchOperatorId =
			OpernameGetOprid(operatorNameList, VectorTypeId(), VectorTypeId());
	}

	return Cache.VectorL2SimilaritySearchOperatorId;
}


/*
 * VectorIPSimilaritySearchOperatorId returns the OID of the <vector> <#> <vector> operator.
 */
Oid
VectorIPSimilaritySearchOperatorId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorIPSimilaritySearchOperatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString("public"), makeString("<#>"));

		Cache.VectorIPSimilaritySearchOperatorId =
			OpernameGetOprid(operatorNameList, VectorTypeId(), VectorTypeId());
	}

	return Cache.VectorIPSimilaritySearchOperatorId;
}


/*
 * VectorIVFFlatCosineSimilarityOperatorFamilyId returns
 * the OID of the vector_cosine_ops operator class for access method ivfflat.
 */
Oid
VectorIVFFlatCosineSimilarityOperatorFamilyId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorIVFFlatCosineSimilarityOperatorFamilyId == InvalidOid)
	{
		bool missingOk = false;
		Cache.VectorIVFFlatCosineSimilarityOperatorFamilyId = get_opfamily_oid(
			PgVectorIvfFlatIndexAmId(), list_make2(makeString("public"), makeString(
													   "vector_cosine_ops")),
			missingOk);
	}

	return Cache.VectorIVFFlatCosineSimilarityOperatorFamilyId;
}


/*
 * VectorHNSWCosineSimilarityOperatorFamilyId returns
 * the OID of the vector_cosine_ops operator class for access method hnsw.
 */
Oid
VectorHNSWCosineSimilarityOperatorFamilyId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorHNSWCosineSimilarityOperatorFamilyId == InvalidOid)
	{
		bool missingOk = false;
		Cache.VectorHNSWCosineSimilarityOperatorFamilyId = get_opfamily_oid(
			PgVectorHNSWIndexAmId(), list_make2(makeString("public"), makeString(
													"vector_cosine_ops")),
			missingOk);
	}

	return Cache.VectorHNSWCosineSimilarityOperatorFamilyId;
}


/*
 * VectorIVFFlatL2SimilarityOperatorFamilyId returns
 * the OID of the vector_l2_ops operator class for access method ivfflat.
 */
Oid
VectorIVFFlatL2SimilarityOperatorFamilyId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorIVFFlatL2SimilarityOperatorFamilyId == InvalidOid)
	{
		bool missingOk = false;
		Cache.VectorIVFFlatL2SimilarityOperatorFamilyId = get_opfamily_oid(
			PgVectorIvfFlatIndexAmId(), list_make2(makeString("public"), makeString(
													   "vector_l2_ops")),
			missingOk);
	}

	return Cache.VectorIVFFlatL2SimilarityOperatorFamilyId;
}


/*
 * VectorHNSWL2SimilarityOperatorFamilyId returns
 * the OID of the vector_l2_ops operator class for access method hnsw.
 */
Oid
VectorHNSWL2SimilarityOperatorFamilyId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorHNSWL2SimilarityOperatorFamilyId == InvalidOid)
	{
		bool missingOk = false;
		Cache.VectorHNSWL2SimilarityOperatorFamilyId = get_opfamily_oid(
			PgVectorHNSWIndexAmId(), list_make2(makeString("public"), makeString(
													"vector_l2_ops")),
			missingOk);
	}

	return Cache.VectorHNSWL2SimilarityOperatorFamilyId;
}


/*
 * VectorIVFFlatIPSimilarityOperatorFamilyId returns
 * the OID of the vector_ip_ops operator class for access method ivfflat.
 */
Oid
VectorIVFFlatIPSimilarityOperatorFamilyId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorIVFFlatIPSimilarityOperatorFamilyId == InvalidOid)
	{
		bool missingOk = false;
		Cache.VectorIVFFlatIPSimilarityOperatorFamilyId = get_opfamily_oid(
			PgVectorIvfFlatIndexAmId(), list_make2(makeString("public"), makeString(
													   "vector_ip_ops")),
			missingOk);
	}

	return Cache.VectorIVFFlatIPSimilarityOperatorFamilyId;
}


/*
 * VectorHNSWIPSimilarityOperatorFamilyId returns
 * the OID of the vector_ip_ops operator class for access method hnsw.
 */
Oid
VectorHNSWIPSimilarityOperatorFamilyId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.VectorHNSWIPSimilarityOperatorFamilyId == InvalidOid)
	{
		bool missingOk = false;
		Cache.VectorHNSWIPSimilarityOperatorFamilyId = get_opfamily_oid(
			PgVectorHNSWIndexAmId(), list_make2(makeString("public"), makeString(
													"vector_ip_ops")),
			missingOk);
	}

	return Cache.VectorHNSWIPSimilarityOperatorFamilyId;
}


/*
 * Returns the OID of gin_bson_exclusion_pre_consistent function.
 * Note: This and the associated call can be removed once 1.11 rolls out.
 */
Oid
BsonExclusionPreConsistentFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonExclusionPreconsistentFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString(
												"gin_bson_exclusion_pre_consistent"));

		bool missingOK = false;
		ObjectWithArgs args = { 0 };
		args.args_unspecified = true;
		args.objname = functionNameList;

		Cache.BsonExclusionPreconsistentFunctionId = LookupFuncWithArgs(OBJECT_FUNCTION,
																		&args, missingOK);
	}

	return Cache.BsonExclusionPreconsistentFunctionId;
}


/*
 * BsonGreaterThanOperatorId returns the OID of the <bson> > <bson> operator.
 */
Oid
BsonGreaterThanOperatorId(void)
{
	return GetCoreBinaryOperatorId(&Cache.BsonGreaterThanOperatorId,
								   BsonTypeId(), ">", BsonTypeId());
}


Oid
BsonGreaterThanEqualOperatorId(void)
{
	return GetCoreBinaryOperatorId(&Cache.BsonGreaterThanEqualOperatorId,
								   BsonTypeId(), ">=", BsonTypeId());
}


Oid
BsonLessThanEqualOperatorId(void)
{
	return GetCoreBinaryOperatorId(&Cache.BsonLessThanEqualOperatorId,
								   BsonTypeId(), "<=", BsonTypeId());
}


/*
 * BsonLessThanOperatorId returns the OID of the <bson> < <bson> operator.
 */
Oid
BsonLessThanOperatorId(void)
{
	return GetCoreBinaryOperatorId(&Cache.BsonLessThanOperatorId,
								   BsonTypeId(), "<", BsonTypeId());
}


/*
 * OID of the operator class for BSON Text operations with pgmongo_rum
 */
Oid
BsonRumTextPathOperatorFamily(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonRumTextPathOperatorFamily == InvalidOid)
	{
		/* Handles extension version upgrades */
		bool missingOk = true;
		Oid rumAmId = RumIndexAmId();
		Cache.BsonRumTextPathOperatorFamily = get_opfamily_oid(
			rumAmId, list_make2(makeString(ApiCatalogSchemaName), makeString(
									"bson_rum_text_path_ops")),
			missingOk);
	}

	return Cache.BsonRumTextPathOperatorFamily;
}


/*
 * OID of the operator class for BSON Single Path operations with pgmongo_rum
 */
Oid
BsonRumSinglePathOperatorFamily(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonRumSinglePathOperatorFamily == InvalidOid)
	{
		/* Handles extension version upgrades */
		bool missingOk = false;
		Oid rumAmId = RumIndexAmId();
		Cache.BsonRumSinglePathOperatorFamily = get_opfamily_oid(
			rumAmId, list_make2(makeString(ApiCatalogSchemaName), makeString(
									"bson_rum_single_path_ops")),
			missingOk);
	}

	return Cache.BsonRumSinglePathOperatorFamily;
}


/*
 * Returns the OID of the pg_catalog.websearch_to_tsquery function that takes
 * a single web search query text.
 */
Oid
WebSearchToTsQueryFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.WebSearchToTsQueryFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("websearch_to_tsquery"));
		Oid paramOids[1] = { TEXTOID };
		bool missingOK = false;

		Cache.WebSearchToTsQueryFunctionId =
			LookupFuncName(functionNameList, 1, paramOids, missingOK);
	}

	return Cache.WebSearchToTsQueryFunctionId;
}


/*
 * Returns the OID of the pg_catalog.websearch_to_tsquery function that
 * takes a web search query text and a text-search dictionary configuration.
 */
Oid
WebSearchToTsQueryWithRegConfigFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.WebSearchToTsQueryWithRegConfigFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("websearch_to_tsquery"));
		Oid paramOids[2] = { REGCONFIGOID, TEXTOID };
		bool missingOK = false;

		Cache.WebSearchToTsQueryWithRegConfigFunctionId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.WebSearchToTsQueryWithRegConfigFunctionId;
}


/*
 * Returns the OID of the extract_tsvector function that the RUM extension
 * has for the default TSVector operator class
 */
Oid
RumExtractTsVectorFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.RumExtractTsVectorFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(RUM_EXTENSION_SCHEMA),
											makeString("rum_extract_tsvector"));
		Oid paramOids[5] = {
			TSVECTOROID, INTERNALOID, INTERNALOID, INTERNALOID, INTERNALOID
		};
		bool missingOK = false;
		Cache.RumExtractTsVectorFunctionId =
			LookupFuncName(functionNameList, 5, paramOids, missingOK);
	}

	return Cache.RumExtractTsVectorFunctionId;
}


/*
 * Returns the OID of the bson_text_meta_qual function ID
 */
Oid
BsonTextSearchMetaQualFuncId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.BsonTextSearchMetaQualFuncId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString("bson_text_meta_qual"));
		Oid paramOids[4] = { BsonTypeId(), TSQUERYOID, BYTEAOID, BOOLOID };
		bool missingOK = false;
		Cache.BsonTextSearchMetaQualFuncId =
			LookupFuncName(functionNameList, 4, paramOids, missingOK);
	}

	return Cache.BsonTextSearchMetaQualFuncId;
}


Oid
TsRankFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.PostgresTsRankFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("ts_rank_cd"));
		Oid paramOids[3] = { FLOAT4ARRAYOID, TSVECTOROID, TSQUERYOID };
		bool missingOK = false;
		Cache.PostgresTsRankFunctionId =
			LookupFuncName(functionNameList, 3, paramOids, missingOK);
	}

	return Cache.PostgresTsRankFunctionId;
}


Oid
TsVectorConcatFunctionId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.TsVectorConcatFunctionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("tsvector_concat"));
		Oid paramOids[2] = { TSVECTOROID, TSVECTOROID };
		bool missingOK = false;
		Cache.TsVectorConcatFunctionId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.TsVectorConcatFunctionId;
}


/*
 * Returns the OID of the ts_match_vq function (maps to the function of
 * the tsvector @@ tsquery operator).
 */
Oid
TsMatchFunctionOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.TsMatchFunctionOid == InvalidOid)
	{
		List *functionNameList = list_make2(makeString("pg_catalog"),
											makeString("ts_match_vq"));
		Oid paramOids[2] = { TSVECTOROID, TSQUERYOID };
		bool missingOK = false;
		Cache.TsMatchFunctionOid =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return Cache.TsMatchFunctionOid;
}


/*
 * GetBinaryOperatorId is a helper function for getting and caching the OID
 * of a <leftTypeOid> <operatorName> <rightTypeOid> operator.
 */
static Oid
GetBinaryOperatorId(Oid *operatorId, Oid leftTypeOid, char *operatorName,
					Oid rightTypeOid)
{
	InitializeHelioApiExtensionCache();

	if (*operatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString(operatorName));

		*operatorId =
			OpernameGetOprid(operatorNameList, leftTypeOid, rightTypeOid);
	}

	return *operatorId;
}


/*
 * Gets the BinaryOperatorId similar to the function above, except in the CORE schema
 * and not the API catalog schema.
 */
static Oid
GetCoreBinaryOperatorId(Oid *operatorId, Oid leftTypeOid, char *operatorName,
						Oid rightTypeOid)
{
	InitializeHelioApiExtensionCache();

	if (*operatorId == InvalidOid)
	{
		List *operatorNameList = list_make2(makeString(CoreSchemaName),
											makeString(operatorName));

		*operatorId =
			OpernameGetOprid(operatorNameList, leftTypeOid, rightTypeOid);
	}

	return *operatorId;
}


/*
 * GetBinaryOperatorFunctionId is a helper function for getting and caching the OID
 * of a <functionName> <leftTypeOid> <rightTypeOid> operator.
 * These are binary operators where we may need to handle "missingOk" scenarios.
 * The releaseName tracks the release that introduced the operator is
 * there. This is needed until the PITR window for that release has passed.
 */
static Oid
GetBinaryOperatorFunctionIdMissingOk(Oid *operatorFuncId, char *operatorName,
									 Oid leftTypeOid, Oid rightTypeOid,
									 const char *releaseName)
{
	InitializeHelioApiExtensionCache();

	if (*operatorFuncId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString(operatorName));
		Oid paramOids[2] = { leftTypeOid, rightTypeOid };
		bool missingOK = true;

		*operatorFuncId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return *operatorFuncId;
}


/*
 * GetBinaryOperatorFunctionId is a helper function for getting and caching the OID
 * of a <functionName> <leftTypeOid> <rightTypeOid> operator.
 */
static Oid
GetBinaryOperatorFunctionId(Oid *operatorFuncId, char *operatorName,
							Oid leftTypeOid, Oid rightTypeOid)
{
	InitializeHelioApiExtensionCache();

	if (*operatorFuncId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString(operatorName));
		Oid paramOids[2] = { leftTypeOid, rightTypeOid };
		bool missingOK = false;

		*operatorFuncId =
			LookupFuncName(functionNameList, 2, paramOids, missingOK);
	}

	return *operatorFuncId;
}


/*
 * GetPostgresInternalFunctionId is a helper function for getting and caching the OID
 * of a postgres internal method
 */
Oid
GetPostgresInternalFunctionId(Oid *funcId, char *operatorName)
{
	InitializeHelioApiExtensionCache();

	if (*funcId == InvalidOid)
	{
		*funcId = fmgr_internal_function(operatorName);
	}

	return *funcId;
}


Oid
ApiCatalogCollectionIdSequenceId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CollectionIdSequenceId == InvalidOid)
	{
		List *sequenceNameList = list_make2(makeString(ApiCatalogSchemaName),
											makeString("collections_collection_id_seq"));
		RangeVar *sequenceRelRangeVar = makeRangeVarFromNameList(sequenceNameList);

		/* use AccessShareLock to prevent it getting dropped concurrently */
		bool missingOk = false;
		Cache.CollectionIdSequenceId =
			RangeVarGetRelid(sequenceRelRangeVar, AccessShareLock, missingOk);
	}

	return Cache.CollectionIdSequenceId;
}


Oid
ApiCatalogCollectionIndexIdSequenceId(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.CollectionIndexIdSequenceId == InvalidOid)
	{
		List *sequenceNameList =
			list_make2(makeString(ApiCatalogSchemaName),
					   makeString("collection_indexes_index_id_seq"));
		RangeVar *sequenceRelRangeVar = makeRangeVarFromNameList(sequenceNameList);

		/* use AccessShareLock to prevent it getting dropped concurrently */
		bool missingOk = false;
		Cache.CollectionIndexIdSequenceId =
			RangeVarGetRelid(sequenceRelRangeVar, AccessShareLock, missingOk);
	}

	return Cache.CollectionIndexIdSequenceId;
}


/*
 * Helper utility for getting Oid of a function name with given number of
 * args and schema name.
 */
static Oid
GetSchemaFunctionIdWithNargs(Oid *functionId, char *schema,
							 char *functionName, int nargs,
							 Oid *argTypes, bool missingOk)
{
	InitializeHelioApiExtensionCache();

	if (*functionId == InvalidOid)
	{
		List *functionNameList = list_make2(makeString(schema),
											makeString(functionName));
		*functionId =
			LookupFuncName(functionNameList, nargs, argTypes, missingOk);
	}

	return *functionId;
}


/*
 * Given Oid of the base element type, return the Oid of the array type.
 */
static Oid
GetArrayTypeOid(Oid *arrayTypeId, Oid baseElementType)
{
	InitializeHelioApiExtensionCache();

	if (*arrayTypeId == InvalidOid)
	{
		*arrayTypeId = get_array_type(baseElementType);
	}

	return *arrayTypeId;
}


/*
 * Wrapper function that checks for cluster version before deciding to return
 * a BsonQueryTypeid or BsonTypeId. TODO - Delete post v1.11.
 */
Oid
GetClusterBsonQueryTypeId()
{
	if (!IsClusterVersionAtleastThis(1, 9, 6))
	{
		return BsonTypeId();
	}

	return BsonQueryTypeId();
}


/*
 * Returns the OID of the ApiInternalSchemaName.index_build_is_in_progress function.
 */
Oid
IndexBuildIsInProgressFunctionId()
{
	int nargs = 1;
	Oid argTypes[1] = { INT4OID };
	bool missingOk = false;
	return GetSchemaFunctionIdWithNargs(&Cache.IndexBuildIsInProgressFunctionId,
										ApiInternalSchemaName,
										"index_build_is_in_progress", nargs, argTypes,
										missingOk);
}


/*
 * Returns the OID of the ApiDataSchemaName namespace
 */
Oid
ApiDataNamespaceOid(void)
{
	InitializeHelioApiExtensionCache();

	if (Cache.ApiDataNamespaceOid == InvalidOid)
	{
		bool missingOk = false;
		Cache.ApiDataNamespaceOid = get_namespace_oid(ApiDataSchemaName, missingOk);
	}

	return Cache.ApiDataNamespaceOid;
}
