CREATE OR REPLACE FUNCTION assert_count_regex_options(expected_row_count int, query documentdb_core.bson)
 RETURNS void
 LANGUAGE plpgsql
AS $$
DECLARE
	returned_row_count int;
BEGIN
	SELECT count(*) INTO returned_row_count FROM collection('db','regex_options') WHERE document @@ query;

	IF returned_row_count <> expected_row_count THEN
		RAISE 'query % returned % rows instead of %', query, returned_row_count, expected_row_count;
	END IF;
END;
$$;

SELECT insert_one('db','regex_options', '{"msg": "hpattern New"}');
SELECT insert_one('db','regex_options', '{"msg": "pattX New"}');
SELECT insert_one('db','regex_options', '{"msg": "pattX\nNew"}');
SELECT insert_one('db','regex_options', '{"msg": "pattx\nNew"}');
SELECT insert_one('db','regex_options', '{"msg": "pattx_New"}');
SELECT insert_one('db','regex_options', '{"msg": "hello\npattx\nNewLineStart"}');
SELECT insert_one('db','regex_options', '{"msg": "hello\npattx NewWord"}'); 
SELECT insert_one('db','regex_options', '{"msg": "hello\npattx NewWord"}');
SELECT insert_one('db','regex_options', '{"msg": "hello\npattX New\nWord"}');

do $$
DECLARE
	temp text;
begin
	for counter in 1..10 loop
		SELECT insert_one('db','regex_options', '{"filler": "fillerValue"}') into temp;
   	end loop;
end
$$;

-- DROP PRIMARY KEY
SELECT documentdb_distributed_test_helpers.drop_primary_key('db', 'regex_options');

SELECT assert_count_regex_options(8, '{"msg": {"$options": "ims", "$regex": "^pattX.New"}}');
SELECT assert_count_regex_options(5, '{"msg": {"$regex": "^pattX.New$", "$options": "mis"}}');
SELECT assert_count_regex_options(8, '{"msg": {"$regex": "^pattX.New",  "$options": "ims"}}');

-- With comment and space in pattern and without extended match flag (x)
SELECT assert_count_regex_options(0, '{"msg": {"$regex": "^pattX.N#COMMENT\n ew",  "$options": "ims"}}');

-- With comment and space in pattern and with extended match flag (x)
SELECT assert_count_regex_options(8, '{"msg": {"$regex": "^pattX.N#COMMENT\n ew",  "$options": "ixms"}}');

SELECT assert_count_regex_options(1, '{"msg": {"$regex": "^pattX.New",  "$options": ""}}');
SELECT assert_count_regex_options(2, '{"msg": {"$regex": "^pattX.New",  "$options": "i"}}');

-- . matches new line when s flag is set.
SELECT assert_count_regex_options(2, '{"msg": {"$regex": "^pattX.New",  "$options": "s"}}');

-- multiline match flag
SELECT assert_count_regex_options(2, '{"msg": {"$regex": "^pattX.New",  "$options": "m"}}');

-- . matches new line and case insensitive match
SELECT assert_count_regex_options(4, '{"msg": {"$regex": "^pattX.New",  "$options": "si"}}');
SELECT assert_count_regex_options(5, '{"msg": {"$regex": "^pattX.New",  "$options": "mi"}}');
SELECT assert_count_regex_options(3, '{"msg": {"$regex": "^pattX.New",  "$options": "ms"}}');

-- This will work as an implicit AND. TODO. To make the behavior same as from Mongo shell through GW : Mongo on Citus
-- SELECT assert_count_regex_options(1,'{"msg": {"$regex": "hpattern.new", "$eq": "hpattern New", "$options": "i"}}');

-- eq does not match
--SELECT assert_count_regex_options(0,'{"msg": {"$regex": "hpattern.new", "$eq": "hpatternNew", "$options": "i"}}');

-- Regex options does not make the document to match query spec.
--SELECT assert_count_regex_options(0,'{"msg": {"$regex": "hpattern.new", "$eq": "hpattern New", "$options": ""}}');

-- When there are duplicate keys, the last one of each will be considered.
SELECT assert_count_regex_options(1,'{"msg": {"$regex": "hpattern.new", "$options": "i", "$regex": "^word$", "$options": "mi"}}');

-- In continuation with the above tests: To ensure that the last regex options pair is really getting taken.
SELECT assert_count_regex_options(0,'{"msg": {"$regex": "hpattern.new", "$options": "i", "$regex": "^word$", "$options": "i"}}');

-- This will error out because $options needed a $regex.
SELECT document FROM collection('db','regex_options') WHERE document @@ '{"msg": {"$options": "i"}}';
