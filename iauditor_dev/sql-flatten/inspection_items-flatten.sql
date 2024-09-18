CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_INSPECTION_ITEMS_FLTN AS
SELECT
    DATA.value:id::string AS ID,
    DATA.value:item_id::string AS ITEM_ID,
    DATA.value:audit_id::string AS AUDIT_ID,
    DATA.value:item_index::int AS ITEM_INDEX,
    DATA.value:template_id::string AS TEMPLATE_ID,
    DATA.value:parent_id::string AS PARENT_ID,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:created_at::string), 'YYYY-MM-DD') AS CREATED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:modified_at::string), 'YYYY-MM-DD') AS MODIFIED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD') AS EXPORTED_AT,
    DATA.value:type::string AS TYPE,
    DATA.value:category::string AS CATEGORY,
    DATA.value:category_id::string AS CATEGORY_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:parent_ids::string AS PARENT_IDS,
    DATA.value:label::string AS LABEL,
    DATA.value:response::string AS RESPONSE,
    DATA.value:response_id::string AS RESPONSE_ID,
    DATA.value:response_set_id::string AS RESPONSE_SET_ID,
    LEFT(DATA.value:is_failed_response::string, 1) AS IS_FAILED_RESPONSE,
    DATA.value:comment::string AS COMMENT,
    DATA.value:media_files::string AS MEDIA_FILES,
    LEFT(DATA.value:media_ids::string, 1) AS MEDIA_IDS,
    DATA.value:media_hypertext_reference::string AS MEDIA_HYPERTEXT_REFERENCE,
    DATA.value:score::int AS SCORE,
    DATA.value:max_score::int AS MAX_SCORE,
    DATA.value:score_percentage::int AS SCORE_PERCENTAGE,
    DATA.value:combined_score::int AS COMBINED_SCORE,
    DATA.value:combined_max_score::int AS COMBINED_MAX_SCORE,
    DATA.value:combined_score_percentage::int AS COMBINED_SCORE_PERCENTAGE,
    LEFT(DATA.value:mandatory::string, 1) AS MANDATORY,
    LEFT(DATA.value:inactive::string, 1) AS INACTIVE,
    DATA.value:location_latitude::float AS LOCATION_LATITUDE,
    DATA.value:location_longitude::float AS LOCATION_LONGITUDE
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_INSPECTION_ITEMS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
