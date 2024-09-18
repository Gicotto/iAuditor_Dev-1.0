CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTION_TIMELINE_ITEMS_FLTN AS
SELECT
    DATA.value:id::string AS ITEM_ID,
    DATA.value:task_id::string AS TASK_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:task_creator_name::string AS TASK_CREATOR_NAME,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:timestamp::string), 'YYYY-MM-DD') AS TIMESTAMP,
    DATA.value:creator_id::string AS CREATOR_ID,
    DATA.value:creator_name::string AS CREATOR_NAME,
    DATA.value:item_type::string AS ITEM_TYPE,
    DATA.value:item_data::string AS ITEM_DATA
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTION_TIMELINE_ITEMS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
