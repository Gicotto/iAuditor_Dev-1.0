CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTION_ASSIGNEES_FLTN AS
SELECT
    DATA_DATE,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS ID,
    DATA.value:action_id::string AS ACTION_ID,
    DATA.value:assignee_id::string AS ASSIGNEE_ID,
    DATA.value:type::string AS TYPE,
    DATA.value:name::string AS CREATOR_USER_NAME,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:modified_at::string AS MODIFIED_AT
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTION_ASSIGNEES,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
