CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_INSPECTIONS_FLTN AS
SELECT
    DATA.value:id::string AS AUDIT_ID,
    DATA.value:name::string AS NAME,
    LEFT(DATA.value:archived::string, 1) AS ARCHIVED,
    DATA.value:owner_name::string AS OWNER_NAME,
    DATA.value:owner_id::string AS OWNER_ID,
    DATA.value:author_name::string AS AUTHOR_NAME,
    DATA.value:author_id::string AS AUTHOR_ID,
    DATA.value:score::int AS SCORE,
    DATA.value:max_score::int AS MAX_SCORE,
    DATA.value:score_percentage::float AS SCORE_PERCENTAGE,
    DATA.value:duration::int AS DURATION,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:template_name::string AS TEMPLATE_NAME,
    DATA.value:template_author::string AS TEMPLATE_AUTHOR,
    DATA.value:site_id::string AS SITE_ID,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:date_started::string), 'YYYY-MM-DD') AS DATE_STARTED,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:date_completed::string), 'YYYY-MM-DD') AS DATE_COMPLETED,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:date_modified::string), 'YYYY-MM-DD') AS DATE_MODIFIED,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:created_at::string), 'YYYY-MM-DD') AS CREATED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:modified_at::string), 'YYYY-MM-DD') AS MODIFIED_AT,
    DATA_DATE AS EXPORTED_AT,
    DATA.value:document_no::string AS DOCUMENT_NO,
    DATA.value:prepared_by::string AS PREPARED_BY,
    DATA.value:location::string AS LOCATION,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:conducted_on::string), 'YYYY-MM-DD') AS CONDUCTED_ON,
    DATA.value:personnel::string AS PERSONNEL,
    DATA.value:client_site::string AS CLIENT_SITE,
    DATA.value:latitude::float AS LATITUDE,
    DATA.value:longitude::float AS LONGITUDE,
    DATA.value:web_report_link::string AS WEB_REPORT_LINK
    FALSE AS DELETED
    DATA.value:asset_id::string AS ASSET_ID,

FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_INSPECTIONS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
