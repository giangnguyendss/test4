USE CATALOG purgo_databricks;

/* 
================================================================================
Databricks SQL: Agent Log Calculated Field Query
================================================================================
Unity Catalog: purgo_databricks
Schema: purgo_playground
Target Table: agent_log
Source Tables: session_tracking_data, user_presence_tracker, agent_profile_data
Business Logic:
- For each agent (person_identifier) and log_date, display calculated fields as per mapping specification.
- Surrogate key agent_key is SHA256 hash of concatenated output fields (except agent_key, data_loaded_at).
- Hardcoded location fields.
- Handles missing/ambiguous data, multiple profile records, NULL propagation.
================================================================================
*/

/*-----------------------------------------------------------------------------
SECTION: UDF for SHA256 Hash Generation (if needed for custom logic)
-----------------------------------------------------------------------------*/
/* 
-- Not required: Use built-in sha2() function for SHA256 hash.
*/

/*-----------------------------------------------------------------------------
SECTION: CTE - Agent Log Calculated Field Construction
-----------------------------------------------------------------------------*/
WITH agent_log_cte AS (
  SELECT
    -- Surrogate key fields
    apd.internal_user_key AS unix_id,
    apd.full_name AS agent_name,
    CAST(date_trunc('DAY', std.session_start_time) AS DATE) AS log_date,
    -- First login/logout per agent per log_date
    MIN(std.session_start_time) AS agent_first_login,
    MAX(std.session_end_time) AS agent_first_logout,
    -- Total login time in hours, formatted as string with 2 decimals, NULL if logout/login is NULL
    CASE
      WHEN MIN(std.session_start_time) IS NOT NULL AND MAX(std.session_end_time) IS NOT NULL
        THEN lpad(
          CAST(
            ROUND(
              (unix_timestamp(MAX(std.session_end_time)) - unix_timestamp(MIN(std.session_start_time))) / 3600.0
            ,2) AS STRING
          ), 4, '0')
      ELSE NULL
    END AS total_login_time_hrs,
    -- Lunch fields: NULL as per mapping spec (not mapped)
    CAST(NULL AS TIMESTAMP) AS agent_lunch_login,
    CAST(NULL AS TIMESTAMP) AS agent_lunch_logout,
    CAST(NULL AS STRING) AS agent_lunch_duration,
    -- Hardcoded location fields
    "New York" AS agent_city,
    "NY" AS agent_state,
    "USA" AS agent_country,
    "10001" AS agent_zip_code,
    -- Data loaded timestamp
    current_timestamp() AS data_loaded_at,
    -- Surrogate key: SHA256 hash of concatenated fields (excluding agent_key, data_loaded_at)
    sha2(
      concat_ws('|',
        apd.internal_user_key,
        apd.full_name,
        CAST(date_trunc('DAY', std.session_start_time) AS STRING),
        CASE WHEN MIN(std.session_start_time) IS NULL THEN NULL ELSE date_format(MIN(std.session_start_time), "yyyy-MM-dd'T'HH:mm:ss'Z'") END,
        CASE WHEN MAX(std.session_end_time) IS NULL THEN NULL ELSE date_format(MAX(std.session_end_time), "yyyy-MM-dd'T'HH:mm:ss'Z'") END,
        CASE
          WHEN MIN(std.session_start_time) IS NOT NULL AND MAX(std.session_end_time) IS NOT NULL
            THEN lpad(
              CAST(
                ROUND(
                  (unix_timestamp(MAX(std.session_end_time)) - unix_timestamp(MIN(std.session_start_time))) / 3600.0
                ,2) AS STRING
              ), 4, '0')
          ELSE NULL
        END,
        "New York",
        "NY",
        "USA",
        "10001"
      ), 256
    ) AS agent_key
  FROM purgo_playground.session_tracking_data std
    -- Join to user_presence_tracker for email_address
    LEFT JOIN purgo_playground.user_presence_tracker upt
      ON std.person_identifier = upt.person_identifier
    -- Join to agent_profile_data for internal_user_key, full_name, pick most recent profile per email
    LEFT JOIN (
      SELECT
        email_address,
        internal_user_key,
        full_name,
        last_modified_timestamp
      FROM (
        SELECT
          email_address,
          internal_user_key,
          full_name,
          last_modified_timestamp,
          ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY last_modified_timestamp DESC) AS rn
        FROM purgo_playground.agent_profile_data
      ) apd_sub
      WHERE rn = 1
    ) apd
      ON upt.email_address = apd.email_address
  WHERE std.session_start_time IS NOT NULL
  GROUP BY
    apd.internal_user_key,
    apd.full_name,
    date_trunc('DAY', std.session_start_time)
)

/*-----------------------------------------------------------------------------
SECTION: Final Select - Display Calculated Agent Log Fields
-----------------------------------------------------------------------------*/
SELECT
  unix_id,
  agent_name,
  log_date,
  agent_first_login,
  agent_first_logout,
  total_login_time_hrs,
  agent_lunch_login,
  agent_lunch_logout,
  agent_lunch_duration,
  agent_city,
  agent_state,
  agent_country,
  agent_zip_code,
  data_loaded_at,
  agent_key
FROM agent_log_cte
ORDER BY log_date, unix_id;

/*-----------------------------------------------------------------------------
SECTION: Column Comments for Documentation
-----------------------------------------------------------------------------*/
COMMENT ON COLUMN purgo_playground.agent_log.unix_id IS 'Agent unique internal user key, from agent_profile_data.internal_user_key. May be NULL if missing profile data.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_name IS 'Agent full name, from agent_profile_data.full_name. May be NULL if missing profile data.';
COMMENT ON COLUMN purgo_playground.agent_log.log_date IS 'Date (YYYY-MM-DD) of agent activity, derived from session_start_time.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_first_login IS 'Earliest session_start_time for agent on log_date, ISO 8601 format.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_first_logout IS 'Latest session_end_time for agent on log_date, ISO 8601 format. May be NULL if missing session_end_time.';
COMMENT ON COLUMN purgo_playground.agent_log.total_login_time_hrs IS 'Total login time in hours (string, 2 decimal places), calculated as agent_first_logout - agent_first_login. NULL if either is NULL.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_lunch_login IS 'Lunch login timestamp. NULL (not mapped).';
COMMENT ON COLUMN purgo_playground.agent_log.agent_lunch_logout IS 'Lunch logout timestamp. NULL (not mapped).';
COMMENT ON COLUMN purgo_playground.agent_log.agent_lunch_duration IS 'Lunch duration in hours (string). NULL (not mapped).';
COMMENT ON COLUMN purgo_playground.agent_log.agent_city IS 'Agent city. Hardcoded as "New York". Valid values: "New York".';
COMMENT ON COLUMN purgo_playground.agent_log.agent_state IS 'Agent state. Hardcoded as "NY". Valid values: "NY".';
COMMENT ON COLUMN purgo_playground.agent_log.agent_country IS 'Agent country. Hardcoded as "USA". Valid values: "USA".';
COMMENT ON COLUMN purgo_playground.agent_log.agent_zip_code IS 'Agent zip code. Hardcoded as "10001". Valid values: "10001".';
COMMENT ON COLUMN purgo_playground.agent_log.data_loaded_at IS 'Timestamp when data was loaded. Current timestamp in ISO 8601 format.';
COMMENT ON COLUMN purgo_playground.agent_log.agent_key IS 'Surrogate key. SHA256 hash of concatenated output fields (except agent_key, data_loaded_at), lowercase hex.';

/*-----------------------------------------------------------------------------
-- End of Agent Log Calculated Field Query
-----------------------------------------------------------------------------*/
