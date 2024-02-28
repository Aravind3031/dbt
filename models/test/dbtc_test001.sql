{{ config( materialized = "view", pre_hook = "" ) }}

WITH 
tFileInputDelimited_1 AS (SELECT study_id, program_id FROM {{ var('tFileInputDelimited_1') }}),
tFileInputDelimited_2 AS (SELECT study_id, study_name, study_description FROM {{ var('tFileInputDelimited_2') }}),
tJoin_1 AS (SELECT study_id, program_id, study_name FROM tFileInputDelimited_1 INNER JOIN tFileInputDelimited_2 ON tFileInputDelimited_1.study_id = tFileInputDelimited_2.study_id),
tFileInputDelimited_3 AS (SELECT study_id, program_id, study_name FROM {{ var('tFileInputDelimited_3') }}),
tUnite_1 AS (SELECT study_id, program_id, study_name FROM tJoin_1 UNION ALL SELECT study_id, program_id, study_name FROM tFileInputDelimited_3),
tFilterRow_2 AS (SELECT study_id, program_id, study_name FROM tUnite_1 WHERE study_id >= 5),
tMap_1 AS (SELECT CASE WHEN CASE WHEN tFilterRow_2.study_id = NULL THEN TRUE ELSE FALSE END = TRUE THEN '-1' ELSE tFilterRow_2.study_id END AS study_id, TRIM(tFilterRow_2.program_id)  AS program_id, UPPER(tFilterRow_2.study_name )  AS study_name FROM tFilterRow_2)
SELECT study_id, program_id, study_name FROM tMap_1