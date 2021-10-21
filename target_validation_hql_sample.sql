use ${db_target_data};
drop table if exists ${db_target_data}.${tbl_target_data};
create table ${db_target_data}.${tbl_target_data} as  -- CREATING TARGET TABLE
SELECT A.*, COALESCE(B.ACCESSORY,0) AS ACC_FLAG FROM ${db_final_score}.${tbl_final_score} A
LEFT JOIN 
-- BELOW IS THE TARGET QUERY
(SELECT * FROM (
SELECT DISTINCT CUST_ID, ACCT_NUM, EQP_CLASS_DESC, PYMNT_DT, 1 AS ACCESSORY,
ROW_NUMBER() OVER (PARTITION BY CUST_ID,ACCT_NUM ORDER BY PYMNT_DT DESC) AS RNUM
FROM ${db_vzw_uda}.EQUIP_SUM_FACT WHERE
EQP_CLASS_DESC = 'Accessories' AND
SLS_DIST_CHNL_TYPE_CD = 'N' AND
PYMNT_DT BETWEEN date_format(${run_date}, 'yyyy-MM-dd') and last_day(${run_date})
AND RPT_MTH = ${run_date}) AB
WHERE RNUM =1) B
ON A.CUST_ID = B.CUST_ID AND A.ACCT_NUM = B.ACCT_NUM

----##### Other Standardised one below ### -----

set validation_dt = last_day(add_months(${run_date}, -1));
set validation_mth = last_day(add_months(${run_date}, -1));

use ${db_target_data};
drop table if exists ${db_target_data}.${tbl_target_data};
create table ${db_target_data}.${tbl_target_data} as
SELECT DISTINCT ${hiveconf:validation_dt} as VALIDATION_DT,
				fs.SCORE_MODEL_ID, fs.SOR_ID, fs.CUST_ID, fs.ACCT_NUM, fs.SCORE_VALUE, fs.SCORE_DECILE, fs.SCORE_CENTILE,
				nvl(tg.TARGET, 0) as TARGET_VALUE, CURRENT_TIMESTAMP AS INSERT_DT
FROM ${db_final_score}.${tbl_final_score} fs
LEFT JOIN 
(SELECT * FROM (
SELECT DISTINCT CUST_ID, ACCT_NUM, EQP_CLASS_DESC, PYMNT_DT, 1 AS TARGET,
ROW_NUMBER() OVER (PARTITION BY CUST_ID,ACCT_NUM ORDER BY PYMNT_DT DESC) AS RNUM
FROM ${db_vzw_uda}.EQUIP_SUM_FACT WHERE
EQP_CLASS_DESC = 'Accessories' AND
SLS_DIST_CHNL_TYPE_CD = 'N' AND
PYMNT_DT BETWEEN date_format(${hiveconf:validation_mth}, 'yyyy-MM-01') and date_format(${hiveconf:validation_mth}, 'yyyy-MM-dd')
AND RPT_MTH = date_format(${hiveconf:validation_mth}, 'yyyy-MM-01')) AB
WHERE RNUM =1) tg
ON fs.CUST_ID = tg.CUST_ID AND fs.ACCT_NUM = tg.ACCT_NUM
WHERE ${rpt_attribute} = ${hiveconf:validation_dt} and fs.score_model_id = '${score_model_id}';