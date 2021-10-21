--# Setting hive and map reduce params (configs)
set hive.merge.tezfiles = True;
set hive.merge.mapfiles = True;
--# Many More configs based on project hql

use ${db_inp_data};

DROP TABLE IF EXISTS ${db_inp_data}.${score_model_id}_base_01;
CREATE TABLE ${db_inp_data}.${score_model_id}_base_01
STORED AS ORC
TBLPROPERTIES(
--#Based on project and org
"orc.compress" = "ZLIB",
"orc.block.size" = "134217728",
"orc.stripe.size" = "134217728",
"orc.create.index" = "true",
"orc.row.index.stride" = "100000",
"orc.file.stripe.level" = "true"
) 
AS
--SCORING POPULATION SQLCODE
SELECT DISTINCT A.CUST_ID,A.ACCT_NUM,A.SOR_ID,A.HOME_AREA_CD AS AREA_CD
FROM ${database}.Table_Name A
WHERE
--FILTER CONDITIONS
A.ACCT_STATUS_IND = 'A' AND
ACCT_ESTB_DT < ${run_date} AND
(ACCT_TERM_DT IS NULL OR ACCT_TERM_DT > last_day(add_months(${run_date},-1))) AND
UPPER(SOR_ID) = 'V';

--FEATURE_ENTITY1
DROP TABLE IF EXISTS ${db_inp_data}.${score_model_id}_FEATURE_ENTITY1;
CREATE TABLE ${db_inp_data}.${score_model_id}_FEATURE_ENTITY1
STORED AS ORC
TBLPROPERTIES(
--#Based on project and org
"orc.compress" = "ZLIB",
"orc.block.size" = "134217728",
"orc.stripe.size" = "134217728",
"orc.create.index" = "true",
"orc.row.index.stride" = "100000",
"orc.file.stripe.level" = "true"
) 
AS
SELECT * FROM (
SELECT DISTINCT A.CUST_ID,A.ACCT_NUM,COALESCE(B.TOTAL_LINES_ON_ACCT,0) AS TOTAL_LINES_ON_ACCT,
COALESCE(B.ACCT_ACTIVE_LOAN_CNT,0) AS ACCT_ACTIVE_LOAN_CNT,
COALESCE(B.ACCT_TENURE_MNTHS,0) AS ACCT_TENURE_MTHS,
ROW_NUMBER() OVER (PARTITION BY A.CUST_ID,A.ACCT_NUM ORDER BY date_format('{0}','yyyy-MM-dd') desc) AS RNUM
FROM ACC_BASE A LEFT JOIN FEATURE_TABLE B
ON A.CUST_ID = B.CUST_ID
WHERE B.RPT_MTH = date_format(add_months('{0}',-1), 'yyyy-MM-01')
AND B.BASE_MTH IS NOT NULL) AB
WHERE RNUM=1;

--FEATURE_ENTITY2
DROP TABLE IF EXISTS ${db_inp_data}.${score_model_id}_FEATURE_ENTITY2;
CREATE TABLE ${db_inp_data}.${score_model_id}_FEATURE_ENTITY2
STORED AS ORC
TBLPROPERTIES(
--#Based on project and org
"orc.compress" = "ZLIB",
"orc.block.size" = "134217728",
"orc.stripe.size" = "134217728",
"orc.create.index" = "true",
"orc.row.index.stride" = "100000",
"orc.file.stripe.level" = "true"
) 
AS
SELECT * FROM (
SELECT A.CUST_ID,A.ACCT_NUM,COALESCE(B.REV_LAST_2YR_PURCHASE,0) AS REV_LAST_2YR_PURCHASE,
COALESCE(B.TOT_ACCSSRY_PURCHASE,0) AS TOT_ACCSSRY_PURCHASE,
ROW_NUMBER() OVER(PARTITION BY A.CUST_ID,A.ACCT_NUM ORDER BY '{0}' DESC) AS RN1
FROM ACC_BASE A
LEFT JOIN
(SELECT * FROM 
  (SELECT CUST_ID,ACCT_NUM,SLS_DIST_CHNL_TYPE_CD,SUM(ITEM_PRICE_AMT) AS REV_LAST_2YR_PURCHASE,
  SUM(NET_SALES) AS TOT_ACCSSRY_PURCHASE
  FROM TABLE WHERE
  LOWER(EQP_CLASS_DESC) = 'accessories' AND 
  PYMNT_DT>=date_format(add_months('{0}',-24),'yyyy-MM-dd') AND PYMNT_DT<date_format('{0}','yyyy-MM-dd')
  AND RPT_MTH<'{0}'
  GROUP BY 1,2,3) AB
  WHERE TOT_ACCSSRY_PURCHASE>0)B
  ON A.CUST_ID=B.CUST_ID AND A.ACCT_NUM=B.ACCT_NUM)X
  WHERE RN1=1;

--FINAL TABLE WITH ALL FEATURES
DROP TABLE IF EXISTS ${db_inp_data}.${score_model_id}_BASE_FINAL;
CREATE TABLE ${db_inp_data}.${score_model_id}_BASE_FINAL
STORED AS ORC
TBLPROPERTIES(
--#Based on project and org
"orc.compress" = "ZLIB",
"orc.block.size" = "134217728",
"orc.stripe.size" = "134217728",
"orc.create.index" = "true",
"orc.row.index.stride" = "100000",
"orc.file.stripe.level" = "true"
) 
AS
SELECT DISTINCT A.CUST_ID,A.ACCT_NUM,A.ACC_FLAG,
COALESCE(B.TOTAL_LINES_ON_ACCT,0) AS TOTAL_LINES_ON_ACCT,
COALESCE(B.ACCT_ACTIVE_LOAN_CNT,0) AS ACCT_ACTIVE_LOAN_CNT,
COALESCE(B.ACCT_TENURE_MTHS,0) AS ACCT_TENURE_MTHS,
COALESCE(ROUND(D.REV_LAST_2YR_PURCHASE,2),0) AS REV_LAST_2YR_PURCHASE,
COALESCE(ROUND(D.TOT_ACCSSRY_PURCHASE,2),0) AS TOT_ACCSSRY_PURCHASE
FROM ACC_BASE A LEFT JOIN FEATURE_ENTITY1 B
ON A.CUST_ID = B.CUST_ID AND A.ACCT_NUM = B.ACCT_NUM
LEFT JOIN FEATURE_ENTITY2 D
ON A.CUST_ID = D.CUST_ID AND A.ACCT_NUM = D.ACCT_NUM;

--DUPLICATE RECORDS CHECK
DROP TABLE IF EXISTS ${db_inp_data}.${tbl_inp_data};
CREATE TABLE ${db_inp_data}.${tbl_inp_data}
STORED AS ORC
TBLPROPERTIES(
--#Based on project and org
"orc.compress" = "ZLIB",
"orc.block.size" = "134217728",
"orc.stripe.size" = "134217728",
"orc.create.index" = "true",
"orc.row.index.stride" = "100000",
"orc.file.stripe.level" = "true"
) 
AS
SELECT A.CUST_ID,
A.ACCT_NUM,
A.TOTAL_LINES_ON_ACCT,
A.ACCT_ACTIVE_LOAN_CNT,
A.ACCT_TENURE_MTHS,
A.REV_LAST_2YR_PURCHASE,
A.TOT_ACCSSRY_PURCHASE,
'${score_model_id}' AS SCORE_MODEL_ID,
'P' AS SCORE_TYPE,
last_day(${run_date}) AS RPT_MTH
FROM 
(SELECT T1.*, ROW_NUMBER() OVER (PARTITION BY T1.CUST_ID,T1.ACCT_NUM) AS RNK
FROM ${db_inp_data}.${score_model_id}_BASE_FINAL T1)A 
WHERE RNK=1;



--COMMAND TO RUN FROM CLI TO EXECUTE HQL SCRIPT
hive -f cli_cmnd_hql_script.hql --hivevar db_inp_data=input_database --hivevar tbl_inp_data=features_table --hivevar run_date=\'2021-05-01\' --hivevar score_model_id=modelname --hivevar database=database_used_sql 
