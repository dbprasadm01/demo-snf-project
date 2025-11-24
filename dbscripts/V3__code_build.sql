--- Procedures to load data
use role DEV_GW_CDA_OWNER;
use warehouse cda_wh_xs;
use database DEV_GW_CDA;
use schema CDA_ABC_SHARED ;
---- 1st Stored procedure
create or replace procedure CDA_ABC_SHARED.SP_CDA_BLD_MANIFEST_ROOT_STAGE (CDA_DB_NAME string , RAW_SCHEMA_NM string , S3_PREFIX_CDA_PARQ STRING , TABLEPRE STRING DEFAULT 'ALL')
  returns string
  language javascript
  EXECUTE AS CALLER
  as
  $$
try {
    var cda_db_nm = CDA_DB_NAME
    var schema_nm = RAW_SCHEMA_NM
    var schema_full_qual = CDA_DB_NAME+"."+RAW_SCHEMA_NM
    var s3_prefix_cda_parq = S3_PREFIX_CDA_PARQ.toLowerCase()
//    var cda_src_env = s3_prefix_cda_parq.split("/")[3]
    var cdaroot_extstg_name = cda_db_nm+"."+schema_nm+".cda_manifest_root_extstage"
    var tablePre ;
    if (TABLEPRE === 'ALL') {
        tablePre = '1=1';
  }   else {
    tablePre = " SPLIT_PART(KEY, '_', 1) = '" + TABLEPRE.toLowerCase() + "'";
  }
//adding this to improve performance as snowflake still checks directory even when stage is out there  and if NOT EXISTS is used in create
    var stg_exists_cmd = "SHOW STAGES LIKE 'CDA_MANIFEST_ROOT_EXTSTAGE' IN schema " + schema_full_qual ;
    var stg_exists_stmt  = snowflake.createStatement({ sqlText: stg_exists_cmd });
    var stg_exists  = stg_exists_stmt.execute();
    if (!stg_exists.next()) {
// Create external stage (if not exists) at the GW root location, to list all available GW tables  across all X-centres in a given ENV  , Parameterise DEV_S3_EDMDEV_GWCDA_SF
        var create_stage_command = "CREATE STAGE IF NOT EXISTS "+cdaroot_extstg_name+" \
                                      url = 's3://"+s3_prefix_cda_parq+"/' \
                                      STORAGE_INTEGRATION = DEV_S3_CDA_SF \
                                      directory = (enable = true auto_refresh = true) \
                                      file_format = ( TYPE = parquet)";
        var create_stage_statement = snowflake.createStatement( {sqlText: create_stage_command} );
        var create_stage = create_stage_statement.execute();
        }
//perform refresh ONLY FOR incremental , IL already got auto_refresh IN STAGE. FOR PC WHEN executing FROM IL one timer , refresh IS blocking sessions FOR other SP
// commenting OUT AND changing logic AS prod IS gettting MORE freq updates than lower AND WITH snowflake 1+ mill FILEs on s3 prefix AT root causing 10+mins
//    if (TABLEPRE === 'ALL') {
//    var refresh_stage_command = "ALTER STAGE "+cdaroot_extstg_name+" REFRESH";
//      var refresh_stage_statement = snowflake.createStatement( {sqlText: refresh_stage_command} );
//      var refresh_stage = refresh_stage_statement.execute();
//   }
// --   var list_stage_command = "select distinct SPLIT_PART(relative_path, '/', 1)  from directory(@"+cdaroot_extstg_name+") where relative_path like '%.snappy.parquet' and SPLIT_PART(relative_path, '/', 1) NOT IN ( \ "
    var list_stage_command = "SELECT DISTINCT  m.KEY FROM @" +cdaroot_extstg_name+ "/manifest.json (FILE_FORMAT => "+cda_db_nm+".CDA_ABC_SHARED.FF_CDA_JSON ) , table(flatten(INPUT => parse_json($1))) m WHERE m.KEY  \
                NOT IN (SELECT lower(table_name) FROM " + cda_db_nm + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" +schema_nm + "' AND  TABLE_TYPE <> 'VIEW'  ) and " +tablePre + " order by 1 ";
//updating this FROM relative_path TO SPLIT_PART(relative_path, '/', 1) -- relative_path  order by 1 limit 10
    var list_stage_statement = snowflake.createStatement( {sqlText: list_stage_command} );
    var list_stage_obj = list_stage_statement.execute();
    var list_stage = [];
    while (list_stage_obj.next())  {
        list_stage.push(list_stage_obj.getColumnValue(1).split("/")[0]);
    };
    table_array = [... new Set(list_stage)]
    for (let i = 0;i < table_array.length; i++) {
        var initiate_table_command = "call " + cda_db_nm + ".CDA_ABC_SHARED.SP_CREATE_CDARAW_STAGES_TABLES_TASKS('" +cda_db_nm + "','" +schema_nm + "','" +s3_prefix_cda_parq + "','" +table_array[i]+ "')";
        var initiate_table_statement = snowflake.createStatement( {sqlText: initiate_table_command} );
        var initiate_table = initiate_table_statement.execute();
    }
  if(table_array.length>0){
    return 'Raw tables created : ' + table_array;
  }
  else{
    return "No tables to add."
  }
} catch (err) {
  throw 'Error: ' + err;
}
  $$
  ;

---- 2nd Stored procedure
use schema CDA_ABC_SHARED ;
create or replace PROCEDURE CDA_ABC_SHARED.SP_CREATE_CDARAW_STAGES_TABLES_TASKS (CDA_DB_NAME string , RAW_SCHEMA_NM string , S3_PREFIX_CDA_PARQ STRING , RAW_TAB_NM STRING)
  returns string
  language javascript
  EXECUTE AS caller
  as
  $$
try {
    var s3_prefix_cda_parq = S3_PREFIX_CDA_PARQ.toLowerCase()
    var raw_tab_nm = RAW_TAB_NM
    var cda_tab_nm = RAW_TAB_NM.toUpperCase()
    var cda_db_nm = CDA_DB_NAME
    var schema_nm = RAW_SCHEMA_NM
    var full_qual_nm = cda_db_nm+"."+schema_nm+"."+cda_tab_nm
    var create_table_cmdd = ""
    var extstg_name = full_qual_nm+"_EXTSTAGE"
    var input_option_error_flag = 0;
    var task_name = full_qual_nm+"_COPY_TASK"
// Create external stage only one time to use for external table creation , Parameterise integration
    var create_stage_cmd = "CREATE STAGE IF NOT EXISTS "+extstg_name+" \
                                  url = 's3://"+s3_prefix_cda_parq+"/"+raw_tab_nm+"/' \
                                  STORAGE_INTEGRATION =  DEV_S3_CDA_SF \
                                  directory = (enable = true auto_refresh = true) \
                                  file_format = ( TYPE = parquet  BINARY_AS_TEXT = FALSE) ";
    var create_stage_statement = snowflake.createStatement( {sqlText: create_stage_cmd} );
    var create_stage = create_stage_statement.execute();
    var refresh_stage_command = "ALTER STAGE "+extstg_name+" REFRESH";
    var refresh_stage_statement = snowflake.createStatement( {sqlText: refresh_stage_command} );
    var refresh_stage = refresh_stage_statement.execute();
//  Chai - Adding this code TO LIMIT infer SCHEMA TO ONLY latest FILE TO workaround WHEN multiple FILES -- Max LOB size (16777216) exceeded, actual size of parsed column is 56543418
    var filepath_cmd = "SELECT relative_path FROM directory('@" + extstg_name + "') where relative_path like '%.snappy.parquet' QUALIFY ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(relative_path, '/', 1) ORDER BY last_modified DESC) = 1";
    var filepath_stmnt = snowflake.createStatement( {sqlText: filepath_cmd} );
    var filepath_exec = filepath_stmnt.execute();
     if (filepath_exec.next()) {
        outresult = filepath_exec.getColumnValue(1);
   } else {
        outresult = "null - error : table not created - " + full_qual_nm ;
        throw outresult
    }
  var latest_filepath_table_nm = filepath_exec.getColumnValue(1) ;
// Original , adding TABLE NAME prefix WITH latest file RELATIVE PATH FOR EACH TABLE //01052024 Added , IGNORE_CASE => TRUE TO avoid double quotes
  create_table_cmd = "create table "+full_qual_nm+" if not exists using template( select array_agg( object_construct(*)) from table( infer_schema( location=>'@"+extstg_name+ "/" +latest_filepath_table_nm + "', FILE_FORMAT=>'" +cda_db_nm+".CDA_ABC_SHARED.FF_CDA_PARQUET' , IGNORE_CASE => TRUE ))) ENABLE_SCHEMA_EVOLUTION = TRUE";
  create_table_statement = snowflake.createStatement( {sqlText: create_table_cmd} );
    tablecreation = create_table_statement.execute();
//-- CREATE OR REPLACE vs if not exists ***** Parameterize WAREHOUSE AND CHANGE FROM LOAD_WH
    var create_task_command = "CREATE OR REPLACE TASK  "+task_name+"  WAREHOUSE = 'CDA_WH_XS' SCHEDULE = '5 MINUTE' AS \
                                    copy into "+full_qual_nm+" FROM @"+extstg_name+" FILE_FORMAT=(TYPE=PARQUET BINARY_AS_TEXT = FALSE ) PATTERN = '.*.parquet' \
                                    match_by_column_name = CASE_INSENSITIVE   ";
//schedule = 'USING CRON 26 10 * * * America/Chicago'
  var create_task_statement = snowflake.createStatement( {sqlText: create_task_command} );
    var create_task = create_task_statement.execute();
//commented OUT since orhestrated FROM opcon
//    var resume_task_command = "alter task "+task_name+" resume";
//    var resume_task_statement = snowflake.createStatement( {sqlText: resume_task_command} );
//    var resume_task = resume_task_statement.execute();
  if(tablecreation){
    return "Created RAW Tables are :  "+full_qual_nm+"\n -------------------------------- \n";
  }
  else{
    return "No new tables are created"
  }
} catch (err) {
  throw  'SP CREATE_CDARAW_STAGES_TABLES_TASKS failed with error : ' + err;
}
  $$
  ;


---- 3rd Stored procedure
use schema CDA_ABC_SHARED ;
CREATE OR REPLACE PROCEDURE CDA_ABC_SHARED.SP_EXECUTE_ALL_RAW_TASKS(CDA_DB_NAME string , RAW_SCHEMA_NM string)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
try {
  var stmt;
  var task_name;
  var cda_db_nm = CDA_DB_NAME
  var sf_env = cda_db_nm.substring(0, cda_db_nm.indexOf("_"));
  var firstUnderscore = RAW_SCHEMA_NM.indexOf('_');
  var xcenter_nm = RAW_SCHEMA_NM.substring(firstUnderscore + 1, RAW_SCHEMA_NM.indexOf('_', firstUnderscore + 1));

  //INSERT INTO logging TABLE AND this event timestamp will be used IN NEXT job/sp FOR checking completion
 var tasksResultSet = snowflake.execute({
    sqlText: `
    INSERT INTO ${CDA_DB_NAME}.CDA_ABC_SHARED.${xcenter_nm}_CDA_LOGGING (LOG_INFO) SELECT TO_VARIANT(PARSE_JSON('{"SP_NAME": "SP_EXECUTE_ALL_RAW_TASKS","ENV": "${sf_env}"}'));
    `
  });
  var tasklist = snowflake.execute({
    sqlText: `    show tasks IN  ${CDA_DB_NAME}.${RAW_SCHEMA_NM}  `
  });
  // Get the task names using last_queryid
  var tasksResultSet = snowflake.execute({
    sqlText: `
    select "name" TASKNAME from table(result_scan(last_query_id()))
    `
  });
//SELECT "name" AS TASKNAME FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY( SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()), RESULT_LIMIT => 1000  )) WHERE SCHEMA_NAME = 'CDA_RAW'
  while (tasksResultSet.next()) {
    task_name = tasksResultSet.getColumnValue('TASKNAME');
    stmt = `EXECUTE TASK   ${CDA_DB_NAME}.${RAW_SCHEMA_NM}.${task_name} ; `
    snowflake.execute({ sqlText: stmt });
  }
  return 'ALL tasks submitted successfully';
} catch (err) {
  throw 'Error in submission of task: ' + err;
}
$$;
-- 4th Stored Procedure
use schema CDA_ABC_SHARED ;
CREATE OR REPLACE PROCEDURE SP_RAW_CDA_AUDIT_METRIC  (DB_NAME VARCHAR, SCHEMA_NAME VARCHAR , ABC_ROWCNT_THRESHOLD string DEFAULT '')
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
try {
    var abc_threshold_percent;
    var last_upd_past_mins = 20 ; // can be added to args qc if need to separate per env
    if (ABC_ROWCNT_THRESHOLD !== '') {
        abc_threshold_percent = ABC_ROWCNT_THRESHOLD;
    } else {
        abc_threshold_percent = '0.00';
    }
  var qry_heartbeat = `SELECT CREATETIME, sysdate() AS gmttime, DATEDIFF('minute', CREATETIME, sysdate()) AS LAST_UPDATED_MINS FROM ${DB_NAME}.${SCHEMA_NAME}.HEARTBEAT ORDER BY 1 DESC LIMIT 1;
  `;
  var stmt_heartbeat = snowflake.createStatement({sqlText: qry_heartbeat});
  var resultSet1 = stmt_heartbeat.execute();
  resultSet1.next();
  var lastUpdatedMins = resultSet1.getColumnValue('LAST_UPDATED_MINS');
  if (lastUpdatedMins > last_upd_past_mins) {
    throw ' \n Error: Heartbeat table was last updated ' + lastUpdatedMins + ' minutes back which exceeds the set limit of ' + last_upd_past_mins + 'minutes. Check the stream , recent execution. '  ;
  }
  var firstUnderscore = SCHEMA_NAME.indexOf('_');
  var xcenter_nm = SCHEMA_NAME.substring(firstUnderscore + 1, SCHEMA_NAME.indexOf('_', firstUnderscore + 1));
    var query1 = `create or replace temporary table  ${DB_NAME}.CDA_ABC_SHARED.SRC_MANIFEST ( cda_raw_table_name varchar , src_row_count integer, parquet_write_timestamp TIMESTAMP_NTZ , schemaHistory VARIANT)
    AS ( SELECT manifesttable.KEY cda_raw_table_name , value:totalProcessedRecordsCount src_row_count , value:lastSuccessfulWriteTimestamp parquet_write_timestamp , value:schemaHistory
    FROM @${DB_NAME}.${SCHEMA_NAME}.cda_manifest_root_extstage/manifest.json (FILE_FORMAT => ${DB_NAME}.CDA_ABC_SHARED.FF_CDA_JSON )
    , table(flatten(INPUT => parse_json($1))) manifesttable )  `;
    var stmt1 = snowflake.createStatement({ sqlText: query1 });
    var result1 = stmt1.execute();
    var query2 = ` INSERT INTO ${DB_NAME}.CDA_ABC_SHARED.${xcenter_nm}_CDA_AUDIT_METRIC_RAW_ABC SELECT upper(CDA_RAW_TABLE_NAME) , SRC_ROW_COUNT , row_count tgt_raw_count , PARQUET_WRITE_TIMESTAMP ,SCHEMAHISTORY,
    current_timestamp target_load_timestamp , SRC_ROW_COUNT - tgt_raw_count audit_diff , (audit_diff*100)/SRC_ROW_COUNT AS audit_diff_percent FROM ${DB_NAME}.CDA_ABC_SHARED.src_manifest src INNER JOIN
    ${DB_NAME}.information_schema.tables T ON upper(table_name) = upper(CDA_RAW_TABLE_NAME) WHERE T.table_schema = '${SCHEMA_NAME}'   `;
    var stmt2 = snowflake.createStatement({ sqlText: query2 });
    var result2 = stmt2.execute();
  var abc_diff_cmd = `SELECT audit_diff, CDA_RAW_TABLE_NAME FROM (SELECT * FROM ${DB_NAME}.CDA_ABC_SHARED.${xcenter_nm}_CDA_AUDIT_METRIC_RAW_ABC qualify ROW_NUMBER () OVER (PARTITION BY CDA_RAW_TABLE_NAME ORDER BY TARGET_LOAD_TIMESTAMP desc) = 1) WHERE
        audit_diff <> 0 AND CDA_RAW_TABLE_NAME <> 'HEARTBEAT'  AND AUDIT_DIFF_PERCENT > ${abc_threshold_percent}  ORDER BY 1 DESC `;
  var abc_diff_stmt = snowflake.createStatement({sqlText: abc_diff_cmd});
  var resultSet = abc_diff_stmt.execute();
  if (!resultSet.next()) {
    return "Success: Latest src/tgt counts inserted and counts balanced ";
  } else {
    var resultArray = [];
    do {
      var audit_diff = resultSet.getColumnValue(1);
      var CDA_RAW_TABLE_NAME = resultSet.getColumnValue(2);
      resultArray.push(CDA_RAW_TABLE_NAME + " audit_diff: " + audit_diff );
    } while (resultSet.next());
    var errorMessage = " - Audit Diff Result set is not empty. Manifest Row Counts not matching with RAW table counts for " + resultArray.length + " tables.  Top 3 tables listed - \n"
    var firstTenElements = resultArray.slice(0, 3 );
// 20240131 Chai adding this since aws opcon api has a limitaion of property name 3999 characters in the middleware API lambda and also truncation in API bridge
    errorMessage += firstTenElements.join("\n");
    errorMessage += ". \n For the full list, execute the query - " + abc_diff_cmd + "\n" ;
    throw errorMessage;
  }
} catch (err) {
  throw 'Error: ' + err;
// double quotes causing opcon job hung from opcon api-bridge. adjusted to match list of replacechars in response lambda
}
$$;
-- 5th Stored Procedure
USE SCHEMA CDA_ABC_SHARED ;
CREATE OR REPLACE PROCEDURE CDA_ABC_SHARED.SP_MONITOR_CDA_TASKS (DBNAME STRING, SCHEMANAME STRING)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
try {
  var curr_loop_cnt = 1;
  var max_loop_cnt = 4;
  var max_sleep_time = 300;
  var loop_ind = 'Y';
  var raise_no_task_failure_exception = 'N';
  var raise_failure_exception = 'N';
  
  
    var stmt = snowflake.createStatement( {
    sqlText: `select  REPLACE(SUBSTR('${DBNAME}',1,4),'_','') ENV, '${DBNAME}'||'.CDA_ABC_SHARED.'||upper(substr('${SCHEMANAME}',5,2))||'_CDA_LOGGING' LOGGING_TBL, '${DBNAME}'||'.INFORMATION_SCHEMA.TABLES' INFOR_SCHM_TBL`});
    var resultSet = stmt.execute(); 
    resultSet.next(); 
    var env_nm = resultSet.getColumnValue('ENV'); 
  var log_tbl_nm = resultSet.getColumnValue('LOGGING_TBL'); 
  var infor_schm_tbl_nm = resultSet.getColumnValue('INFOR_SCHM_TBL'); 
  
  var logResultSet = snowflake.execute({
    sqlText: `
    SELECT event_timestamp EVENT_TIMESTAMP from ${log_tbl_nm} where LOG_INFO:SP_NAME='SP_EXECUTE_ALL_RAW_TASKS' qualify ROW_NUMBER() OVER(PARTITION BY LOG_INFO:SP_NAME ORDER BY event_timestamp DESC)=1
    `});
  logResultSet.next();
  var tasks_start_time = logResultSet.getColumnValueAsString('EVENT_TIMESTAMP');
  
  snowflake.execute({ 
  sqlText: `
    INSERT INTO  ${log_tbl_nm} (LOG_INFO) SELECT TO_VARIANT(PARSE_JSON('{"SP_NAME": "SP_MONITOR_CDA_TASKS","ENV":"${env_nm}","STATUS": "STARTED"}'))
  ` });
  
   var qryResultSet = snowflake.execute({
    sqlText: `
    select count(distinct table_name) TAB_CNT from ${DBNAME}.INFORMATION_SCHEMA.TABLES where table_catalog='${DBNAME}' and table_schema='${SCHEMANAME}' and table_type='BASE TABLE' and is_temporary='NO' and table_name<>'HEARTBEAT'
    `});
  qryResultSet.next();
  var tot_tab_cnt = qryResultSet.getColumnValue('TAB_CNT'); 

  while (loop_ind == 'Y' && curr_loop_cnt <= max_loop_cnt) {
    var qryResultSet = snowflake.execute({
      sqlText: `
        SELECT COUNT(1) TASK_CNT FROM (SELECT name FROM TABLE(${DBNAME}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START=> TO_TIMESTAMP_LTZ('${tasks_start_time}'),RESULT_LIMIT => 10000)) WHERE database_name='${DBNAME}' AND schema_name ='${SCHEMANAME}' AND state <> 'SCHEDULED' qualify ROW_NUMBER() OVER(PARTITION BY name ORDER BY query_start_time DESC, completed_time DESC)=1)
      `});
    qryResultSet.next();
    var tot_task_cnt = qryResultSet.getColumnValue('TASK_CNT');
    
    var qryResultSet = snowflake.execute({
      sqlText: `
        SELECT name,state,error_code,error_message FROM TABLE(${DBNAME}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START=>TO_TIMESTAMP_LTZ('${tasks_start_time}'),RESULT_LIMIT => 10000)) WHERE database_name='${DBNAME}' AND schema_name ='${SCHEMANAME}' AND state <> 'SCHEDULED' qualify ROW_NUMBER() OVER(PARTITION BY name ORDER BY query_start_time DESC, completed_time DESC)=1
      `});
    
    var rs_row_cnt = qryResultSet.getRowCount();
    qryResultSet.next();
    
    
    for (var i = 1; i <= rs_row_cnt; i++){
    
      var task_name = qryResultSet.getColumnValue(1);
      var task_state = qryResultSet.getColumnValue(2);
      var task_error_code = qryResultSet.getColumnValue(3);
      var task_error_message = qryResultSet.getColumnValue(4);
            
      if (tot_task_cnt === 0 || !tot_task_cnt ) {
        raise_no_task_failure_exception = 'Y';
        loop_ind = 'N';
        break;
      } else if (tot_tab_cnt > tot_task_cnt) {
        break;
      } else if (task_state === 'FAILED' || task_state === 'FAILED_AND_AUTO_SUSPENDED') {
        if (task_error_code != '110000') {
          raise_failure_exception = 'Y';
          loop_ind = 'N';       
          break;
        } else if (task_error_code === '110000') {
          
          var failedTaskResultSet = snowflake.execute({
            sqlText: `
              SELECT name FROM TABLE(${DBNAME}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START=>TO_TIMESTAMP_LTZ('${tasks_start_time}'),RESULT_LIMIT => 10000,ERROR_ONLY => TRUE)) WHERE database_name='${DBNAME}' AND schema_name ='${SCHEMANAME}' AND state IN ('FAILED','FAILED_AND_AUTO_SUSPENDED') AND error_code ='110000' qualify ROW_NUMBER() OVER(PARTITION BY name ORDER BY query_start_time DESC, completed_time DESC)=1
            `});
          
          var failed_task_array = [];
          
          while (failedTaskResultSet.next()) {
            failed_task_array.push(failedTaskResultSet.getColumnValue(1));
          }
          
          for (var i = 0 ; i < failed_task_array.length; i++) {
            var failedTaskName = failed_task_array[i];
            snowflake.execute({ sqlText: `EXECUTE TASK ${DBNAME}.${SCHEMANAME}.${failedTaskName}` });
          }
          break;
        }
      } else if (task_state === 'EXECUTING') {
        break;
      }
      
      qryResultSet.next();
    }
    
    if (tot_tab_cnt <= tot_task_cnt ) {
      loop_ind = 'N';
    }
    
    if (loop_ind === 'Y') {
      curr_loop_cnt += 1;
      snowflake.execute({ sqlText: `SELECT SYSTEM$WAIT(${max_sleep_time},'SECONDS')` });
    }
  }
  
  if (raise_failure_exception === 'Y') {
    var errorMessage = "CDA task has failed. Task name: " + task_name ;
    throw errorMessage;
  } else if (raise_no_task_failure_exception === 'Y') {
    var errorMessage = "CDA tasks have not started";
    throw errorMessage;
  }
  if (tot_tab_cnt > tot_task_cnt && curr_loop_cnt === max_loop_cnt + 1) {
    var errorMessage = "Few CDA Tasks are long running.";
    throw errorMessage;
  } else if (tot_tab_cnt <= tot_task_cnt) {
    var successMessage = "CDA Tasks completed successfully.";
    return successMessage;
  }
} catch (err) {
  throw 'Execution Error: ' + err;
}
$$
;
-- 6th Stored procedure
use schema CDA_ABC_SHARED ;
CREATE OR REPLACE PROCEDURE SP_CREATE_CDA_MERGED_VIEWS( SOURCE_DATABASE STRING , SOURCE_SCHEMA STRING,  TARGET_SCHEMA STRING )
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try {
  var statement = snowflake.createStatement({
    sqlText: `SELECT table_name FROM  ${SOURCE_DATABASE}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}' AND  table_type = 'BASE TABLE' -- AND LAST_ALTERED < '2024-01-09 11:10:11.976'
    AND  table_name not  IN (SELECT table_name FROM  ${SOURCE_DATABASE}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${TARGET_SCHEMA}' AND  table_type = 'VIEW' )
    UNION SELECT table_name FROM  ${SOURCE_DATABASE}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}' AND  table_type = 'BASE TABLE' AND last_ddl > dateadd('day',-1,current_timestamp())  ORDER BY 1 desc `     });
//'gwcbi___seqval'  --      AND table_name IN (SELECT distinct table_name FROM  ${SOURCE_DATABASE}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${SOURCE_SCHEMA}' AND column_name = 'ID'   ) ORDER BY 1 DESC
//Changed from last_altered TO last_ddl
  var ret_completedviews = 'No views to create'
  var resultSet = statement.execute();
  var tables = [];
  while (resultSet.next()) {
    tables.push(resultSet.getColumnValue(1));
  }
  var totalTables = tables.length;
  ret_completedviews = 'Total Views and corresponding table list of ' + totalTables + ' from raw tables --> ' + tables;
    for (var i = 0 ; i < totalTables; i++) {
      var tableName = tables[i];
      var createViewSql = `
        CREATE OR REPLACE VIEW ${SOURCE_DATABASE}.${TARGET_SCHEMA}.${tableName} AS
        SELECT * EXCLUDE (GWCBI___SEQVAL_HEX, GWCBI___LSN, GWCBI___TX_ID, GWCBI___SEQVAL, GWCBI___PAYLOAD_TS_MS, GWCBI___CONNECTOR_TS_MS )
    ,CASE WHEN GWCBI___OPERATION = '1' THEN 'Y' ELSE 'N' END AS ETL_DEL_FL
        FROM ${SOURCE_SCHEMA}.${tableName} -- WHERE createtime <> '1970-01-01 00:00:00.000'
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY lpad(gwcbi___seqval_hex, 32, '0') DESC) = 1
      `;
// LENGTH("publicid") > 0   , GWCBI___OPERATION DELETE records
      statement = snowflake.createStatement({
        sqlText: createViewSql
      });
      statement.execute();
    }
  return 'Success in creation of merge views: ' + ret_completedviews
} catch (err) {
  throw 'Error: Table Name ' +tableName + err.message + ret_completedviews;
}
$$;


