import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from .. import log
from . import client, s3, glue


KB = 1024
MB = KB * KB
GB = MB * KB
TB = GB * KB
COST_PER_BYTE = 5 / TB

QUERY_COMPLETE_STATUS = ['SUCCEEDED', 'FAILED', 'CANCELLED']
CONVERT_MAX_CONCURRENT = 10


def create_client():
    return client.create_client('athena')


# submit query to Athena
def start_query(database_name, query, query_result_location=None, workgroup=None, athena_client=None):
    if workgroup is None and query_result_location is None:
        raise ValueError("Missing output location. Must provide either query_result_location and/or workgroup. "
                         f"Database={database_name}, query={query}")

    kwargs = {}
    if workgroup is not None:
        kwargs['WorkGroup'] = workgroup

    if query_result_location is not None:
        kwargs['ResultConfiguration'] = {'OutputLocation': query_result_location}

    if athena_client is None:
        athena_client = create_client()

    resp = athena_client.start_query_execution(QueryString=query,
                                               QueryExecutionContext={'Database': database_name},
                                               **kwargs)

    return resp['QueryExecutionId']


def get_query_status(query_execution_id, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    return athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']


def batch_get_query_status(query_execution_id_list, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    return athena_client.batch_get_query_execution(QueryExecutionIds=query_execution_id_list)


def _snooze(sleep_lag, total_sleep_time, sleep_lag_max=30, max_sleep_time=None, query_execution_id_list=None):
    # wait progressively longer, up to 30 seconds
    # snooze_lag = (sleep_lag * 2) if sleep_lag < sleep_lag_max else sleep_lag_max
    snooze_lag = (sleep_lag + 1) if sleep_lag < sleep_lag_max else sleep_lag_max

    if max_sleep_time and (total_sleep_time+snooze_lag) > max_sleep_time:
        raise TimeoutError(f"Timed out! Waited {total_sleep_time} seconds for queries to complete, but it is still "
                           f"running. Input queries={query_execution_id_list}")

    time.sleep(snooze_lag)

    return snooze_lag


def wait_for_query(query_execution_id, sleep_lag_max=30, max_sleep_time=None, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    status = "PENDING"
    sleep_lag = 0
    total_sleep_time = 0
    resp = None

    while status not in QUERY_COMPLETE_STATUS:
        resp = get_query_status(query_execution_id, athena_client=athena_client)
        status = resp['Status']['State']

        if status not in QUERY_COMPLETE_STATUS:
            sleep_lag = _snooze(sleep_lag, total_sleep_time,
                                sleep_lag_max=sleep_lag_max,
                                max_sleep_time=max_sleep_time,
                                query_execution_id_list=[query_execution_id])
            total_sleep_time += sleep_lag

    log.get_logger().info(f"Query {query_execution_id} is done. Slept for {total_sleep_time} seconds.")

    return resp


def batch_wait_for_queries(query_execution_id_list, sleep_lag_max=30, max_sleep_time=None, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    sleep_lag = 0
    total_sleep_time = 0
    batch_resp = []

    pending_execution_id_set = set(query_execution_id_list)

    while pending_execution_id_set:
        batch_resp = batch_get_query_status(list(pending_execution_id_set), athena_client)

        if 'QueryExecutions' in batch_resp:
            for resp in batch_resp['QueryExecutions']:
                if 'Status' in resp and 'State' in resp['Status'] \
                        and resp['Status']['State'] in QUERY_COMPLETE_STATUS:
                    batch_resp[resp['QueryExecutionId']] = resp
                    pending_execution_id_set.remove(resp['QueryExecutionId'])

        if 'UnprocessedQueryExecutionIds' in batch_resp:
            for error_resp in batch_resp['UnprocessedQueryExecutionIds']:
                if 'Status' not in error_resp:
                    error_resp['Status'] = {'State': 'FAILED'}
                batch_resp[error_resp['QueryExecutionId']] = error_resp
                pending_execution_id_set.remove(error_resp['QueryExecutionId'])

        if pending_execution_id_set:
            sleep_lag = _snooze(sleep_lag, total_sleep_time,
                                sleep_lag_max=sleep_lag_max,
                                max_sleep_time=max_sleep_time,
                                query_execution_id_list=pending_execution_id_set)
            total_sleep_time += sleep_lag

    log.get_logger().info(f"Batch queries are done. Slept for {total_sleep_time} seconds.")

    return batch_resp


def yield_query_results(query_execution_id, max_results=None, delete_output=False, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    kwargs = {}
    if max_results is not None:
        # add 1 for header row, it is counted as a row in the result set
        kwargs['PaginationConfig'] = {'MaxItems': max_results+1}

    headers = None

    paginator = athena_client.get_paginator('get_query_results')
    page_iterator = paginator.paginate(QueryExecutionId=query_execution_id, **kwargs)

    for page in page_iterator:
        if headers is None:
            headers = [h['Name'] for h in page['ResultSet']['ResultSetMetadata']['ColumnInfo']]

        for result_row in page['ResultSet']['Rows']:
            # ignore header row
            if result_row['Data'][0]['VarCharValue'] != headers[0]:
                yield {
                    headers[idx]: val.get('VarCharValue')
                    for idx, val in enumerate(result_row['Data'])
                }

    # delete output file of athena query
    if delete_output:
        query_resp = get_query_status(query_execution_id, athena_client=athena_client)
        s3.delete_uri(query_resp['ResultConfiguration']['OutputLocation'])


def get_query_results(query_execution_id, max_results=None, athena_client=None, delete_output=None):
    return [row for row in yield_query_results(query_execution_id, max_results=max_results,
                                               athena_client=athena_client, delete_output=delete_output)]


# execute and wait for query
def run_query(database_name, query, query_result_location=None, delete_output=False, workgroup=None,
              athena_client=None):
    logger = log.get_logger()

    query_execution_id = start_query(database_name, query,
                                     query_result_location=query_result_location,
                                     workgroup=workgroup,
                                     athena_client=athena_client)

    # handle error status
    query_resp = wait_for_query(query_execution_id, athena_client=athena_client)

    status = query_resp['Status']['State']
    statistics = query_resp['Statistics']

    logger.info("{}: query_execution_id={}, status={}, Run time={}, scanned bytes={}, cost=${}"
                . format(database_name, query_execution_id, status,
                         statistics['EngineExecutionTimeInMillis'],
                         statistics['DataScannedInBytes'],
                         statistics['DataScannedInBytes'] * COST_PER_BYTE))

    # delete output file of athena query
    if delete_output:
        s3.delete_uri(query_resp['ResultConfiguration']['OutputLocation'])

    if status != 'SUCCEEDED':
        raise ValueError(f"Failed to run query: {query_resp}")

    return query_resp


def get_partitions(database_name, table_name, query_result_location, workgroup=None, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    query = f"show partitions {database_name}.{table_name}"
    query_resp = run_query(database_name, query,
                           query_result_location=query_result_location,
                           workgroup=workgroup)

    query_results = get_query_results(query_resp['QueryExecutionId'],
                                      athena_client=athena_client,
                                      delete_output=True)

    return [value for partition in query_results for value in partition.values()]


def refresh_partitions(database_name, table_name, query_result_location=None, workgroup=None):
    query = f"msck repair table {database_name}.{table_name}"
    return run_query(database_name, query,
                     query_result_location=query_result_location,
                     delete_output=True,
                     workgroup=workgroup)


def run_ctas_query(source_database_name, source_table_name, target_database_name, target_table_name,
                   external_location, data_format, partition_columns=None, bucket_columns=None, bucket_count=None,
                   query_result_location=None, workgroup=None):
    ctas_column_list = _build_ctas_columns(source_database_name, source_table_name, partition_columns)
    with_statement = _build_ctas_with(data_format, external_location, partition_columns, bucket_columns, bucket_count)

    query = "create table {}.{} with ({}) as select {} from {}.{}" . format(target_database_name, target_table_name,
                                                                            with_statement, ','.join(ctas_column_list),
                                                                            source_database_name, source_table_name)

    # delete output file of athena query since CTAS doesn't produce any useful results
    return run_query(target_database_name, query,
                     query_result_location=query_result_location,
                     delete_output=True,
                     workgroup=workgroup)


# Returns list of columns to select from source table in CTAS statement.
# If table is partitioned, partition columns must be moved to the end of the list.
# If table is not partitioned, column placement is not important so return "*" instead of full column list.
def _build_ctas_columns(database_name, table_name, partition_columns=None):
    # move partition columns to the end of list
    if partition_columns:
        column_list = list(glue.get_columns(database_name, table_name).keys())
        ctas_column_list = list(set(column_list) - set(partition_columns))
        ctas_column_list.extend(partition_columns)

        # surround column names with double-quotes
        return [f'"{col_name}"' for col_name in ctas_column_list]

    # select all columns for tables that are not partitioned
    return ['*']


def _build_ctas_with(data_format, external_location, partition_columns=None, bucket_columns=None, bucket_count=None):
    with_conditions = [
        f"format='{data_format}'",
        f"external_location='{external_location}'",
        "{}_compression='{}'" . format(data_format, glue.CLASSIFICATION_TABLE_MAP[data_format]['compressionType'])
    ]

    if partition_columns:
        esc_partitioned_by = [f"'{col_name}'" for col_name in partition_columns]
        with_conditions.append("partitioned_by=ARRAY[{}]" . format(','.join(esc_partitioned_by)))

    if bucket_columns:
        if bucket_count is None:
            raise ValueError("Must specify bucket_count if bucket_columns are provided!")

        esc_bucketed_by = [f"'{col_name}'" for col_name in bucket_columns]
        with_conditions.append("bucketed_by=ARRAY[{}]" . format(','.join(esc_bucketed_by)))
        with_conditions.append(f"bucket_count={bucket_count}")

    return ','.join(with_conditions)


def get_work_group(workgroup_name, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    response = athena_client.get_work_group(WorkGroup=workgroup_name)
    if 'WorkGroup' in response:
        workgroup_response = response['WorkGroup']
        if 'Name' in workgroup_response and workgroup_response['Name'] == workgroup_name \
                and 'State' in workgroup_response and workgroup_response['State'] == 'ENABLED':
            return workgroup_response


def create_work_group(workgroup_name, output_location, description=None, encryption_option='SSE_S3',
                      enforce_workgroup_configuration=False, publish_cloudwatch_metics=True, athena_client=None):
    if athena_client is None:
        athena_client = create_client()

    try:
        if get_work_group(workgroup_name, athena_client=athena_client):
            return
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidRequestException' \
                and "is not found." in e.response['Error']['Message']:
            pass
        else:
            raise e

    if not description:
        description = workgroup_name

    try:
        athena_client.create_work_group(
            Name=workgroup_name,
            Description=description,
            Configuration={
                'ResultConfiguration': {
                    'OutputLocation': output_location,
                    'EncryptionConfiguration': {'EncryptionOption': encryption_option}
                },
                'EnforceWorkGroupConfiguration': enforce_workgroup_configuration,
                'PublishCloudWatchMetricsEnabled': publish_cloudwatch_metics
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidRequestException' \
                and 'is already created' in e.response['Error']['Message']:
            pass
        else:
            raise e

    if not get_work_group(workgroup_name, athena_client=athena_client):
        raise ValueError(f"Failed to create workgroup {workgroup_name}!")


def convert_database(source_database_name, target_database_name, target_classification, target_s3_uri,
                     table_config=None, default_partition_by=None, query_result_location=None, workgroup=None,
                     max_concurrent=CONVERT_MAX_CONCURRENT):
    logger = log.get_logger()
    logger.info(f"convert database {source_database_name} to {target_database_name} in format {target_classification}, "
                f"target_s3_uri={target_s3_uri}, workgroup={workgroup}")

    table_partition = {}
    if table_config is not None:
        for table_name in table_config.keys():
            if 'partition_by' in table_config[table_name]:
                table_partition[table_name] = table_config[table_name]['partition_by']

    if glue.insert_database(target_database_name):
        logger.info(f"Created database {target_database_name}")

    total_data_scanned = 0
    table_count = 0
    convert_exception = None

    # get list of glue tables in source database
    table_list = glue.get_table_list(source_database_name)

    with ThreadPoolExecutor(max_workers=min(max_concurrent, len(table_list))) as executor:
        futures = {}

        for table_name in table_list:
            partition_columns = table_partition[table_name] if table_name in table_partition else default_partition_by
            futures[executor.submit(convert_table, source_database_name, target_database_name, table_name,
                                    target_classification, target_s3_uri, query_result_location,
                                    partition_columns, workgroup=workgroup)] = table_name

        for future in as_completed(futures):
            try:
                data_scanned = future.result()
                logger.info(f"{futures[future]}: Data scanned={data_scanned}")
                total_data_scanned += data_scanned
                table_count += 1
            except Exception as e:
                logger.exception("Failed to convert table {}.{}: {}".format(target_database_name, futures[future], e))
                convert_exception = e
            finally:
                del futures[future]

    logger.info("Converted database {}, {} tables. Total data scanned={}KB, cost=${}"
                . format(target_database_name, table_count, total_data_scanned/KB, total_data_scanned*COST_PER_BYTE))

    # throw last captured exception after all tables have been converted
    if convert_exception is not None:
        raise convert_exception


def convert_table(source_database_name, target_database_name, table_name, target_classification,
                  target_s3_uri, partition_columns=None, query_result_location=None, workgroup=None):
    logger = log.get_logger()
    logger.info(f"Converting table {target_database_name}.{table_name} in {target_classification} format")

    ctas_table_name = f"ctas_{table_name}"

    target_table_s3_uri = os.path.join(target_s3_uri, table_name, "")
    ctas_target_table_s3_uri = os.path.join(target_s3_uri, "ctas", ctas_table_name, "")

    try:
        # only include partition keys that exist as columns in source table
        target_partition_columns = glue.build_partition_columns(source_database_name, table_name, partition_columns) \
            if partition_columns is not None else None

        # convert to a temporary table using Athena CTAS
        ctas_resp = run_ctas_query(source_database_name, table_name, target_database_name, ctas_table_name,
                                   ctas_target_table_s3_uri, target_classification,
                                   partition_columns=target_partition_columns,
                                   query_result_location=query_result_location,
                                   workgroup=workgroup)

        # copy converted files from temporary table location to target table location
        copy_flag = glue.copy_table(target_database_name, target_database_name, ctas_table_name, table_name,
                                    target_table_s3_uri, target_classification, target_partition_columns,
                                    copy_partitions_flag=True, copy_files_flag=True)
        logger.info(f"{target_database_name}.{table_name}: Copied table {copy_flag}")

        # use Athena to recover new partitions
        # if target_partition_columns:
        #    refresh_partitions(target_database_name, table_name, query_result_location)

        logger.info(f"Converted {source_database_name}.{table_name} to {target_table_s3_uri}")

        return ctas_resp['Statistics']['DataScannedInBytes']\
            if 'Statistics' in ctas_resp and 'DataScannedInBytes' in ctas_resp['Statistics']\
            else 0
    finally:
        # delete converted table and files
        logger.info(f"{target_database_name}.{table_name}: will delete table {ctas_table_name}")
        glue.delete_table(target_database_name, ctas_table_name, delete_files=False)
