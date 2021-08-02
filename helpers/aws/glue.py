from . import client, s3
from .. import log
from botocore.exceptions import ClientError, ParamValidationError


CLASSIFICATION_TABLE_MAP = {
    "csv": {
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
            "Parameters": {"separatorChar": ",", "serialization.format": "1"}
        },
        "compressionType": "none",
        "Parameters": {
            'areColumnsQuoted': 'false',
            'skip.header.line.count': '1'
        }
    },
    "json": {
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hive.hcatalog.data.JsonSerDe",
            "Parameters": {"serialization.format": "1"}
        },
        "compressionType": "none"
    },
    "parquet": {
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"}
        },
        "compressionType": "snappy"
    }
}


def create_client():
    return client.create_client('glue')


# Returns database definition if exists
def get_database(database_name, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    try:
        db = glue_client.get_database(Name=database_name)

        if db is not None and 'Database' in db and 'Name' in db['Database'] \
                and db['Database']['Name'] == database_name:
            return db
    except glue_client.exceptions.EntityNotFoundException:
        pass
    except (ClientError, ParamValidationError) as e:
        raise ValueError("Glue Error. Failed to get database {}: {}".format(database_name, e)) from e

    return None


def insert_database(database_name):
    created = False

    glue_client = create_client()

    if get_database(database_name, glue_client=glue_client) is None:
        try:
            glue_client.create_database(DatabaseInput={'Name': database_name})
            created = True
        except glue_client.exceptions.AlreadyExistsException:
            pass

        # validate using a new glue connection that database was inserted successfully
        if get_database(database_name) is None:
            raise ValueError(f"Failed to create database {database_name}")

    return created


# Returns list of table names for a database
def get_table_list(database_name, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    paginator_iterator = glue_client.get_paginator('get_tables').paginate(DatabaseName=database_name)

    table_list = []

    for response in paginator_iterator:
        if response is not None and 'TableList' in response:
            table_list.extend([table['Name'] for table in response['TableList']])

    return table_list


# Returns table definition if exists
def get_table(database_name, table_name, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    try:
        table = glue_client.get_table(DatabaseName=database_name, Name=table_name)

        if table is not None and GlueTable.parse_name(table) == table_name:
            return table
    except glue_client.exceptions.EntityNotFoundException:
        pass

    return None


def insert_table(database_name, table_input, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    glue_client.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )


def update_table(database_name, table_input, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    glue_client.update_table(
        DatabaseName=database_name,
        TableInput=table_input
    )


def delete_table(database_name, table_name, glue_client=None, delete_files=False):
    if glue_client is None:
        glue_client = create_client()

    table_definition = get_table(database_name, table_name, glue_client=glue_client)

    # ignore non-existent table
    if table_definition is None:
        return

    # delete partitions to free up resources immediately,
    # otherwise, partitions are left orphaned and AWS will eventually delete them asynchronously
    delete_all_partitions(database_name, table_name, delete_files)

    # delete table files
    if delete_files:
        data_location_uri = GlueTable.get_location(table_definition)
        if data_location_uri is not None:
            s3.delete_uri(data_location_uri)

    # finally delete table
    glue_client.delete_table(DatabaseName=database_name, Name=table_name)


def get_columns(database_name, table_name, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    column_dict = {}

    try:
        table = glue_client.get_table(DatabaseName=database_name, Name=table_name)

        if table is not None:
            columns = GlueTable.parse_columns(table) + GlueTable.parse_partition_keys(table)
            # do not include comments
            column_dict = {col['Name']: {'Name': col['Name'], 'Type': col['Type']} for col in columns}
    except glue_client.exceptions.EntityNotFoundException:
        pass

    return column_dict


def get_partition_columns(database_name, table_name, glue_client=None):
    table_definition = get_table(database_name, table_name, glue_client=glue_client)

    if table_definition is None:
        raise ValueError(f"Cannot parse partition keys from table {table_name}.  "
                         f"Table does not exist in database {database_name}.")

    partition_keys = GlueTable.parse_partition_keys(table_definition)

    # do not return Comment
    return [{'Name': col['Name'], 'Type': col['Type']} for col in partition_keys]


def build_partition_columns(database_name, table_name, default_partition_columns):
    columns = get_columns(database_name, table_name)
    return [col_name for col_name in default_partition_columns if col_name in columns]


def yield_partitions(database_name, table_name, glue_client=None, max_results=None):
    if glue_client is None:
        glue_client = create_client()

    kwargs = {}
    if max_results is not None:
        kwargs['PaginationConfig'] = {'MaxItems': max_results}

    paginator = glue_client.get_paginator('get_partitions')
    paginator_iterator = paginator.paginate(DatabaseName=database_name, TableName=table_name, **kwargs)

    for response in paginator_iterator:
        if 'Partitions' in response:
            for response_partition in response['Partitions']:
                yield {'Values': response_partition['Values'],
                       'Location': response_partition['StorageDescriptor']['Location'],
                       'StorageDescriptor': response_partition['StorageDescriptor']}


def get_all_partition_values(database_name, table_name, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    partition_list = yield_partitions(database_name, table_name, glue_client=glue_client)
    return [partition['Values'] for partition in partition_list]


# Creates one or more partitions to a table
def insert_partitions(database_name, table_name, partition_input_list, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    create_kwargs = {
        "DatabaseName": database_name,
        "TableName": table_name
    }

    # use batch mode to add more than one partition
    if len(partition_input_list) == 1:
        return glue_client.create_partition(PartitionInput=partition_input_list[0], **create_kwargs)
    else:
        return glue_client.batch_create_partition(PartitionInputList=partition_input_list, **create_kwargs)


def delete_partitions(database_name, table_name, partitions_to_delete, glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    return glue_client.batch_delete_partition(DatabaseName=database_name,
                                              TableName=table_name,
                                              PartitionsToDelete=partitions_to_delete)


def delete_all_partitions(database_name, table_name, delete_files=False, glue_client=None):
    partitions = yield_partitions(database_name, table_name, glue_client=glue_client)

    if delete_files:
        s3.delete_uri_list([partition['Location'] for partition in partitions])

    partitions_to_delete = [{'Values': partition['Values']} for partition in partitions]
    if partitions_to_delete:
        delete_partitions(database_name, table_name, partitions_to_delete, glue_client=glue_client)

    return len(partitions_to_delete)


def get_location_uri(database_name, table_name, glue_client=None):
    table_definition = get_table(database_name, table_name, glue_client=glue_client)

    if table_definition is None:
        raise ValueError(f"Cannot find table {table_name} in database {database_name}.")

    return GlueTable.get_location(table_definition)


# Copy source table definition (columns and partition keys) to target table in the catalog.
# If target table already exists, source columns and partitions are merged into existing definition.
# If copy_partitions_flag is True, partition metadata is copied to target table and updated with the target S3 location.
# If copy_files_flag is True, S3 data files are copied from source location to target location.
def copy_table(source_database_name, target_database_name, source_table_name, target_table_name,
               target_location_uri, classification, target_partition_columns=None,
               copy_partitions_flag=True, copy_files_flag=True, glue_client=None):
    logger = log.get_logger()
    logger.info(f"Copying table {source_database_name}.{source_table_name} to "
                f"{target_database_name}.{target_table_name}")

    if glue_client is None:
        glue_client = create_client()

    column_list = list(get_columns(source_database_name, source_table_name, glue_client=glue_client).values())

    updated_flag = create_table(target_database_name, target_table_name, column_list, target_location_uri,
                                classification, partition_columns=target_partition_columns,
                                glue_client=glue_client)

    # copy source S3 data files to target location
    file_count = 0
    if copy_files_flag:
        source_location_uri = get_location_uri(source_database_name, source_table_name, glue_client=glue_client)
        file_count = s3.copy_uri(source_location_uri, target_location_uri)

    # copy partition metadata
    partition_count = 0
    if copy_partitions_flag:
        partition_count = copy_partitions(source_database_name, target_database_name,
                                          source_table_name, target_table_name, glue_client=glue_client)

    logger.info(f"Created table {target_database_name}.{target_table_name}, changed={updated_flag}; "
                f"copied partitions={partition_count}, files={file_count}")

    return updated_flag


# Copy partitions from source table to target table.
# Data location is updated to reflect target table location on S3.
# Actual S3 data files that are associated with the partitions are not copied.
def copy_partitions(source_database_name, target_database_name, source_table_name, target_table_name,
                    glue_client=None):
    if glue_client is None:
        glue_client = create_client()

    # table is not partitioned
    target_partition_columns = get_partition_columns(target_database_name, target_table_name, glue_client=glue_client)
    if not target_partition_columns:
        return 0

    # only copy partitions if the partition keys match
    source_partition_columns = get_partition_columns(source_database_name, source_table_name, glue_client=glue_client)
    if source_partition_columns != target_partition_columns:
        return 0

    # get existing partitions values in target table
    target_partition_values = get_all_partition_values(target_database_name, target_table_name, glue_client=glue_client)

    source_location_uri = get_location_uri(source_database_name, source_table_name, glue_client=glue_client)
    target_location_uri = get_location_uri(target_database_name, target_table_name, glue_client=glue_client)

    partition_list = yield_partitions(source_database_name, source_table_name)

    # Filter out partition values that already exist in target table
    # Replace original location with target table location
    # Only include Name and Type in column list, exclude other fields like 'Comment'
    partition_input_list = [
        {
            'Values': partition['Values'],
            'StorageDescriptor': {
                'Columns': [{'Name': col['Name'], 'Type': col['Type']}
                            for col in partition['StorageDescriptor']['Columns']],
                'Location': partition['Location'].replace(source_location_uri, target_location_uri),
                'InputFormat': partition['StorageDescriptor']['InputFormat'],
                'OutputFormat': partition['StorageDescriptor']['OutputFormat'],
                'SerdeInfo': partition['StorageDescriptor']['SerdeInfo']
            }
        }
        for partition in partition_list
        if partition['Values'] not in target_partition_values
    ]

    if partition_input_list:
        insert_partitions(target_database_name, target_table_name, partition_input_list, glue_client=glue_client)

    return len(partition_input_list)


def create_table(database_name, table_name, column_list, s3_uri, classification,
                 partition_columns=None, overwrite=False, glue_client=None):
    if not column_list:
        return None

    table_model = GlueTable(database_name, table_name, column_list, classification, s3_uri,
                            partition_columns=partition_columns)

    changed = False

    db_table = get_table(database_name, table_name, glue_client=glue_client)

    # new table
    if db_table is None:
        insert_table(database_name, table_model.build_table_input(), glue_client=glue_client)
        changed = True
    else:
        if overwrite:
            # replace entire table if definition is different
            can_update = not table_model.equals(db_table)
        else:
            # merge in existing table definition if overwrite mode is not set
            can_update = table_model.append_table_definition(db_table)

        if can_update:
            update_table(database_name, table_model.build_table_input(), glue_client=glue_client)
            changed = True

    return changed


class GlueTable(object):
    def __init__(self, database_name, table_name, column_list, classification, location,
                 partition_keys=None, partition_columns=None):
        if classification not in CLASSIFICATION_TABLE_MAP:
            raise ValueError(f"Unknown classification {classification}.  Supported values are: "
                             f"{list(CLASSIFICATION_TABLE_MAP.keys())}")

        self.database_name = database_name
        self.table_name = table_name
        self.column_list = column_list
        self.classification = classification
        self.location = location
        self.partitions = []
        self.partition_keys = partition_keys if partition_keys else []

        if not partition_keys and partition_columns:
            self.partition_keys = GlueTable.build_partition_keys_from_names(self.column_list, partition_columns)

        # remove partition keys from column list
        if self.partition_keys:
            self.column_list = GlueTable._remove_from_list(self.column_list, self.partition_keys)

    # returns True if table definition has the same partition keys and columns
    def equals(self, table_definition):
        changed = False

        if self.partition_keys != GlueTable.parse_partition_keys(table_definition):
            changed = True
        elif not GlueTable.columns_equal(self.column_list, GlueTable.parse_columns(table_definition)):
            changed = True

        return not changed

    # merge columns and partition keys from table definition
    # returns True if merged results changed
    def append_table_definition(self, table_definition):
        changed = self._append_partition_keys(table_definition)
        changed = self._append_column_list(table_definition) or changed

        return changed

    def _append_partition_keys(self, table_definition):
        changed = False

        old_partition_keys = GlueTable.parse_partition_keys(table_definition)

        # merge partition_keys on top of old table_definition
        self.partition_keys = GlueTable._append_columns(old_partition_keys, self.partition_keys)

        if self.partition_keys != old_partition_keys:
            changed = True

        return changed

    def _append_column_list(self, table_definition):
        changed = False

        old_column_list = GlueTable.parse_columns(table_definition)

        # merge column_list on top of old table_definition
        self.column_list = GlueTable._append_columns(old_column_list, self.column_list)

        # make sure to remove partition keys from column list
        self.column_list = GlueTable._remove_from_list(self.column_list, self.partition_keys)

        if not GlueTable.columns_equal(self.column_list, old_column_list):
            changed = True

        return changed

    @staticmethod
    def build_partition_keys_from_names(column_list, partition_columns):
        column_dict = {col['Name']: col for col in column_list}

        # only include partition columns that are found in column list
        missing_partition_cols = set(partition_columns) - set(column_dict)
        actual_partition_cols = [col for col in partition_columns if col not in missing_partition_cols]


        # add new partition keys to column list
        # new_partition_columns = set(partition_columns) - set(column_dict)
        # for new_partition_col in new_partition_columns:
        #    column_dict[new_partition_col] = {"Name": new_partition_col, "Type": "string"}

        # convert partition_by from a list of string to a list of dictionary {Name: <name>, Type: <type>}
        partition_keys = [column_dict[key_name] for key_name in actual_partition_cols]

        return partition_keys

    @staticmethod
    def _remove_from_list(column_list, exclude_list):
        exclude_names = [col['Name'] for col in exclude_list]
        return [col for col in column_list if col['Name'] not in exclude_names]

    @staticmethod
    def _append_columns(old_column_list, new_column_list):
        # create a lookup dictionary from the new column list
        new_column_dict = {col['Name']: col for col in new_column_list}

        # update existing entries
        merged_column_list = [new_column_dict[col['Name']] if col['Name'] in new_column_dict
                              else col for col in old_column_list]

        # append new entries
        old_column_names = [col['Name'] for col in old_column_list]
        merged_column_list.extend([col for col in new_column_list if col['Name'] not in old_column_names])

        return merged_column_list

    def build_table_input(self):
        table_map = CLASSIFICATION_TABLE_MAP.get(self.classification)

        table_input = {
            "Name": self.table_name,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Columns": self.column_list,
                "Location": self.location,
                "InputFormat": table_map.get("InputFormat"),
                "OutputFormat": table_map.get("OutputFormat"),
                "SerdeInfo": table_map.get("SerdeInfo")
            },
            "PartitionKeys": self.partition_keys,
            "Parameters": {
                "classification": self.classification,
                "compressionType": table_map.get("compressionType"),
                "EXTERNAL": "TRUE",
                "typeOfData": "file"
            }
        }

        if "Parameters" in table_map:
            table_input["Parameters"].update(table_map.get("Parameters"))

        return table_input

    def build_partition_input(self):
        table_map = CLASSIFICATION_TABLE_MAP.get(self.classification)

        partition_input_list = []
        for partition in self.partitions:
            partition_input_list.append(self.build_partition(table_map,
                                                             partition['value'],
                                                             partition['location']))

        return partition_input_list

    def build_partition(self, table_map, value, location):
        partition_input = {
            "Values": [value],
            "StorageDescriptor": {
                "Columns": self.column_list,
                "Location": location,
                "InputFormat": table_map.get("InputFormat"),
                "OutputFormat": table_map.get("OutputFormat"),
                "SerdeInfo": table_map.get("SerdeInfo")
            },
            "Parameters": {
                "classification": self.classification,
                "compressionType": table_map.get("compressionType"),
                "EXTERNAL": "TRUE",
                "typeOfData": "file"
            }
        }

        if "Parameters" in table_map:
            partition_input["Parameters"].update(table_map.get("Parameters"))

        return partition_input

    @staticmethod
    def parse_name(table_definition):
        if 'Table' in table_definition and 'Name' in table_definition['Table']:
            return table_definition['Table']['Name']

    @staticmethod
    def parse_columns(table_definition):
        if 'Table' in table_definition and 'StorageDescriptor' in table_definition['Table']\
                and 'Columns' in table_definition['Table']['StorageDescriptor']:
            return table_definition['Table']['StorageDescriptor']['Columns']

        return []

    @staticmethod
    def get_location(table_definition):
        if 'Table' in table_definition and 'StorageDescriptor' in table_definition['Table'] \
                and 'Location' in table_definition['Table']['StorageDescriptor']:
            return table_definition['Table']['StorageDescriptor']['Location']

    @staticmethod
    def parse_partition_keys(table_definition):
        if 'Table' in table_definition and 'PartitionKeys' in table_definition['Table']:
            return table_definition['Table']['PartitionKeys']

        return []

    @staticmethod
    def sort_columns(column_list):
        return sorted(column_list, key=lambda col: col['Name'])

    @staticmethod
    def columns_equal(column_list1, column_list2):
        if column_list1 is None and column_list2 is None:
            return True
        elif column_list1 is not None and column_list2 is not None:
            # first check length
            if len(column_list1) != len(column_list2):
                return False

            # then do deep compare
            return GlueTable.sort_columns(column_list1) == GlueTable.sort_columns(column_list2)
        else:
            return False
