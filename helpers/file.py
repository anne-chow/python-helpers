import csv
import gzip
import os
import shutil
import traceback
import zipfile
from pathlib import Path
import simplejson as json


def local_path_exists(file_path):
    return Path(file_path).exists()


def ensure_local_path_exists(file_path):
    Path(file_path).mkdir(parents=True, exist_ok=True)


def format_folder_name(file_path):
    return file_path if len(file_path) == 0 or file_path.endswith('/') else ''.join([file_path, '/'])


# get list of local files recursively
def list_files_recursively(file_path, prefix=None, suffix=None):
    if prefix is None:
        prefix = ""
    if suffix is None:
        suffix = ""

    return [os.path.join(parent, name)
            for (parent, subdirs, files) in os.walk(file_path)
            for name in files
            if name.startswith(prefix) and name.endswith(suffix)]


def get_file_size(file_name):
    return os.stat(file_name).st_size


def peek_file_line(fh):
    # get current position
    pos = fh.tell()
    # read a line
    line = fh.readline()
    # go back to original position
    fh.seek(pos)

    return line


# delete file or directory from local file system
def delete_local_path(file_path):
    if file_path is not None:
        try:
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                os.remove(file_path)
        except OSError as e:
            if os.path.exists(file_path):
                print("Failed to delete local path {}: {}".format(file_path, e))
                traceback.print_exc()


# Returns path to compressed local file
def compress_file(input_path, compression_type='gzip'):
    if compression_type == 'gzip':
        return gzip_file(input_path)
    elif compression_type == 'zip':
        return zip_file(input_path)
    else:
        raise ValueError(f"Unsupported {compression_type} compression type.")


# Support zipfile or gzip to decompress a local file
# Returns path to list of decompressed local files
def decompress_file(input_path, max_lines_per_file=None):
    if zipfile.is_zipfile(input_path):
        return unzip_file(input_path)
    else:
        return ungzip_file(input_path, max_lines_per_file)


# decorator to handle compressed files
# intermediate files are deleted immediately to release tmp disk space
def compressed_file_handler(func):
    def decorator(file_path, *args, **kwargs):
        decompressed_file = None

        try:
            # decompress file to a local temp file
            decompressed_file = decompress_file(file_path)[0]

            # delete original file
            delete_local_path(file_path)

            # use decompressed temp file as input to decorated function
            output_path = func(decompressed_file, *args, **kwargs)
        finally:
            if decompressed_file is not None:
                delete_local_path(decompressed_file)

        compressed_path = None
        if output_path is not None:
            try:
                # compress output file from function
                compressed_path = compress_file(output_path)
            finally:
                delete_local_path(output_path)

        return compressed_path

    return decorator


def zip_file(input_path, compression=zipfile.ZIP_DEFLATED, compress_level=6):
    if compression is None:
        compression = zipfile.ZIP_DEFLATED
    if compress_level is None:
        compress_level = 6

    # add .zip extension
    output_path = f"{input_path}.zip"
    archive_name = os.path.basename(input_path)

    with zipfile.ZipFile(output_path, 'w', compression=compression, compresslevel=compress_level) as f_out:
        f_out.write(input_path, arcname=archive_name)

    return output_path


def unzip_file(input_path):
    output_path = os.path.dirname(input_path)

    if not zipfile.is_zipfile(input_path):
        raise ValueError(f"Cannot unzip file {input_path}.  It is not a zip file.")

    zipped_file = zipfile.ZipFile(input_path)
    zipped_file.extractall(output_path)

    return output_path


# Use gzip to compress a local file
def gzip_file(input_path):
    # add .gz extension
    output_path = f"{input_path}.gz"

    with open(input_path, 'rb') as f_in:
        with gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return output_path


# Use gzip to decompress a local file
# Returns path to list of decompressed local files
def ungzip_file(input_path, max_lines_per_file=None):
    # drop .gz extension
    output_path, _ = os.path.splitext(input_path)

    with gzip.open(input_path, 'rb') as f_in:
        # write content to a single file
        if max_lines_per_file is None or max_lines_per_file == 0:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        else:
            return split_file_obj(f_in, output_path, max_lines_per_file)

    return [output_path]


# Split file to multiple files, each with specified maximum number of lines
# Output files will have a numeric index appended to the original file root, e.g. filename_00000001.txt
def split_file_obj(f_in, output_path, max_lines_per_file):
    if max_lines_per_file < 1:
        raise ValueError("max_lines_per_file must be > 0")

    output_paths = []
    file_ext = ''.join(Path(output_path).suffixes)
    file_root = output_path.rsplit(file_ext, 1)[0] if file_ext else output_path

    file_num = 0
    line_num = 0
    f_out = None

    try:
        for line in f_in:
            if line_num >= max_lines_per_file or f_out is None:
                if f_out is not None:
                    f_out.close()

                output_file = "{}_{:08d}{}" . format(file_root, file_num, file_ext)
                f_out = open(output_file, 'wb')

                output_paths.append(output_file)
                file_num += 1
                line_num = 0

            f_out.write(line)
            line_num += 1
    finally:
        if f_out is not None:
            f_out.close()

    return output_paths


def build_fixed_length(record, record_layout):
    formatted_length = 0
    formatted_result = []

    for rl in record_layout:
        field_name = rl['source_name'] if rl.get('source_name') is not None else rl['name']
        start_position = formatted_length + 1 if rl.get('start_position') is None \
            else rl['start_position']

        # validate start position
        if formatted_length >= start_position:
            if formatted_length == 0:
                raise ValueError(f"Record layout field {field_name} is configured with an invalid start position of "
                                 f"{rl.get('start_position')}! It must be >= 1 in "
                                 f"config.extract_field table.")
            else:
                raise ValueError(f"Record layout {field_name} is configured with an invalid start position. "
                                 f"The config.extract_field table has a position of "
                                 f"{rl.get('start_position')} but the previous field ended at position "
                                 f"{formatted_length}!")

        # validate length
        field_value = str(record.get(field_name)) if record.get(field_name) is not None else ''
        if len(field_value) == 0 and rl.get('default_value') is not None:
            field_value = rl['default_value']

        if rl.get('length') is not None and len(field_value) > rl['length']:
            if rl.get('truncate_flag'):
                field_value = field_value[0:rl['length']]
            else:
                raise ValueError(f"Record layout field {field_name} is configured with a length of "
                                 f"{rl['length']} but the current record exceeds that! It has a "
                                 f"length of {len(field_value)} characters!")

        # pad filler and value
        if start_position > (formatted_length+1):
            filler_length = start_position - formatted_length - 1
            formatted_result.append(' ' * filler_length)
            formatted_length += filler_length

        # left-justified by default
        if rl.get('length') is not None:
            if rl.get('right_justify'):
                formatted_result.append(f"{field_value:>{rl['length']}}")
            else:
                formatted_result.append(f"{field_value:<{rl['length']}}")
        else:
            formatted_result.append(field_value)

        formatted_length += len(formatted_result[-1])

    return ''.join(formatted_result)


def get_csv_file_columns(input_file_path, delimiter=','):
    with open(input_file_path, 'r', newline='') as fh:
        csv_reader = csv.reader(fh, delimiter=delimiter)

        try:
            for line in csv_reader:
                if line:
                    return [col_name for col_name in line if col_name]
        except csv.Error as e:
            raise Exception("Failed to read CSV file {}, line {}: {}"
                            .format(input_file_path, csv_reader.line_num, e)) from e
        except ValueError as e:
            raise ValueError("Failed to read CSV file {}: {}".format(input_file_path, e)) from e


def yield_csv_file_row(input_file_path, delimiter=',', has_header=True, column_mapping={},
                       data_exception_handler=None, data_exception_kwargs=None):
    if not column_mapping:
        column_mapping = {}

    def _csv_row_handle_null(row_data):
        return None if row_data is not None and len(row_data) == 0 else row_data

    with open(input_file_path, 'r', newline='') as fh:
        csv_reader = csv.reader(fh, delimiter=delimiter)

        try:
            line_number = 1
            if has_header:
                header_row = next(csv_reader)
            else:
                first_row = peek_file_line(fh)
                num_of_fields = len(first_row.split(','))
                header_row = [i for i in range(num_of_fields)]

            header_dict = {column_mapping[col_val] if col_val in column_mapping else col_val: col_index
                           for col_index, col_val in enumerate(header_row)}

            for row in csv_reader:
                line_number += 1
                if row:
                    try:
                        yield {field_name: _csv_row_handle_null(row[header_dict[field_name]])
                               for field_name in header_dict}
                    except Exception as row_e:
                        if data_exception_handler:
                            data_exception_handler(line_number, row, row_e, **data_exception_kwargs)
                        else:
                            raise row_e
        except csv.Error as e:
            msg = "Failed to read CSV file {}, line {}".format(input_file_path, csv_reader.line_num)
            e.args = (e.args if e.args else ()) + (msg,)
            raise e
        except ValueError as e:
            raise ValueError("Failed to read CSV file {}: {}".format(input_file_path, e)) from e


def get_json_file_columns(input_file_path):
    try:
        with open(input_file_path, "r") as fh:
            for line in fh.readlines():
                if line:
                    return list(json.loads(line).keys())
    except ValueError as e:
        raise ValueError("Failed to read JSON file {}: {}".format(input_file_path, e)) from e


def yield_json_file_row(input_file_path, data_exception_handler=None, data_exception_kwargs=None):
    line_number = 0

    with open(input_file_path, 'r') as fh:
        try:
            for row in fh.readlines():
                line_number += 1
                if row:
                    try:
                        yield json.loads(row)
                    except Exception as row_e:
                        if data_exception_handler:
                            data_exception_handler(line_number, row, row_e, **data_exception_kwargs)
                        else:
                            raise row_e
        except ValueError as e:
            raise ValueError("Failed to read file {}. Error at line {}: {}"
                             .format(input_file_path, line_number, e)) from e
