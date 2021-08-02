import os
import time
import uuid
from .. import log
from .. import util
from .. import file
from . import client
from ..exception import NoSuchS3File
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError


def create_client():
    return client.create_client('s3')


def create_resource():
    return client.create_resource('s3')


def parse_bucket_and_prefix_from_uri(s3_uri):
    return s3_uri.replace('s3a://', '').replace('s3://', '').split('/', 1)


def build_file_uri(bucket_name, file_path, protocol="s3"):
    return f"{protocol}://{bucket_name}/{file_path}"


def prefix_exists(bucket, prefix, include_suffix=None, s3_client=None):
    if s3_client is None:
        s3_client = create_client()

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )

    if response['KeyCount'] > 0 and 'Contents' in response:
        if include_suffix is not None:
            file_list = [f['Key'] for f in response['Contents'] if f['Key'].endswith(include_suffix)]
        else:
            # exclude folders
            file_list = [f['Key'] for f in response['Contents'] if not f['Key'].endswith('/')]
        return len(file_list) > 0

    return False


def uri_exists(s3_uri):
    bucket, prefix = parse_bucket_and_prefix_from_uri(s3_uri)
    return prefix_exists(bucket, prefix)


def yield_file_detail_list(bucket, prefix=None, s3_client=None, include_suffix=None, max_keys=None):
    kwargs = {}
    if max_keys is not None:
        kwargs['PaginationConfig'] = {'PageSize': max_keys}

    if prefix is not None:
        kwargs['Prefix'] = prefix

    if s3_client is None:
        s3_client = create_client()

    paginator_iterator = s3_client.get_paginator('list_objects_v2').paginate(Bucket=bucket,
                                                                             **kwargs)

    file_count = 0

    for response in paginator_iterator:
        if response['KeyCount'] > 0 and 'Contents' in response:
            for f in response['Contents']:
                # exclude folders
                if (include_suffix is not None and f['Key'].endswith(include_suffix)) or not f['Key'].endswith('/'):
                    yield f

                    if max_keys is not None:
                        file_count += 1
                        if file_count >= max_keys:
                            return


def yield_file_list(bucket, prefix=None, s3_client=None, include_suffix=None, max_keys=None):
    kwargs = {}
    if max_keys is not None:
        kwargs['PaginationConfig'] = {'PageSize': max_keys}

    if prefix is not None:
        kwargs['Prefix'] = prefix

    if s3_client is None:
        s3_client = create_client()

    paginator_iterator = s3_client.get_paginator('list_objects_v2').paginate(Bucket=bucket,
                                                                             **kwargs)

    file_count = 0

    for response in paginator_iterator:
        if response['KeyCount'] > 0 and 'Contents' in response:
            for f in response['Contents']:
                # exclude folders
                if (include_suffix is not None and f['Key'].endswith(include_suffix)) or not f['Key'].endswith('/'):
                    yield f['Key']

                    if max_keys is not None:
                        file_count += 1
                        if file_count >= max_keys:
                            return


def get_full_file_list(bucket, prefix=None, s3_client=None, include_suffix=None, max_keys=None):
    return list(yield_file_list(bucket,
                                prefix,
                                s3_client=s3_client,
                                include_suffix=include_suffix,
                                max_keys=max_keys))


def get_file_list(bucket, prefix=None, s3_client=None, include_suffix=None, continuation_token=None, max_keys=None):
    kwargs = {}
    if continuation_token is not None:
        kwargs['ContinuationToken'] = continuation_token

    #if max_keys is not None:
    #    kwargs['MaxKeys'] = max_keys

    if prefix is not None:
        kwargs['Prefix'] = prefix

    if s3_client is None:
        s3_client = create_client()

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        **kwargs
    )

    file_list = []

    if response['KeyCount'] > 0 and 'Contents' in response:
        result_list = response['Contents']

        file_count = 0
        for f in result_list:
            file_count += 1
            if include_suffix is not None:
                if f['Key'].endswith(include_suffix):
                    file_list.append(f['Key'])
            elif not f['Key'].endswith('/'):
                # exclude folders
                file_list.append(f['Key'])

            if max_keys is not None and len(file_list) >= max_keys:
                new_continuation_token = f['Key'] if file_count < response['KeyCount'] else None
                return file_list, new_continuation_token

        if include_suffix is not None:
            file_list = [f['Key'] for f in result_list if f['Key'].endswith(include_suffix)]
        else:
            # exclude folders
            file_list = [f['Key'] for f in result_list if not f['Key'].endswith('/')]

    new_continuation_token = response['NextContinuationToken'] if response['IsTruncated'] else None

    return file_list, new_continuation_token


def yield_folder_list(bucket, prefix=None, s3_client=None, include_suffix=None, exclude_suffix=None, max_keys=None):
    kwargs = {}
    if max_keys is not None:
        kwargs['PaginationConfig'] = {'PageSize': max_keys}

    # prefix should end with '/'
    if prefix is not None:
        if not prefix.endswith('/'):
            prefix += '/'
        kwargs['Prefix'] = prefix

    if s3_client is None:
        s3_client = create_client()

    paginator = s3_client.get_paginator('list_objects_v2')
    paginator_iterator = paginator.paginate(Bucket=bucket, Delimiter='/', **kwargs)

    folder_count = 0

    for response in paginator_iterator:
        if response['KeyCount'] > 0 and 'CommonPrefixes' in response:
            result_list = response['CommonPrefixes']

            if include_suffix is not None:
                folder_list = [f['Prefix'] for f in result_list if f['Prefix'].endswith(include_suffix)]
            elif exclude_suffix is not None:
                folder_list = [f['Prefix'] for f in result_list if not f['Prefix'].endswith(exclude_suffix)]
            else:
                folder_list = [f['Prefix'] for f in result_list]

            for folder_name in folder_list:
                yield folder_name

                if max_keys is not None:
                    folder_count += 1
                    if folder_count >= max_keys:
                        return


def get_full_folder_list(bucket, prefix=None, s3_client=None, include_suffix=None, max_keys=None):
    return list(yield_folder_list(bucket,
                                  prefix=prefix,
                                  s3_client=s3_client,
                                  include_suffix=include_suffix,
                                  max_keys=max_keys))


def get_recursive_folder_list(bucket, prefix=None, s3_client=None, include_suffix=None):
    if s3_client is None:
        s3_client = create_client()

    folder_list = get_full_folder_list(bucket, prefix=prefix,
                                       s3_client=s3_client,
                                       include_suffix=include_suffix)

    full_folder_list = []

    for folder in folder_list:
        subfolder_list = get_recursive_folder_list(bucket, folder,
                                                   s3_client=s3_client,
                                                   include_suffix=include_suffix)

        if subfolder_list:
            full_folder_list.extend(subfolder_list)
        else:
            full_folder_list.append(folder)

    return full_folder_list


def get_folder_list(bucket, prefix=None, s3_client=None, include_suffix=None, exclude_suffix=None,
                    continuation_token=None, max_keys=None):
    kwargs = {}

    if continuation_token is not None:
        kwargs['ContinuationToken'] = continuation_token

    if max_keys is not None:
        kwargs['MaxKeys'] = max_keys

    if prefix is not None:
        # prefix should end with '/'
        if not prefix.endswith('/'):
            prefix += '/'
        kwargs['Prefix'] = prefix

    if s3_client is None:
        s3_client = create_client()

    # Prefix=prefix.strip('/'),
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Delimiter='/',
        **kwargs
    )

    folder_list = []

    if response['KeyCount'] > 0 and 'CommonPrefixes' in response:
        result_list = response['CommonPrefixes']

        if include_suffix is not None:
            folder_list = [f['Prefix'] for f in result_list if f['Prefix'].endswith(include_suffix)]
        elif exclude_suffix is not None:
            folder_list = [f['Prefix'] for f in result_list if not f['Prefix'].endswith(exclude_suffix)]
        else:
            folder_list = [f['Prefix'] for f in result_list]

    new_continuation_token = response['NextContinuationToken'] if response['IsTruncated'] else None

    return folder_list, new_continuation_token


def is_folder_empty(bucket, prefix=None, include_suffix=None, s3_client=None):
    return len(get_full_file_list(bucket, prefix, s3_client=s3_client, include_suffix=include_suffix, max_keys=1)) <= 0


def copy_file_to_folder(source_bucket, source_file_key, target_bucket, target_folder, s3_client=None,
                        target_file_name=None, target_suffix=None):
    file_name = os.path.basename(source_file_key) if target_file_name is None else target_file_name

    target_file_key = os.path.join(target_folder, file_name)
    if target_suffix:
        target_file_root, _ = os.path.splitext(target_file_key)
        target_file_key = ''.join([target_file_root, target_suffix])

    # do nothing if source and target file location are the same
    if source_bucket == target_bucket and source_file_key == target_file_key:
        return None

    copy_source = {
        'Bucket': source_bucket,
        'Key': source_file_key
    }

    if s3_client is None:
        s3_client = create_client()

    s3_client.copy(copy_source, target_bucket, target_file_key)

    return target_file_key


def move_file_to_folder(source_bucket, source_file_key, target_bucket, target_folder, s3_client=None,
                        target_file_name=None, target_suffix=None):
    if copy_file_to_folder(source_bucket, source_file_key, target_bucket, target_folder, s3_client=s3_client,
                           target_file_name=target_file_name, target_suffix=target_suffix):
        delete_file(source_bucket, source_file_key, s3_client=s3_client)


def move_folder(source_bucket, source_folder, target_bucket, target_folder):
    target_prefix = os.path.join(target_folder, os.path.basename(source_folder.rstrip('/')))

    if copy_folder(source_bucket, source_folder, target_bucket, target_prefix):
        delete_path(source_bucket, source_folder)


# Copies files from source_uri to target_uri recursively
def copy_uri(source_uri, target_uri, include_suffix=None):
    source_bucket, source_prefix = parse_bucket_and_prefix_from_uri(source_uri)
    target_bucket, target_prefix = parse_bucket_and_prefix_from_uri(target_uri)

    return copy_folder(source_bucket, source_prefix, target_bucket, target_prefix, include_suffix)


# Try to recreate folder tree structure in target path
# First strip original source_folder name from file path to get subfolder name
# Then append subfolder name to target_prefix
# e.g. if source_folder is extract/pass,
#      then extract/pass/assessments/file.txt -> <target_folder>/assessments/file.txt
def append_subfolder_tree_to_target(file_name, source_folder, target_folder):
    # make sure folders has trailing backslash
    if not target_folder.endswith('/'):
        target_folder = os.path.join(target_folder, '')

    folder_name = os.path.dirname(file_name)
    subfolder_name = folder_name.replace(source_folder.rstrip('/'), '', 1).strip('/')
    target_prefix = os.path.join(target_folder, subfolder_name, '') if subfolder_name else target_folder

    return target_prefix


# Copies files from source_bucket/source_folder to target_bucket/target_folder recursively
def copy_folder(source_bucket, source_folder, target_bucket, target_folder, include_suffix=None):
    s3_client = create_client()

    # make sure folders has trailing backslash
    if not source_folder.endswith('/'):
        source_folder = ''.join([source_folder, '/'])
    if not target_folder.endswith('/'):
        target_folder = ''.join([target_folder, '/'])

    file_list = yield_file_list(source_bucket, source_folder, include_suffix=include_suffix)

    file_count = 0
    for file_name in file_list:
        file_count += 1

        # Recreate subfolder structure in target path
        target_prefix = append_subfolder_tree_to_target(file_name, source_folder, target_folder)

        try:
            copy_file_to_folder(source_bucket, file_name, target_bucket, target_prefix, s3_client=s3_client)
        except Exception as e:
            raise Exception("Failed to copy file {}/{} to {}/{}: {}"
                            .format(source_bucket, file_name, target_bucket, target_prefix, e)) from e

    return file_count


def delete_uri_list(s3_uri_list, s3=None):
    if s3 is None:
        s3 = client.create_resource('s3')

    for s3_uri in s3_uri_list:
        delete_uri(s3_uri, s3=s3)


def delete_uri(s3_uri, s3=None):
    bucket, prefix = parse_bucket_and_prefix_from_uri(s3_uri)
    return delete_path(bucket, prefix, s3=s3)


def delete_path(bucket, prefix, s3=None):
    if s3 is None:
        s3 = client.create_resource('s3')

    return s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()


def delete_path_list(bucket, prefix_list, max_concurrent_deletes=500):
    s3_client = create_client()

    batch_file_list = []

    for prefix in prefix_list:
        for file_name in yield_file_list(bucket, prefix):
            batch_file_list.append(file_name)

            if len(batch_file_list) >= max_concurrent_deletes:
                delete_file_list(bucket, batch_file_list, s3_client=s3_client)
                batch_file_list = []

    if batch_file_list:
        delete_file_list(bucket, batch_file_list, s3_client=s3_client)


def delete_file(bucket, file_key, s3_client=None):
    if s3_client is None:
        s3_client = create_client()
    return s3_client.delete_object(Bucket=bucket, Key=file_key)


def delete_file_list(bucket, file_list, s3_client=None):
    if s3_client is None:
        s3_client = create_client()

    delete_objects = [{'Key': file_key} for file_key in file_list]

    return s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_objects})


def get_file_size(bucket, file_key, s3_resource=None):
    if s3_resource is None:
        s3_resource = create_resource()

    s3_object = s3_resource.Object(bucket, file_key)

    try:
        return s3_object.content_length
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise NoSuchS3File(e)
        else:
            raise e


# write content to a new file on S3
def write_file(bucket, key, body=None, md5sum=None, s3_client=None, **kwargs):
    if s3_client is None:
        s3_client = create_client()

    content = body if body is not None else ""

    if md5sum is None:
        md5sum = util.get_md5sum(content.encode('utf-8'))

    response = s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content,
        ContentMD5=md5sum,
        Metadata={'md5checksum': md5sum},
        ServerSideEncryption='AES256',
        **kwargs
    )

    return response['ETag']


def read_file(bucket, key):
    uid = uuid.uuid4()
    local_file = '/tmp/{}_{}' . format(uid, key.replace("/", "_"))
    downloaded_name = download_file(bucket, key, local_file_name=local_file)

    try:
        with open(downloaded_name, 'rb') as f:
            file_content = f.read()
    finally:
        file.delete_local_path(local_file)

    return file_content


def download_file(bucket, key, download_dir=None, s3_client=None, local_file_name=None):
    if local_file_name is not None:
        file.ensure_local_path_exists(download_dir)
        download_path = os.path.join(download_dir, os.path.basename(key))
    else:
        download_path = local_file_name

    if s3_client is None:
        s3_client = create_client()

    # download s3 file to a local folder
    max_retry_count = 5
    while max_retry_count:
        try:
            s3_client.download_file(bucket, key, download_path)
            max_retry_count = 0
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise NoSuchS3File(e)
            elif e.response['Error']['Code'] == '503' or e.response['Error']['Code'] == 'SlowDown':
                max_retry_count -= 1
                if max_retry_count <= 0:
                    raise e
                time.sleep(1)
            else:
                raise e

    return download_path


def download_uri(s3_uri, download_dir, s3_client=None):
    bucket, key = parse_bucket_and_prefix_from_uri(s3_uri)
    return download_file(bucket, key, download_dir, s3_client)


# download s3 file to a local temp file
def download_file_to_tmp(bucket, key, s3_client=None):
    # use uuid to create a unique local file name
    return download_file(bucket, key, '/tmp', s3_client)


# Copies files from source_bucket/source_folder to local file system recursively
def download_folder(source_bucket, source_folder, target_folder, include_suffix=None, s3_client=None):
    if s3_client is None:
        s3_client = create_client()

    # make sure folders has trailing backslash
    if not source_folder.endswith('/'):
        source_folder = os.path.join(source_folder, '/')

    # append source folder name to target
    downloaded_folder = os.path.join(target_folder, os.path.basename(source_folder.rstrip('/')), '')

    file_list = yield_file_list(source_bucket, source_folder, include_suffix=include_suffix)

    for file_name in file_list:
        # Recreate subfolder structure in target path
        target_prefix = append_subfolder_tree_to_target(file_name, source_folder, downloaded_folder)

        try:
            download_file(source_bucket, file_name, target_prefix, s3_client=s3_client)
        except Exception as e:
            raise Exception("Failed to download folder {}/{} to {}: {}"
                            . format(source_bucket, file_name, target_prefix, e)) from e

    return downloaded_folder


# Copies files from source_bucket/source_folder to /tmp recursively
def download_folder_to_tmp(source_bucket, source_folder, include_suffix=None, s3_client=None):
    return download_folder(source_bucket, source_folder, '/tmp', include_suffix, s3_client)


def upload_file(local_file, target_bucket, target_key, md5sum=None, content_type=None, delete_local_file=False,
                s3_client=None):
    if s3_client is None:
        s3_client = create_client()

    if md5sum is None:
        md5sum = util.get_md5sum(open(local_file, 'rb').read())

    extra_args = {
        'ServerSideEncryption': 'AES256',
        'Metadata': {'md5checksum': md5sum}
    }
    if content_type:
        extra_args['ContentType'] = content_type

    max_retry_count = 5
    while max_retry_count:
        try:
            s3_client.upload_file(local_file,
                                  target_bucket,
                                  target_key,
                                  ExtraArgs=extra_args)
            max_retry_count = 0
        except ClientError as e:
            if e.response['Error']['Code'] == '503' or e.response['Error']['Code'] == 'SlowDown':
                max_retry_count -= 1
                if max_retry_count <= 0:
                    raise e
                time.sleep(1)
            else:
                raise e
        except (ValueError, S3UploadFailedError) as e:
            raise Exception("Failed to upload file {} to S3 {}/{}: {}"
                            .format(local_file, target_bucket, target_key, e)) from e

    if delete_local_file:
        file.delete_local_path(local_file)


# local_folder = /tmp/src_filename/schema/assessment
# target_folder = pass/precatalog/src_filename/schema/assessment
def upload_folder(local_folder, target_bucket, target_folder, content_type=None, delete_local_folder=False,
                  s3_client=None):
    if s3_client is None:
        s3_client = create_client()

    # get list of local files recursively
    for file_name in file.list_files_recursively(local_folder):
        base_file_name = os.path.basename(file_name)
        target_prefix = append_subfolder_tree_to_target(file_name, local_folder, target_folder)

        upload_file(local_file=file_name,
                    target_bucket=target_bucket,
                    target_key=os.path.join(target_prefix, base_file_name),
                    content_type=content_type,
                    s3_client=s3_client)

    if delete_local_folder:
        file.delete_local_path(local_folder)


# decorator to download input file from S3 and upload output file(s) back to S3
def file_transfer_handler(func):
    def decorator(bucket, key, s3_client=None, destination_bucket=None, destination_folder=None,
                  delete_source=True, *args, **kwargs):
        logger = log.get_logger()

        target_bucket = destination_bucket if destination_bucket is not None else bucket
        target_folder_name = destination_folder if destination_folder is not None else os.path.dirname(key)

        # use uuid to create a unique local file name
        uid = uuid.uuid4()
        local_file = '/tmp/{}_{}' . format(uid, os.path.basename(key))
        downloaded_name = None
        upload_path_list = None

        try:
            if s3_client is None:
                s3_client = create_client()

            # download s3 file to a local temp file
            downloaded_name = download_file(bucket, key, local_file_name=local_file)
            logger.info(f"Downloaded {bucket}/{key} to {downloaded_name}")

            # use local temp file in decorated function
            func_result = func(downloaded_name, *args, **kwargs)

            # convert result to a list if a single object is returned from function
            upload_path_list = [] if func_result is None \
                else func_result if isinstance(func_result, type([])) else [func_result]

            upload_count = len(upload_path_list)
            logger.info(f"{func.__name__} complete: {upload_count}")

            # function could have produced multiple output files
            # batch-upload all files only after everything processed successfully
            upload_key = None
            for upload_path in upload_path_list:
                # strip out uuid from file name before uploading to s3
                upload_file_name = os.path.basename(upload_path).replace(f"{uid}_", '')
                upload_key = os.path.join(target_folder_name, upload_file_name)

                # upload output file to s3
                upload_file(upload_path, target_bucket, upload_key, delete_local_file=False, s3_client=s3_client)
                logger.info(f"Uploaded to {target_bucket}/{upload_key}")

            # delete original file only if new file name is different from original file name
            if delete_source and upload_path_list and \
                    (len(upload_path_list) > 1 or bucket != target_bucket or upload_key != key):
                delete_file(bucket, key, s3_client=s3_client)
                logger.info(f"Deleted {bucket}/{key}")

            logger.info(f"{func.__name__} Done processing {bucket}/{key}: output {upload_count} files")

            return upload_count
        finally:
            # clean up
            # delete temp input file
            if downloaded_name is not None:
                file.delete_local_path(downloaded_name)

            # delete temp output files
            if upload_path_list is not None:
                for upload_path in upload_path_list:
                    file.delete_local_path(upload_path)

    return decorator


@file_transfer_handler
def compress(download_path):
    return file.compress_file(download_path)


# May return multiple output files if max_lines_per_file is provided
@file_transfer_handler
def decompress(download_path, max_lines_per_file=None):
    return file.decompress_file(download_path, max_lines_per_file)
