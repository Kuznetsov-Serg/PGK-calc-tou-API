import os
from datetime import timedelta
import pandas
from fastapi import UploadFile, HTTPException
from minio import Minio
from typing import List

from file_uploads.file_upload_types import SupplierFile, SupplierFileMeta
from server.settings import PARSED_CONFIG, ALLOWED_EXCEL_EXTENSIONS, ALLOWED_TABLE_EXTENSIONS, ALLOWED_CSV_EXTENSIONS

client = Minio(
    PARSED_CONFIG.minio.dsn,
    access_key=PARSED_CONFIG.minio.access_key,
    secret_key=PARSED_CONFIG.minio.secret_key,
    region=PARSED_CONFIG.minio.region,
    secure=False
)

client_external = Minio(
    endpoint=PARSED_CONFIG.server.minio_external_url,
    access_key=PARSED_CONFIG.minio.access_key,
    secret_key=PARSED_CONFIG.minio.secret_key,
    region=PARSED_CONFIG.minio.region,
    secure=False
)


def check_bucket(client_bucket: str):
    if not client.bucket_exists(bucket_name=client_bucket):
        client.make_bucket(bucket_name=client_bucket)


def store_client_file(upload_file: UploadFile, client_bucket: str):
    try:
        client.put_object(bucket_name=client_bucket,
                          object_name=upload_file.filename,
                          data=upload_file.file,
                          length=-1,
                          content_type=upload_file.content_type,
                          part_size=10 * 1024 * 1024)
    except ValueError as e:
        raise HTTPException(status_code=502, detail=e)


def store_client_files(files: List[UploadFile], client_bucket: str):
    [store_client_file(upload_file=item, client_bucket=client_bucket) for item in files]


def file_linker(file: str, bucket_name: str):
    return client_external.get_presigned_url(
        "GET",
        bucket_name,
        file,
        expires=timedelta(days=7),
    )


def date_retrieve(client_bucket: str, file: str):
    stats = client.stat_object(bucket_name=client_bucket, object_name=file)
    return SupplierFileMeta(
        date=stats.last_modified,
        content_type=stats.content_type
    )


def remove_object(client_bucket: str, object_name: str):
    try:
        client.remove_object(bucket_name=client_bucket, object_name=object_name)
    except ValueError as e:
        raise HTTPException(status_code=502, detail=e)


def get_files_list(client_bucket: str):
    objects = [i.object_name for i in client.list_objects(client_bucket)]
    return [SupplierFile(
        name=i,
        link=file_linker(i, bucket_name=client_bucket),
        meta=date_retrieve(client_bucket=client_bucket, file=i)
    ).dict() for i in objects]


async def validate_excel(file: UploadFile):
    extension = os.path.splitext(file.filename)[1]
    if extension in ALLOWED_TABLE_EXTENSIONS:
        return extension
    raise HTTPException(status_code=400, detail=f"Files with {extension} extension not allowed")


def table_reader(file: UploadFile, ext):
    if ext in ALLOWED_EXCEL_EXTENSIONS:
        return pandas.read_excel(io=file.file.read(), sheet_name='Заявки', index_col=False)
    elif ext in ALLOWED_CSV_EXTENSIONS:
        return pandas.read_csv(file.file)
    return None

