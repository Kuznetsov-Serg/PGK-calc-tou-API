import logging
import sys
import warnings
from collections.abc import Mapping
from enum import Enum
from functools import wraps
from io import BytesIO, StringIO
from pathlib import Path
from time import process_time
from typing import Optional

import numpy as np
import pandas as pd
import yaml
from fastapi import HTTPException
from pandas import DataFrame, ExcelWriter
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.utils.utils_df import MAPPING_NAME_COGNOS


def read_yaml(path: Path) -> Mapping:
    with open(path, encoding="utf-8") as file:
        return yaml.safe_load(file)


def merge(left: Mapping, right: Mapping) -> Mapping:
    """
    Merge two mappings objects together, combining overlapping Mappings,
    and favoring right-values
    left: The left Mapping object.
    right: The right (favored) Mapping object.
    NOTE: This is not commutative (merge(a,b) != merge(b,a)).
    """
    merged = {}

    left_keys = frozenset(left)
    right_keys = frozenset(right)

    # Items only in the left Mapping
    for key in left_keys - right_keys:
        merged[key] = left[key]

    # Items only in the right Mapping
    for key in right_keys - left_keys:
        merged[key] = right[key]

    # in both
    for key in left_keys & right_keys:
        left_value = left[key]
        right_value = right[key]

        if isinstance(left_value, Mapping) and isinstance(right_value, Mapping):  # recursive merge
            merged[key] = merge(left_value, right_value)
        else:  # overwrite with right value
            merged[key] = right_value

    return merged


def measure(func):
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(process_time() * 1000))
        try:
            return func(*args, **kwargs)
        finally:
            end_ = int(round(process_time() * 1000)) - start
            logging.info(f"Total execution time {func.__name__}: {end_ if end_ > 0 else 0} ms")

    return _time_it


def multi_case_filter_evaluation(filters: list[str], field: str):
    parsed_filters = []
    for item in filters:
        if isinstance(item, str):
            parsed_filters.append(item.lower())
        elif isinstance(item, Enum):
            parsed_filters.append(item.value.lower())
    if isinstance(field, str):
        if field.lower() in parsed_filters:
            return True
    elif isinstance(field, Enum):
        if field.value.lower() in parsed_filters:
            return True
    return False


def filter_parsed_models(filters: dict[str, list], sequence: list):
    filtered_sequence = []
    if not filters:
        return sequence
    for item in sequence:
        for k, v in filters.items():
            field = getattr(item, k)
            if multi_case_filter_evaluation(v, field):
                filtered_sequence.append(item)
    return list(set(filtered_sequence))


def table_writer(dataframes: dict[Optional[str], DataFrame], param: Optional = "xlsx") -> BytesIO:
    output = BytesIO()
    writer = ExcelWriter(output, engine="openpyxl")
    for name, dataframe in dataframes.items():
        if param == "xlsx":
            dataframe.to_excel(writer, sheet_name=name if name else "sheet 1", index=False)
            writer.save()
        elif param == "csv":
            dataframe.to_csv(output, index=False)
        output.seek(0)
    return output


def df_to_new_table(db: Session, engine: Engine, df, table_name, is_cast_uppercase=False):
    # def load_csv_into_new_table(file, db_schema, db_table, db_name='default',
    #                             is_cast_uppercase=False, delimeter='|', encoding='utf-8', decimal='.'):

    def _prepare_create_table_cmd(df):
        dtype_conv_dict = {
            "datetime64[ns]": "date",
            "int64": "bigint",  # some values exceed 4 bytes of pg integer...
            "float64": "numeric",
            "object": "varchar",
            "string": "varchar",
            "bool": "bool",
        }
        create_table_cmd = f'create table "{table_name}" (\n'
        for col in df.columns:
            dtype = str(df[col].dtype)
            field_mod_str = ""
            if dtype in ("object", "string"):
                max_len = df[col].str.len().max()
                max_len = 10 if max_len is np.nan else int(max_len + 10)
                field_mod_str = f"({max_len})"
            pg_type = dtype_conv_dict[dtype]
            create_table_cmd = create_table_cmd + f'"{col}" {pg_type}{field_mod_str},\n'
        create_table_cmd = create_table_cmd[:-2] + ");"
        return create_table_cmd

    print(f"Casting types ...")
    # try to cast all not determined typed to dates
    for col in df.select_dtypes("object").columns:
        df.loc[:, col] = pd.to_datetime(df.loc[:, col], errors="ignore")
    print("done!")

    print(f"Creating new table ...")
    comment_dict = {MAPPING_NAME_COGNOS[col]: col for col in df.columns if col in MAPPING_NAME_COGNOS}
    df.rename(columns=MAPPING_NAME_COGNOS, inplace=True)
    if is_cast_uppercase:
        df.columns = [f"{col.upper()}" for col in df.columns]
    # else:
    #     df.columns = [f'{col}' for col in df.columns]

    drop_table_cmd = f'drop table if exists "{table_name}" CASCADE;'
    db.execute(drop_table_cmd)
    db.commit()

    create_table_cmd = _prepare_create_table_cmd(df)
    db.execute(create_table_cmd)
    db.commit()

    for col, comment in comment_dict.items():
        create_comment_cmd = f"comment on column {table_name}.{col} is '{comment}';"
        db.execute(create_comment_cmd)
    db.commit()
    print("done!")

    print(f"Loading into db ...")
    save_df_to_model_via_csv(engine, df, cols=df.columns, db_table=table_name)
    print("done!")


def save_df_to_model_via_csv(engine: Engine, df, cols=None, model_class=None, db_table=None):
    # fastest way to insert into model raw data via CSV file
    assert model_class or db_table, "model_class or db_table should be provided"
    if df.empty:
        return
    cols = df.columns if cols is None else cols
    if model_class:
        db_table = model_class.__tablename__
        cols_from_model = set(map(lambda x: str(x).split(".")[-1], model_class.__table__.columns))
        cols = list(cols_from_model.intersection(cols))

        for col in cols:
            if str(model_class.__dict__[col].type).startswith("VARCHAR("):
                df.loc[df[col].isnull(), col] = ""
                # df[col].fillna("", inplace=True)
                max_len_db_col = model_class.__dict__[col].type.length
                max_len_df_col = max(df[col].apply(str).apply(len))
                if max_len_df_col > max_len_db_col:
                    df[col] = df[col].str.slice(0, max_len_db_col - 1)
                    print(f"{col} max len ={max_len_df_col}")

    output = StringIO()
    # import csv
    df[cols].to_csv(output, sep="\t", header=False, index=False, quotechar="&")  # , quoting=csv.QUOTE_MINIMAL)
    # df[cols].to_csv(output, sep="\t", header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    fake_conn = engine.raw_connection()
    fake_cur = fake_conn.cursor()
    fake_cur.copy_from(output, db_table, null="", columns=cols)
    fake_conn.commit()


def save_df_with_unique(
    db: Session,
    engine: Engine,
    db_table_name: str,
    df: pd.DataFrame,
    cols: list = None,
    unique_cols: list = None,
    is_update_exist: bool = False,
):
    if df.shape[0] == 0:
        return {"add_row": 0, "update_row": 0, "message": "DataFrame is empty"}
    db_table_name_tmp = db_table_name + "_tmp"
    if cols is None:
        cols = df.columns if unique_cols is None else unique_cols
    unique_cols = cols if unique_cols is None else unique_cols

    df[cols].to_sql(db_table_name_tmp, con=engine, if_exists="replace", index=False)
    sql_command = (
        f"INSERT INTO {db_table_name} ({', '.join(cols)}) "
        f"SELECT DISTINCT {', '.join(['tmp.' + el for el in cols])} "
        f"FROM {db_table_name_tmp} AS tmp "
        f"LEFT JOIN {db_table_name} AS cur "
        f"USING ({', '.join(unique_cols)}) WHERE cur.id is null;"
    )
    add_rows = db.execute(sql_command).rowcount
    if is_update_exist and add_rows < df.shape[0]:
        sql_command = (
            f"UPDATE {db_table_name} as new "
            f"SET {', '.join([el + ' = tmp.' + el for el in cols])} "
            f"FROM {db_table_name_tmp} AS tmp "
            f"LEFT JOIN {db_table_name} AS cur "
            f"USING ({', '.join(unique_cols)}) "
            f"WHERE new.id = cur.id and not cur.id is null;"
        )
        update_rows = db.execute(sql_command).rowcount
    else:
        update_rows = 0
    db.execute(f"DROP TABLE {db_table_name_tmp} CASCADE;")
    db.commit()
    return {"add_row": add_rows, "update_row": update_rows}


def find_headers_row(
    content, headers_list: list[str], number_analyzed_rows: int = 20, is_return_df: bool = False, skip_footer: int = 0
):
    header_row = 0
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")  # for remove warn: "Workbook contains no default style, ..."
        report_df = pd.read_excel(content, engine="openpyxl", skipfooter=skip_footer)
    if len(headers_list) != sum(1 for el in headers_list if el in list(report_df.columns)):
        for num_str in range(min(report_df.shape[0], number_analyzed_rows)):
            if len(headers_list) == sum(1 for el in headers_list if el in report_df.loc[num_str].values):
                header_row = num_str + 1
                break
        if header_row == 0:
            raise HTTPException(
                status_code=422, detail=f"The full list of headers was not found in the file: ({headers_list})"
            )
        if is_return_df:
            headers = report_df.loc[header_row - 1].values
            report_df = report_df[header_row:]
            report_df.columns = headers
    return report_df[headers_list].reset_index(drop=True) if is_return_df else header_row


def read_excel_with_find_headers(
    content, headers_list: list[str], number_analyzed_rows: int = 20, skip_footer: int = 0
):
    return find_headers_row(
        content=content,
        headers_list=headers_list,
        number_analyzed_rows=number_analyzed_rows,
        is_return_df=True,
        skip_footer=skip_footer,
    )


def get_info_from_excel(content):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")  # for remove warn: "Workbook contains no default style, ..."
        try:
            report_df = pd.read_excel(content, engine="openpyxl")
        except Exception as e:
            try:
                report_df = pd.read_csv(content)
            except Exception as e:
                return {"rows": 0, "cols": 0}
    return {"rows": report_df.shape[0], "cols": report_df.shape[1]}


def read_sql_with_chunk(postgre_eng: Engine, sql_command: str, chunk_size: int = 100000) -> DataFrame:
    sys.stdout.write(f'Performing SQL: "{sql_command}"')
    offset = 0
    report_df = DataFrame()
    while True:
        sql = sql_command + f" limit {chunk_size} offset {offset}"
        df = pd.read_sql(sql, con=postgre_eng)
        # df = reduce_mem_usage(df)
        report_df = pd.concat([report_df, df])
        offset += chunk_size
        sys.stdout.write(".")
        sys.stdout.flush()
        if df.shape[0] < chunk_size:
            break
    report_df.reset_index(drop=True, inplace=True)
    return report_df


def optimize_memory_usage(df, print_size=True):
    # Function optimizes memory usage in dataframe.
    # (RU) Функция оптимизации типов в dataframe.

    # Types for optimization.
    # Типы, которые будем проверять на оптимизацию.
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    # Memory usage size before optimize (Mb).
    # (RU) Размер занимаемой памяти до оптимизации (в Мб).
    before_size = df.memory_usage().sum() / 1024 ** 2
    for column in df.columns:
        column_type = df[column].dtypes
        if column_type in numerics:
            column_min = df[column].min()
            column_max = df[column].max()
            if str(column_type).startswith('int'):
                if column_min > np.iinfo(np.int8).min and column_max < np.iinfo(np.int8).max:
                    df[column] = df[column].astype(np.int8)
                elif column_min > np.iinfo(np.int16).min and column_max < np.iinfo(np.int16).max:
                    df[column] = df[column].astype(np.int16)
                elif column_min > np.iinfo(np.int32).min and column_max < np.iinfo(np.int32).max:
                    df[column] = df[column].astype(np.int32)
                elif column_min > np.iinfo(np.int64).min and column_max < np.iinfo(np.int64).max:
                    df[column] = df[column].astype(np.int64)
            else:
                if column_min > np.finfo(np.float32).min and column_max < np.finfo(np.float32).max:
                    df[column] = df[column].astype(np.float32)
                else:
                    df[column] = df[column].astype(np.float64)
                    # Memory usage size after optimize (Mb).
    # (RU) Размер занимаемой памяти после оптимизации (в Мб).
    after_size = df.memory_usage().sum() / 1024 ** 2
    if print_size:
        print(
        'Memory usage size: before {:5.4f} Mb - after {:5.4f} Mb ({:.1f}%).'.format(before_size, after_size, 100 * (
                before_size - after_size) / before_size))
    return df


def transliteration(text):
    cyrillic = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
    latin = "a|b|v|g|d|e|e|zh|z|i|i|k|l|m|n|o|p|r|s|t|u|f|kh|tc|ch|sh|shch||y||e|iu|ia".split("|")
    tran_dict = {k: v for k, v in zip(cyrillic, latin)}
    new_text = ""
    for letter in text:
        new_letter = tran_dict.get(letter.lower(), letter)
        new_text += new_letter if letter.islower() else new_letter.upper()
    return new_text


def timedelta_to_dhms(duration):
    # преобразование в дни, часы, минуты и секунды
    class TimeDelta:
        __slots__ = ["days", "hours", "minutes", "seconds"]

    result = TimeDelta()
    result.days = duration.days
    result.hours = f"{(duration.seconds // 3600):02}"
    result.minutes = f"{((duration.seconds % 3600) // 60):02}"
    result.seconds = f"{((duration.seconds % 60)):02}"
    return result
