import datetime

import chardet
import numpy as np
import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.core import models
from app.core.crud import get_calc_tou, get_season_coefficient_body_df, write_log
from app.settings import CalcStateEnum, MyLogTypeEnum
from app.utils.utils import table_writer, read_sql_with_chunk


def calc_tou(db: Session, postgre_eng: Engine, komandor_eng: Engine, calc_tou_id: int, username: str = ""):
    def save_log(message: str, type_log: MyLogTypeEnum = MyLogTypeEnum.START, is_append: bool = True):
        write_log(
            db=db,
            parent_id=calc_tou_id,
            parent_name="calc_tou",
            type=type_log,
            msg=message,
            is_append=is_append,
            username=username,
        )
        print(message)

    parameters = get_calc_tou(db, calc_tou_id)
    if parameters.status != CalcStateEnum.new:
        save_log(f"The attempt to calculate the TOU was rejected (status = {parameters.status})", is_append=False)
        return

    db.query(models.CalcTOU).filter(models.CalcTOU.id == calc_tou_id).update({"status": CalcStateEnum.in_process})
    db.commit()

    time_start = datetime.datetime.now()
    save_log(f'Start function "calc_tou"', is_append=False)

    pd.options.display.max_columns = 100

    # report_df = pd.read_sql('select * from fact limit 10000;', con=postgre_eng)
    # file_name = f'report_tou_{parameters.date_from.strftime("%Y-%m")}_{parameters.date_to.strftime("%Y-%m")}'
    #
    # stream = table_writer(dataframes={'Sheet1': report_df}, param='xlsx')
    # db_file_storage = models.FileStorage(
    #     file_name=file_name,
    #     file_body=stream.read(),
    # )
    # db.add(db_file_storage)
    # db.commit()
    #
    # db.query(models.CalcTOU).filter(models.CalcTOU.id == calc_tou_id).update({'file_storage_id': db_file_storage.id})
    # db.commit()
    #
    # return report_df, file_name

    sql_command = f"select * from fact WHERE date_rep between '{parameters.date_from}' and '{parameters.date_to}'"
    if parameters.branch_id:
        sql_command += f" and org_id = '{parameters.branch_id}'"
    if parameters.type_operation_list:
        type_operation_list = [el.type_operation.name for el in parameters.type_operation_list]
        type_operation_list = ", ".join(map(lambda x: f"'{x}'", type_operation_list))
        sql_command += f" and type_op in ({type_operation_list})"
    if parameters.rps_list:
        rps_list = [el.rps_short for el in parameters.rps_list]
        rps_list = ", ".join(map(lambda x: f"'{x}'", rps_list))
        sql_command += f" and rps_short in ({rps_list})"
    if parameters.station_list:
        station_list = [el.st_code for el in parameters.station_list]
        station_list = ", ".join(map(lambda x: f"'{x}'", station_list))
        sql_command += f" and st_code in ({station_list})"

    # report_df = pd.read_sql(sql_command, con=postgre_eng)
    report_df = read_sql_with_chunk(postgre_eng, sql_command, 3000000)
    # report_df = pd.read_sql("select * from fact WHERE date_rep between '2019-01-01' and '2019-12-31';", con=postgre_eng)
    # report_df = pd.read_sql('select * from fact limit 10000;', con=postgre_eng)
    # report_df = pd.read_sql('select * from fact;', con=postgre_eng)
    report_df = add_info_by_station_cod(komandor_eng, report_df, parameters.group_data)
    report_df = add_info_by_client_sap_id(postgre_eng, report_df)
    report_df = add_info_cargo_group_go_short(komandor_eng, report_df)
    columns_for_rename = {
        "date_rep": "Отчётная дата",
        "st_code": "Станция выполнения ГО код",
        "st_name": "Станция выполнения ГО",
        "rw_code": "Дорога выполнения ГО код",
        "rw_short_name": "Дорога выполнения ГО Сокр",
        "rw_name": "Дорога выполнения ГО Полн",
        "org_id": "Филиал ГО ID",
        "org_shortname": "Филиал ГО Сокр",
        "org_name": "Филиал ГО Полн",
        "client_sap_id": "Клиент ID SAP",
        "client": "Клиент Наименование",
        "type_op": "Операция тип",
        "wagon_num": "Вагон №",
        "rps_short": "РПС Наименование Сокр",
        "cargo_group_num": "Группа груза ГО, номер",
        "gg_name": "Группа груза ГО Наименование Сокр",
        "parking_fact": "Простои Факт, ваг-сут",
    }
    column_for_group = [
        "Период",
        "РПС Наименование Сокр",
        "Операция тип",
        "Группа груза ГО, номер",
        "Станция выполнения ГО код",
        "Филиал ГО ID",
        "Клиент ID SAP",
    ]

    if parameters.group_data == "РОС1С2КГ":
        columns_for_rename.update(
            {
                "st_code_from": "Станция отправления код",
                "st_name_from": "Станция отправления",
                "st_code_to": "Станция назначения код",
                "st_name_to": "Станция назначения",
            }
        )
        column_for_group += ["Станция отправления код", "Станция назначения код"]

    report_df = report_df[columns_for_rename.keys()]
    report_df.rename(columns=columns_for_rename, inplace=True)
    report_df["Отчётная дата"] = pd.to_datetime(report_df["Отчётная дата"], errors="coerce")
    report_df["Период"] = report_df["Отчётная дата"].dt.strftime("%Y-%m")
    report_df["Группа груза ГО, номер"].fillna(0, inplace=True)
    # report_df.loc[report_df["Клиент Наименование"].isnull(), "Клиент Наименование"] = report_df["Клиент ID SAP"]
    report_df.loc[:, "Сумма 'Вагон №' по ПРОСКГ, ед"] = report_df.groupby(column_for_group)["Отчётная дата"].transform(
        "count"
    )
    report_df.loc[:, "Сумма 'Вагон №' по ПФРО, ед."] = report_df.groupby(
        [
            "Период",
            "РПС Наименование Сокр",
            "Операция тип",
            "Филиал ГО ID",
        ]
    )["Отчётная дата"].transform("count")
    report_df.loc[:, "Доля в ПФРО, %"] = (
        report_df["Сумма 'Вагон №' по ПРОСКГ, ед"] / report_df["Сумма 'Вагон №' по ПФРО, ед."]
    )

    # Расчет финальной витрины
    report_df = report_df[
        (report_df["Простои Факт, ваг-сут"] > parameters.exclude_to)
        & (report_df["Простои Факт, ваг-сут"] < parameters.exclude_from)
        & (report_df["Доля в ПФРО, %"] > float(parameters.exclude_volumes_traffic_less))  # for TEST (need uncomment)
    ]

    save_log(f'Received {report_df.shape[0]} rows from the "fact", after applying a filter on calc_tou parameters.')
    # print(report_df.shape)
    # print(report_df.head())

    column_for_group = [
        "Филиал ГО Сокр",
        "РПС Наименование Сокр",
        "Операция тип",
        "Группа груза ГО, номер",
        "Группа груза ГО Наименование Сокр",
        "Клиент ID SAP",
        "Клиент Наименование",
        "Станция выполнения ГО код",
        "Станция выполнения ГО",
    ]
    if parameters.group_data == "РОС1С2КГ":
        column_for_group += [
            "Станция отправления код",
            "Станция назначения код",
            "Станция отправления",
            "Станция назначения",
        ]

    # for TEST !!! (need comment)
    # column_for_group += ["Доля в ПФРО, %", "Сумма 'Вагон №' по ПРОСКГ, ед", "Сумма 'Вагон №' по ПФРО, ед."]

    report_df = report_df.groupby(column_for_group).agg(
        {
            "Вагон №": [("Количество вагоноотправок, ед.", "count")],
            "Простои Факт, ваг-сут": [
                ("Простои Факт Среднее, ваг-сут", "mean"),
                ("Q1", lambda x: x.quantile(0.25)),
                ("Q2", lambda x: x.quantile(0.5)),
                ("Мода", lambda x: pd.Series.mode(round(x, 2))[0]),
            ],
        }
    )
    report_df.columns = report_df.columns.droplevel()

    save_log(f"After aggregation - {report_df.shape[0]} rows, {report_df.shape[1]} cols.")
    # print(report_df.shape)
    # print(report_df.head())

    report_df["Объем < 32"] = report_df["Количество вагоноотправок, ед."] < 32
    report_df["Q2 > срзнач"] = report_df["Q2"] > report_df["Простои Факт Среднее, ваг-сут"]

    report_df["Конечное значение ТОУ, ваг-сут"] = np.where(
        report_df["Объем < 32"], report_df["Простои Факт Среднее, ваг-сут"], report_df["Q1"]
    )
    report_df["drop_Базовый Уровень на начало расчёта"] = np.where(
        report_df["Q2 > срзнач"], report_df["Q2"], report_df["Простои Факт Среднее, ваг-сут"]
    )

    report_df["Потенциал, %"] = (
        report_df["drop_Базовый Уровень на начало расчёта"] - report_df["Конечное значение ТОУ, ваг-сут"]
    )
    report_df["75% потенциала"] = report_df["Потенциал, %"] * 0.75

    report_df["Процентная годовая динамика 75%"] = (
        np.power(
            (report_df["drop_Базовый Уровень на начало расчёта"] - report_df["75% потенциала"])
            / report_df["drop_Базовый Уровень на начало расчёта"],
            1 / 5,
        )
        - 1
    )

    report_df[f"{parameters.base_year}г"] = report_df["drop_Базовый Уровень на начало расчёта"]
    # report_df["Базовый Уровень на начало расчёта"] = report_df["drop_Базовый Уровень на начало расчёта"]
    report_df = report_df.drop(columns=["drop_Базовый Уровень на начало расчёта"])

    prev_tou = report_df[f"{parameters.base_year}г"]
    for i in range(parameters.amount_year_period + 1):
        if i == 0:
            report_df[f"{parameters.base_year + i}г"] = prev_tou
        else:
            report_df[f"{parameters.base_year + i}г"] = (
                prev_tou + prev_tou * report_df["Процентная годовая динамика 75%"]
            )
        prev_tou = report_df[f"{parameters.base_year + i}г"]
    # report_df["Целевое ТОУ на конец планового периода"] = report_df[f"{parameters.base_year + parameters.amount_year_period}г"]

    save_log(
        f"After add {parameters.amount_year_period} year periods - "
        f"{report_df.shape[0]} rows, {report_df.shape[1]} cols."
    )
    # print(report_df.shape)
    # print(report_df.head())

    season_coeffs_df = get_season_coefficient_body_df(postgre_eng, parameters.seasonal_coefficient_id)
    season_coeffs_df.rename(
        columns={"rps_short": "РПС Наименование Сокр", "type_operation": "Операция тип"}, inplace=True
    )

    report_df = report_df.reset_index().merge(season_coeffs_df, on=["РПС Наименование Сокр", "Операция тип"])

    for i in range(parameters.amount_year_period + 1):
        for j in range(1, 13):
            col_name = f"{parameters.base_year + i}-{j:02d}"
            report_df[col_name] = report_df[f"{parameters.base_year + i}г"] * report_df[f"СК{j:02d}"]

    report_df = report_df.drop(
        columns=[c for c in report_df.columns if c[:2] == "СК"]
        # columns=[c for c in report_df.columns if c[:2] == "СК"] + ["БУ+0г", f"БУ+{parameters.amount_year_period}г"]
    )

    save_log(f"After merged seasonal coefficients: {report_df.shape[0]} rows, {report_df.shape[1]} cols.")
    # print(report_df.shape)
    # print(report_df.head())
    # df_to_new_table(db=db, engine=postgre_eng, df=report_df, table_name='calc_tou_result')
    # save_df_to_model_via_csv(engine=postgre_eng, df=report_df, cols=report_df.columns, db_table='calc_tou_result')
    # report_df.drop(columns=["Группа груза ГО, номер", "Станция выполнения ГО код"], inplace=True)
    if parameters.group_data == "РОС1С2КГ":
        report_df.insert(
            0,
            parameters.group_data,
            report_df["РПС Наименование Сокр"].astype(str)
            + report_df["Операция тип"].astype(str)
            + report_df["Станция отправления код"].astype(str)
            + report_df["Станция назначения код"].astype(str)
            + report_df["Клиент ID SAP"].astype(str)
            + report_df["Группа груза ГО, номер"].apply(lambda x: "None" if x is None else str(int(x))),
        )
    else:
        report_df.insert(
            0,
            parameters.group_data,
            report_df["РПС Наименование Сокр"].astype(str)
            + report_df["Операция тип"].astype(str)
            + report_df["Станция выполнения ГО код"].astype(str)
            + report_df["Клиент ID SAP"].astype(str)
            + report_df["Группа груза ГО, номер"].apply(lambda x: "None" if x is None else str(int(x))),
        )

    report_df.insert(0, "База", f"{parameters.id}: {parameters.name}")
    # report_df = report_df.round(2)
    # report_df.to_excel("report_tou_2019.xlsx", index=False)
    # chunksize = 20000
    # report_df.to_sql(
    #     "calc_tou_result", if_exists="replace", index_label="id", con=postgre_eng, chunksize=chunksize, method="multi"
    # )
    file_name = f'report_tou_{parameters.date_from.strftime("%Y-%m")}_{parameters.date_to.strftime("%Y-%m")}.xlsx'
    save_log(f"Started saving result in DB ({file_name} - {report_df.shape[0]} rows, {report_df.shape[1]} cols).")

    stream = table_writer(dataframes={f"base year {parameters.base_year}": report_df}, param="xlsx")
    db_file_storage = models.FileStorage(file_name=file_name, file_body=stream.read())
    db.add(db_file_storage)
    db.commit()

    db.query(models.CalcTOU).filter(models.CalcTOU.id == calc_tou_id).update(
        {"file_storage_id": db_file_storage.id, "status": CalcStateEnum.done}
    )
    db.commit()
    save_log(
        f'Finished function (execution period {str(datetime.datetime.now() - time_start).split(".", 2)[0]})',
        type_log=MyLogTypeEnum.FINISH,
    )
    # return report_df, file_name


def get_file_encoding(file_name):
    test_str = b""
    count = 0
    with open(file_name, "rb") as x:
        line = x.readline()
        while line and count < 50:  # Set based on lines you'd want to check
            test_str = test_str + line
            count = count + 1
            line = x.readline()
    return chardet.detect(test_str)["encoding"]


def add_info_by_station_cod(engine_ora: Engine, df: DataFrame, group_data: str = "РОСКГ"):
    dim_st_rw_org_df = pd.read_sql(
        """
    SELECT
        s.ST_CODE, s.ST_NAME
        , vr.RW_CODE, vr.RW_SHORT_NAME, vr.RW_NAME
        , f.ORG_ID as org_id2, f.SHORTNAME org_shortname, f.NAME org_name
    FROM ssp.STATIONS s
    INNER JOIN nsi.V_RAILWAY_SYSDATE vr
        ON s.ROADID = vr.RW_CODE
    INNER JOIN ssp.ORG_FILIAL f
        ON s.BRANCH_ID = f.ORG_ID AND vr.RW_CODE = f.RW_CODE
    """,
        con=engine_ora,
    )

    new_report_df = df.merge(dim_st_rw_org_df, how="left", left_on="st_code", right_on="st_code")
    if group_data == "РОС1С2КГ":
        new_report_df = new_report_df.merge(
            dim_st_rw_org_df.add_prefix("from_"), how="left", left_on="st_code_from", right_on="from_st_code"
        )
        new_report_df = new_report_df.merge(
            dim_st_rw_org_df.add_prefix("to_"), how="left", left_on="st_code_to", right_on="to_st_code"
        )
        new_report_df.rename(columns={"from_st_name": "st_name_from", "to_st_name": "st_name_to"}, inplace=True)
    return new_report_df


def add_info_by_client_sap_id(engine: Engine, df: DataFrame):
    mapping_df = pd.read_sql("SELECT * FROM mapping_client_cognos_sap", con=engine)
    df = df.merge(mapping_df, how="left", left_on="client_sap_id", right_on="client_sap_id")
    df.loc[df["client"].isnull(), "client"] = df["client_sap_id"]
    return df


def add_info_cargo_group_go_short(engine_ora: Engine, df: DataFrame):
    dim_freight_df = pd.read_sql("SELECT GG_NUMBER, GG_NAME FROM nsi.V_SUM_FREIGHT_SYSDATE", con=engine_ora)
    new_report_df = df.merge(
        dim_freight_df,
        how="left",
        left_on="cargo_group_num",
        right_on="gg_number",
    )
    return new_report_df
