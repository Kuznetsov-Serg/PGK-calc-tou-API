from datetime import timedelta
from pathlib import Path

import pandas as pd
from fastapi import UploadFile
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.core.crud import create_season_coefficient, create_season_coefficient_body_list, delete_facts
from app.core.models import Fact
from app.core.schemas import SeasonCoefficientBodyCreate, SeasonCoefficientCreate
from app.utils.utils import read_excel_with_find_headers, save_df_to_model_via_csv, save_df_with_unique
from app.utils.utils_df import MAPPING_NAME_COGNOS, MAPPING_SEASONAL_COEFFICIENT
from app.utils.utils_os import OsCls


def load_fact_from_pickle(db: Session, engine: Engine):
    columns_for_rename = {
        "Отчётная дата": "date_rep",
        "Станция выполнения ГО код": "st_code",
        "Станция выполнения ГО": "st_name",
        "Филиал ГО ID": "org_id",
        "Клиент ID SAP": "client_sap_id",
        "Операция тип": "type_op",
        "Вагон №": "wagon_num",
        "РПС Наименование Сокр": "rps_short",
        "Группа груза ГО, номер": "cargo_group_num",
        "Простои Факт, ваг-сут": "parking_fact",
        "load_from": "load_from",
    }
    content_path = OsCls.get_import_path("content")
    # content_path = Path("Z:\\PoC проекты\\2022-01 Расчёт целевых ТОУ\\01 Jupyter\\tou\\content")
    report_cognos_file = Path(content_path, "report_cognos.pickle")
    report_sap_file = Path(content_path, "report_sap.pickle")

    if not OsCls.get_files_list(content_path, "report_cognos.pickle"):
        return {"message": f"Not find file to load Cognos data ({report_cognos_file})"}
    if not OsCls.get_files_list(content_path, "report_sap.pickle"):
        return {"message": f"Not find file to load SAP data ({report_sap_file})"}

    print(f"Start reading file ({report_cognos_file})")
    cognos_df = pd.read_pickle(report_cognos_file)
    cognos_df["load_from"] = "Cognos"
    cognos_df = cognos_df[columns_for_rename.keys()]
    # report_df = cognos_df
    print(f"Start reading file ({report_sap_file})")
    sap_df = pd.read_pickle(report_sap_file)
    sap_df["load_from"] = "SAP"
    sap_df = sap_df[columns_for_rename.keys()]

    # delete previously uploaded records from this period
    deleted_rec_cognos = delete_facts(
        db=db, date_min=cognos_df["Отчётная дата"].min(), date_max=cognos_df["Отчётная дата"].max(), load_from="Cognos"
    )
    deleted_rec_sap = delete_facts(
        db=db, date_min=sap_df["Отчётная дата"].min(), date_max=sap_df["Отчётная дата"].max(), load_from="SAP"
    )
    # Объединение отчетов
    print("Started concat Cognos + SAP")
    report_df = pd.concat([cognos_df, sap_df]).reset_index(drop=True)
    del [[cognos_df, sap_df]]

    # report_df = report_df[columns_for_rename.keys()]
    report_df.rename(columns=columns_for_rename, inplace=True)
    print(f"Finished concat DF ({report_df.shape[0]} rows)\nStart save in SQL DB")

    save_df_to_model_via_csv(engine=engine, df=report_df, cols=report_df.columns, model_class=Fact)
    return {
        "message": f'Added {len(report_df.index)} records (from Cognos {deleted_rec_cognos["message"]}, '
        f'from SAP {deleted_rec_sap["message"]}).'
    }


# async def load_cognos_file(         # for single load-cognos-excel
def load_cognos_file(db: Session, engine: Engine, engine_ora: Engine, uploaded_file: UploadFile, is_overwrite=True):
    def calc_downtime(row):
        if row["Сдвоенная операция"] != "да":
            result = (row["Дата приема след."] - row["Дата прибытия тек."]).total_seconds() / timedelta(
                days=1
            ).total_seconds()
        elif row["Ваг-сут простоя для сдвоенных"] > 0:
            result = (
                (
                    (row["Дата приема след."] - row["Дата прибытия тек."]).total_seconds()
                    / timedelta(days=1).total_seconds()
                )
                / row["Ваг-сут простоя для сдвоенных"]
                * row["Факт ваг-сут простоя"]
            )
        else:
            result = row["Факт ваг-сут простоя"]
        return round(result, 6)

    content = uploaded_file.read()  # async read
    # content = await uploaded_file.read()    # for single load-cognos-excel
    report_df = pd.read_excel(
        content,
        usecols=[
            "Отчетная дата",
            "Код станции ГО",
            "Станция выполнения ГО",
            "Станция отправления тек.",
            "Станция назначения след.",
            "Грузоотправитель на станции выполнения ГО ",  # Пробел на конце!
            "Грузополучатель на станции выполнения ГО",
            "Id клиента",
            "Тип операции",
            "№ вагона",
            "Род вагона",
            "Наименование груза тек.",
            "Наименование груза тек.",
            "Наименование груза след.",
            "Факт ваг-сут простоя",
            "Сдвоенная операция",
            "Дата приема след.",
            "Дата прибытия тек.",
            "Ваг-сут простоя для сдвоенных",
        ],
    )
    report_df.columns = report_df.columns.str.strip()
    print(report_df.shape)
    print(report_df.head(5))

    # Обработка "Код станции ГО"
    report_df.loc[report_df["Код станции ГО"].notnull(), "Код станции ГО"] = report_df.loc[
        report_df["Код станции ГО"].notnull(), "Код станции ГО"
    ].apply(lambda code: str(int(code)).zfill(5))

    # Обработка "Id клиента"
    report_df.loc[report_df["Id клиента"].isnull(), "Id клиента"] = -1
    report_df["Id клиента"] = report_df["Id клиента"].astype("int")

    # Удаление незначащих пробелов
    report_df = report_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    print(report_df.shape)
    print(report_df.head(5))

    # Adding columns
    dim_station_df = pd.read_sql(
        """
    SELECT st_code5, st_code6
    FROM (
        SELECT DISTINCT
            ROW_NUMBER() OVER(
                PARTITION BY s.ST_CODE
                ORDER BY s.RECDATEEND DESC
            ) rn
            , s.ST_CODE st_code5, s.ST_CODE6
        FROM nsi.STATION s
        WHERE s.ST_CODE IS NOT NULL
    ) WHERE rn = 1
    """,
        con=engine_ora,
    )
    report_df["Код станции ГО"] = report_df["Код станции ГО"].map(dim_station_df.set_index("st_code5")["st_code6"])

    report_df = add_station_code_for_from_and_to(engine_ora, report_df)  # Get station code

    mapping_df = pd.read_sql("""select * from mapping_client_cognos_sap""", con=engine)
    report_df = report_df.astype(dtype={"Id клиента": "string"}).merge(
        mapping_df,
        how="inner",
        left_on="Id клиента",
        right_on="client_cognos_id",
    )

    print(report_df.shape)
    print(report_df.head(5))

    dim_freight_df = pd.read_sql(
        """
        SELECT
            t.FR_CODE_ETSNG, t.FR_SHORT_NAME, t.FR_NAME,
            t.GG_NUMBER, t.GG_NAME
        FROM (
            SELECT
                vfr.FR_CODE_ETSNG, vfr.FR_SHORT_NAME, vfr.FR_NAME,
                vgfr.GG_NUMBER, vgfr.GG_NAME,
                ROW_NUMBER() OVER(PARTITION BY vfr.FR_NAME ORDER BY vfr.RECDATEBEGIN DESC) rn
            FROM nsi.V_FREIGHT_SYSDATE_MOD11 vfr
            INNER JOIN nsi.V_SUM_FREIGHT_SYSDATE vgfr ON vfr.FR_GG_NUMBER = vgfr.GG_NUMBER
        ) t
        WHERE rn = 1
        """,
        con=engine_ora,
    )

    dim_freight_df["Наименование груза"] = dim_freight_df["fr_name"].str.lower()

    # for correct merge (if columns is empty, dataFrame use float type - error in merge)
    report_df["Наименование груза тек."] = report_df["Наименование груза тек."].astype(str)
    report_df["Наименование груза след."] = report_df["Наименование груза след."].astype(str)

    report_df = report_df.merge(
        dim_freight_df.add_prefix("cur_"),
        how="left",
        left_on="Наименование груза тек.",
        right_on="cur_Наименование груза",
    )
    report_df["Группа груза, номер тек."] = report_df["cur_gg_number"]
    report_df["Группа груза Наименование Сокр тек."] = report_df["cur_gg_name"]
    report_df["Груз Наименование Сокр тек."] = report_df["cur_fr_short_name"]
    report_df["Груз Наименование Полн тек."] = report_df["cur_fr_name"].fillna(report_df["Наименование груза тек."])
    report_df["Груз ЕТСНГ код тек."] = report_df["cur_fr_code_etsng"]

    report_df = report_df.merge(
        dim_freight_df.add_prefix("next_"),
        how="left",
        left_on="Наименование груза след.",
        right_on="next_Наименование груза",
    )
    report_df["Группа груза, номер след."] = report_df["next_gg_number"]
    report_df["Группа груза Наименование Сокр след."] = report_df["next_gg_name"]
    report_df["Груз Наименование Сокр след."] = report_df["next_fr_short_name"]
    report_df["Груз Наименование Полн след."] = report_df["next_fr_name"].fillna(report_df["Наименование груза след."])
    report_df["Груз ЕТСНГ код след."] = report_df["next_fr_code_etsng"]

    dim_freight_unmatched_df = pd.DataFrame(
        {
            "концентрат железорудный (гематит)": {
                "Группа груза, номер": 7,
                "Группа груза Наименование Сокр": "РУДА ЖЕЛЕЗНАЯ И МАРГАНЦЕВАЯ",
                "Груз Наименование Сокр": "ГЕМАТИТ",
                "Груз Наименование Полн": "КОНЦЕНТРАТ ЖЕЛЕЗОРУДНЫЙ (ГЕМАТИТ, МАГНЕТИТ)",
                "Груз ЕТСНГ код": "141092",
            },
            "кварциты, кроме бакальских, криворожских и кма": {
                "Группа груза, номер": 22,
                "Группа груза Наименование Сокр": "ОГНЕУПОРЫ",
                "Груз Наименование Сокр": "КВАРЦИТЫ НЕКРИВ",
                "Груз Наименование Полн": "КВАРЦИТЫ, КРОМЕ  КРИВОРОЖСКИХ И КМА",
                "Груз ЕТСНГ код": "301059",
            },
            'средства транспортирования (тележка тт-20 "бухара" тяжеловесная и др.), не поименованные в алфавите': {
                "Группа груза, номер": 12,
                "Группа груза Наименование Сокр": "МЕТИЗЫ",
                "Груз Наименование Сокр": "СРЕДСТВА ТР ПР",
                "Груз Наименование Полн": "СРЕДСТВА ТРАНСПОРТИРОВАНИЯ, НЕ ПОИМЕНОВАННЫЕ В АЛФАВИТЕ",
                "Груз ЕТСНГ код": "391303",
            },
            "вещества радиоактивные": {
                "Группа груза, номер": 43,
                "Группа груза Наименование Сокр": "ОСТАЛЬНЫЕ И  СБОРНЫЕ ГРУЗЫ",
                "Груз Наименование Сокр": "ВЕЩЕСТ РАДИОАКТ",
                "Груз Наименование Полн": "ВЕЩЕСТВА РАДИОАКТИВНЫЕ, НЕ ПОИМЕНОВАННЫЕ В АЛФАВИТЕ",
                "Груз ЕТСНГ код": "693015",
            },
            "холодильники электробытовые": {
                "Группа груза, номер": 12,
                "Группа груза Наименование Сокр": "МЕТИЗЫ",
                "Груз Наименование Сокр": "ХОЛОДИЛЬН мороз",
                "Груз Наименование Полн": "ХОЛОДИЛЬНИКИ, холодильники-морозильники и морозильники электробытовыеЫЕ",
                "Груз ЕТСНГ код": "404217",
            },
        }
    ).transpose()

    print(dim_freight_unmatched_df)

    cur_filter = report_df["Груз Наименование Полн тек."].notnull() & report_df["Груз ЕТСНГ код тек."].isnull()

    report_df.loc[cur_filter, "Группа груза, номер тек."] = report_df.loc[cur_filter, "Наименование груза тек."].map(
        dim_freight_unmatched_df["Группа груза, номер"]
    )
    report_df.loc[cur_filter, "Группа груза Наименование Сокр тек."] = report_df.loc[
        cur_filter, "Наименование груза тек."
    ].map(dim_freight_unmatched_df["Группа груза Наименование Сокр"])
    report_df.loc[cur_filter, "Груз Наименование Сокр тек."] = report_df.loc[cur_filter, "Наименование груза тек."].map(
        dim_freight_unmatched_df["Груз Наименование Сокр"]
    )
    report_df.loc[cur_filter, "Груз Наименование Полн тек."] = report_df.loc[cur_filter, "Наименование груза тек."].map(
        dim_freight_unmatched_df["Груз Наименование Полн"]
    )
    report_df.loc[cur_filter, "Груз ЕТСНГ код тек."] = report_df.loc[cur_filter, "Наименование груза тек."].map(
        dim_freight_unmatched_df["Груз ЕТСНГ код"]
    )

    next_filter = report_df["Груз Наименование Полн след."].notnull() & report_df["Груз ЕТСНГ код след."].isnull()

    report_df.loc[next_filter, "Группа груза, номер след."] = report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Группа груза, номер"])
    report_df.loc[next_filter, "Группа груза Наименование Сокр след."] = report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Группа груза Наименование Сокр"])
    report_df.loc[next_filter, "Груз Наименование Сокр след."] = report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Груз Наименование Сокр"])
    report_df.loc[next_filter, "Груз Наименование Полн след."] = report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Груз Наименование Полн"])
    report_df.loc[next_filter, "Груз ЕТСНГ код след."] = report_df.loc[next_filter, "Наименование груза след."].map(
        dim_freight_unmatched_df["Груз ЕТСНГ код"]
    )

    print(report_df.shape)
    print(report_df.head())

    # Анализ пропусков
    pd.concat(
        [
            report_df.loc[
                report_df["Груз Наименование Полн тек."].notnull() & report_df["Груз ЕТСНГ код тек."].isnull(),
                ["Груз Наименование Полн тек.", "Отчетная дата"],
            ].rename(columns={"Груз Наименование Полн тек.": "Наименование груза"}),
            report_df.loc[
                report_df["Груз Наименование Полн след."].notnull() & report_df["Груз ЕТСНГ код след."].isnull(),
                ["Груз Наименование Полн след.", "Отчетная дата"],
            ].rename(columns={"Груз Наименование Полн след.": "Наименование груза"}),
        ]
    ).groupby(["Наименование груза"])[["Отчетная дата"]].count().rename(
        columns={"Отчетная дата": "Кол-во пропусков"}
    ).sort_values(
        by="Кол-во пропусков", ascending=False
    )

    # calculate the fact of daily downtime

    report_df["Факт ваг-сут простоя"] = report_df.apply(calc_downtime, axis=1)

    # report_df["Простои Факт, ваг-сут"] = report_df["Факт ваг-сут простоя"]
    # report_df["Простои Расчётный, ваг-сут"] = None
    report_df["Простои Факт ВПС, ваг-сут"] = None
    report_df["Признак: Используем в ВПС"] = None
    report_df["Простои Сдвоенные, ваг-сут"] = report_df["Ваг-сут простоя для сдвоенных"]
    # report_df["Простои Факт без округления, ваг-сут"] = None

    # Сохранение добавленных столбцов
    # report_df[report_columns].to_pickle(report_file)

    # report_df = report_df[report_columns].copy()

    print(report_df.shape)
    print(report_df.head())

    report_df.loc[
        report_df["Тип операции"] == "Выгрузка",
        [
            "Группа груза ГО, номер",
            "Группа груза ГО Наименование Сокр",
            "Груз ГО Наименование Сокр",
            "Груз ГО Наименование Полн",
            "Груз ЕТСНГ ГО код",
        ],
    ] = report_df.loc[
        report_df["Тип операции"] == "Выгрузка",
        [
            "Группа груза, номер тек.",
            "Группа груза Наименование Сокр тек.",
            "Груз Наименование Сокр тек.",
            "Груз Наименование Полн тек.",
            "Груз ЕТСНГ код тек.",
        ],
    ].to_numpy()

    report_df.loc[
        report_df["Тип операции"] == "Погрузка",
        [
            "Группа груза ГО, номер",
            "Группа груза ГО Наименование Сокр",
            "Груз ГО Наименование Сокр",
            "Груз ГО Наименование Полн",
            "Груз ЕТСНГ ГО код",
        ],
    ] = report_df.loc[
        report_df["Тип операции"] == "Погрузка",
        [
            "Группа груза, номер след.",
            "Группа груза Наименование Сокр след.",
            "Груз Наименование Сокр след.",
            "Груз Наименование Полн след.",
            "Груз ЕТСНГ код след.",
        ],
    ].to_numpy()

    print(report_df.shape)
    print(report_df.head())

    report_df.rename(columns=MAPPING_NAME_COGNOS, inplace=True)
    report_df["load_from"] = "Cognos"
    report_df = add_info_by_station_cod(engine_ora, report_df)

    # delete previously uploaded records from this period
    deleted_rec = (
        delete_facts(
            db=db, date_min=report_df["date_rep"].min(), date_max=report_df["date_rep"].max(), load_from="Cognos"
        )
        if is_overwrite
        else 0
    )

    # report_columns = ['date_rep', 'load_from', 'st_code', 'org_id', 'sender_cod', 'receiver_cod', 'client_sap_id',
    #                   'type_op', 'wagon_num', 'rps_short', 'cargo_group_num', 'double_operation', 'parking_fact']
    # report_columns = ['date_rep', 'load_from', 'st_code', 'org_id', 'client_sap_id',
    #                   'type_op', 'wagon_num', 'rps_short', 'cargo_group_num', 'parking_fact']
    report_df.drop("id", axis=1, inplace=True)
    # report_df = report_df[report_columns]
    save_df_to_model_via_csv(engine=engine, df=report_df, cols=report_df.columns, model_class=Fact)
    # df_to_new_table(db, engine, report_df, table_name="fact_cognos")
    return f"Added {len(report_df.index)} records {deleted_rec}."


def load_sap_file(db: Session, engine: Engine, engine_ora: Engine, uploaded_file: UploadFile, is_overwrite=True):
    usecols = [
        "Отчётная дата",
        "Код станции ГО",
        "Станция выполнения ГО",
        "Станция отправления тек.",
        "Станция назначения след.",
        "Дорога выполнения ГО",
        "Грузоотправитель на станции выполнения ГО ",  # Пробел на конце!
        "Грузополучатель на станции выполнения ГО ",  # Пробел на конце!
        "id клиента SAP",
        "Наименование клиента",
        "Тип операции",
        "№ вагона",
        "Род вагона",
        "№ накладной тек.",
        "Наименование груза тек.",
        "Код груза ЕТСНГ тек.",
        "Дата прибытия тек.",
        "№ накладной след.",
        "Наименование груза след.",
        "Код груза ЕТСНГ след.",
        "Дата приема след.",
        "Сдвоенная\nоперация",  # Символ новой строки!
        "Факт ваг-сут\nпростоя",  # Символ новой строки!
        "Ваг-сут простоя\nдля сдвоенных",  # Символ новой строки!
    ]
    content = uploaded_file.read()  # async read

    report_df = read_excel_with_find_headers(content=content, headers_list=usecols, skip_footer=1)
    report_df.columns = report_df.columns.str.replace("\n", " ").str.strip()

    print(report_df.shape)
    print(report_df.head(5))

    # Добавление незначащих нулей
    for col in ["Код станции ГО", "Код груза ЕТСНГ тек.", "Код груза ЕТСНГ след."]:
        # report_df[col] = report_df[col].astype("str")
        report_df.loc[report_df[col].notnull(), col] = (
            report_df.loc[report_df[col].notnull(), col].astype("str").str.split(".").str[0].str.zfill(6)
        )

    report_df["№ вагона"] = report_df["№ вагона"].astype("int").astype(str)
    report_df["Факт ваг-сут простоя"] = report_df["Факт ваг-сут простоя"].astype(float)
    # report_df["Факт ваг-сут простоя"] = report_df["Факт ваг-сут простоя"].astype(int)
    report_df["Ваг-сут простоя для сдвоенных"] = report_df["Ваг-сут простоя для сдвоенных"].astype(float)

    # Обработка "id клиента SAP"
    report_df.loc[report_df["id клиента SAP"].isnull(), "id клиента SAP"] = -1
    report_df["id клиента SAP"] = report_df["id клиента SAP"].astype("int")

    # Удаление незначащих пробелов
    report_df = report_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # report_df.to_pickle(report_clear_file)

    print(report_df.shape)
    print(report_df.head(5))

    # Добавление столбцов
    report_df = report_df.copy()
    report_columns = list()

    report_df["Отчётная дата"] = report_df["Отчётная дата"]
    report_df["Период"] = report_df["Отчётная дата"].dt.strftime("%Y-%m")

    report_columns += ["Отчётная дата", "Период"]

    print(report_df.shape)
    print(report_df.head(5))

    dim_st_rw_org_df = pd.read_sql(
        """
        SELECT
            s.ST_CODE, s.ST_NAME
            , vr.RW_CODE, vr.RW_SHORT_NAME, vr.RW_NAME
            , f.ORG_ID, f.SHORTNAME org_shortname, f.NAME org_name
        FROM ssp.STATIONS s
        INNER JOIN nsi.V_RAILWAY_SYSDATE vr
            ON s.ROADID = vr.RW_CODE
        INNER JOIN ssp.ORG_FILIAL f
            ON s.BRANCH_ID = f.ORG_ID AND vr.RW_CODE = f.RW_CODE
        """,
        con=engine_ora,
    )

    report_df = report_df.merge(dim_st_rw_org_df, how="left", left_on="Код станции ГО", right_on="st_code")

    report_df["Станция выполнения ГО код"] = report_df["Код станции ГО"]
    report_df["Станция выполнения ГО"] = report_df["Станция выполнения ГО"]

    report_df["Дорога выполнения ГО код"] = report_df["rw_code"]
    report_df["Дорога выполнения ГО Сокр"] = report_df["Дорога выполнения ГО"]
    report_df["Дорога выполнения ГО Полн"] = report_df["rw_name"]

    report_df["Филиал ГО ID"] = report_df["org_id"]
    report_df["Филиал ГО Сокр"] = report_df["org_shortname"]
    report_df["Филиал ГО Полн"] = report_df["org_name"]

    report_df = add_station_code_for_from_and_to(engine_ora, report_df)  # Get station code

    report_columns += [
        "Станция выполнения ГО код",
        # "Станция выполнения ГО",
        # "Дорога выполнения ГО код",
        # "Дорога выполнения ГО Сокр",
        # "Дорога выполнения ГО Полн",
        # "Филиал ГО ID",
        # "Филиал ГО Сокр",
        # "Филиал ГО Полн",
    ]

    print(report_df.shape)
    print(report_df.head(5))

    # Анализ пропусков
    report_df.loc[
        report_df["Дорога выполнения ГО код"].isnull(),
        # ["Станция выполнения ГО", "Код станции ГО"],
    ].groupby(["Станция выполнения ГО", "Код станции ГО"])["Отчётная дата"].count()

    report_df["Грузоотправитель на станции выполнения ГО код"] = ""
    report_df["Грузополучатель на станции выполнения ГО код"] = ""

    report_columns += [
        "Грузоотправитель на станции выполнения ГО код",
        "Грузоотправитель на станции выполнения ГО",
        "Грузополучатель на станции выполнения ГО код",
        "Грузополучатель на станции выполнения ГО",
    ]

    report_df["Клиент ID SAP"] = report_df["id клиента SAP"]
    report_df["Клиент Наименование"] = report_df["Наименование клиента"]

    report_columns += ["Клиент ID SAP", "Клиент Наименование"]

    report_df["Операция тип ВПС"] = ""
    report_df["Операция тип"] = report_df["Тип операции"]

    report_columns += ["Операция тип ВПС", "Операция тип"]

    # РПС
    dim_rod_df = pd.read_sql(
        """
    SELECT
        m.ROD_ID
        , m.SHORTNAME
        , m.NAME
    FROM ssp.MODELS m
    ORDER BY m.ROD_ID
    """,
        con=engine_ora,
    )

    report_df = report_df.merge(dim_rod_df, how="inner", left_on="Род вагона", right_on="name")

    report_df["Вагон №"] = report_df["№ вагона"]

    report_df["РПС код"] = report_df["rod_id"]
    report_df["РПС Наименование Сокр"] = report_df["shortname"]
    report_df["РПС Наименование Полн"] = report_df["name"]

    report_columns += ["Вагон №", "РПС код", "РПС Наименование Сокр", "РПС Наименование Полн"]

    print(report_df.shape)
    print(report_df.head(5))

    # Груз
    dim_freight_df = pd.read_sql(
        """
    SELECT
        t.FR_CODE_ETSNG, t.FR_SHORT_NAME, t.FR_NAME,
        t.GG_NUMBER, t.GG_NAME
    FROM (
        SELECT
            vfr.FR_CODE_ETSNG, vfr.FR_SHORT_NAME, vfr.FR_NAME,
            vgfr.GG_NUMBER, vgfr.GG_NAME,
            ROW_NUMBER() OVER(PARTITION BY vfr.FR_NAME ORDER BY vfr.RECDATEBEGIN DESC) rn
        FROM nsi.V_FREIGHT_SYSDATE_MOD11 vfr
        INNER JOIN nsi.V_SUM_FREIGHT_SYSDATE vgfr ON vfr.FR_GG_NUMBER = vgfr.GG_NUMBER
    ) t
    WHERE rn = 1
    """,
        con=engine_ora,
    )

    report_df = report_df.merge(
        dim_freight_df.add_prefix("cur_"), how="left", left_on="Код груза ЕТСНГ тек.", right_on="cur_fr_code_etsng"
    )
    report_df["Группа груза, номер тек."] = report_df["cur_gg_number"]
    report_df["Группа груза Наименование Сокр тек."] = report_df["cur_gg_name"]
    report_df["Груз Наименование Сокр тек."] = report_df["cur_fr_short_name"]
    report_df["Груз Наименование Полн тек."] = report_df["cur_fr_name"].fillna(report_df["Наименование груза тек."])
    report_df["Груз ЕТСНГ код тек."] = report_df["Код груза ЕТСНГ тек."]

    report_df = report_df.merge(
        dim_freight_df.add_prefix("next_"), how="left", left_on="Код груза ЕТСНГ след.", right_on="next_fr_code_etsng"
    )
    report_df["Группа груза, номер след."] = report_df["next_gg_number"]
    report_df["Группа груза Наименование Сокр след."] = report_df["next_gg_name"]
    report_df["Груз Наименование Сокр след."] = report_df["next_fr_short_name"]
    report_df["Груз Наименование Полн след."] = report_df["next_fr_name"].fillna(report_df["Наименование груза след."])
    report_df["Груз ЕТСНГ код след."] = report_df["Код груза ЕТСНГ след."]

    report_columns += [
        "№ накладной тек.",
        "Группа груза, номер тек.",
        "Группа груза Наименование Сокр тек.",
        "Груз Наименование Сокр тек.",
        "Груз Наименование Полн тек.",
        "Груз ЕТСНГ код тек.",
        "Дата прибытия тек.",
        "№ накладной след.",
        "Группа груза, номер след.",
        "Группа груза Наименование Сокр след.",
        "Груз Наименование Сокр след.",
        "Груз Наименование Полн след.",
        "Груз ЕТСНГ код след.",
        "Дата приема след.",
    ]

    print(report_df.shape)
    print(report_df.head(5))

    # Анализ пропусков
    pd.concat(
        [
            report_df.loc[
                report_df["Груз Наименование Полн тек."].notnull() & report_df["Груз ЕТСНГ код тек."].isnull(),
                ["Груз Наименование Полн тек.", "Отчётная дата"],
            ].rename(columns={"Груз Наименование Полн тек.": "Наименование груза"}),
            report_df.loc[
                report_df["Груз Наименование Полн след."].notnull() & report_df["Груз ЕТСНГ код след."].isnull(),
                ["Груз Наименование Полн след.", "Отчётная дата"],
            ].rename(columns={"Груз Наименование Полн след.": "Наименование груза"}),
        ]
    ).groupby(["Наименование груза"])[["Отчётная дата"]].count().rename(
        columns={"Отчётная дата": "Кол-во пропусков"}
    ).sort_values(
        by="Кол-во пропусков", ascending=False
    )

    # Сдвоенная опреация\Простои
    report_df["Простои Факт, ваг-сут"] = report_df["Факт ваг-сут простоя"]
    # report_df["Простои Расчётный, ваг-сут"] = None
    report_df["Простои Факт ВПС, ваг-сут"] = None
    report_df["Признак: Используем в ВПС"] = None
    report_df["Простои Сдвоенные, ваг-сут"] = report_df["Ваг-сут простоя для сдвоенных"]
    # report_df["Простои Факт без округления, ваг-сут"] = None

    report_columns += [
        "Сдвоенная операция",
        "Простои Факт, ваг-сут",
        # "Простои Расчётный, ваг-сут",
        "Простои Факт ВПС, ваг-сут",
        "Признак: Используем в ВПС",
        "Простои Сдвоенные, ваг-сут",
        # "Простои Факт без округления, ваг-сут",
        "st_code_from",
        "st_code_to",
    ]

    # Сохранение добавленных столбцов
    # report_df[report_columns].to_pickle(report_file)

    print(report_df[report_columns].shape)
    print(report_df[report_columns].head(5))

    report_df = report_df[report_columns].copy()

    # Груз от "Операция тип"
    report_df.loc[
        report_df["Операция тип"] == "Выгрузка",
        [
            "Группа груза ГО, номер",
            "Группа груза ГО Наименование Сокр",
            "Груз ГО Наименование Сокр",
            "Груз ГО Наименование Полн",
            "Груз ЕТСНГ ГО код",
        ],
    ] = report_df.loc[
        report_df["Операция тип"] == "Выгрузка",
        [
            "Группа груза, номер тек.",
            "Группа груза Наименование Сокр тек.",
            "Груз Наименование Сокр тек.",
            "Груз Наименование Полн тек.",
            "Груз ЕТСНГ код тек.",
        ],
    ].to_numpy()

    report_df.loc[
        report_df["Операция тип"] == "Погрузка",
        [
            "Группа груза ГО, номер",
            "Группа груза ГО Наименование Сокр",
            "Груз ГО Наименование Сокр",
            "Груз ГО Наименование Полн",
            "Груз ЕТСНГ ГО код",
        ],
    ] = report_df.loc[
        report_df["Операция тип"] == "Погрузка",
        [
            "Группа груза, номер след.",
            "Группа груза Наименование Сокр след.",
            "Груз Наименование Сокр след.",
            "Груз Наименование Полн след.",
            "Груз ЕТСНГ код след.",
        ],
    ].to_numpy()

    print(report_df.shape)
    print(report_df.head())

    # Сохраняем итоговый результат
    report_df.rename(columns=MAPPING_NAME_COGNOS, inplace=True)
    report_df = add_info_by_station_cod(engine_ora, report_df)
    report_df.drop_duplicates(
        subset=[
            "date_rep",
            "st_code",
            "st_code_from",
            "st_code_to",
            "org_id",
            "client_sap_id",
            "type_op",
            "wagon_num",
            "cargo_group_num",
        ],
        inplace=True,
    )
    # report_df['parking_fact'] = report_df['parking_fact'].apply(np.ceil)

    result_spr = add_sap_client_in_spr(db, engine, report_df)
    print(f"Обновление справочников({result_spr})")

    report_df["load_from"] = "SAP"
    print(f"После удаления дублей, осталось {report_df.shape[0]} rows")

    # delete previously uploaded records from this period
    deleted_rec = (
        delete_facts(db=db, date_min=report_df["date_rep"].min(), date_max=report_df["date_rep"].max(), load_from="SAP")
        if is_overwrite
        else 0
    )

    save_df_to_model_via_csv(engine=engine, df=report_df, cols=report_df.columns, model_class=Fact)
    # df_to_new_table(db, engine, report_df, table_name="fact_sap")
    return f"Added {len(report_df.index)} records {deleted_rec}. Обновление справочников ({result_spr})."


def add_info_by_station_cod(engine_ora: Engine, df: DataFrame):
    dim_st_rw_org_df = pd.read_sql(
        """
    SELECT
        s.ST_CODE, s.ST_NAME
        , vr.RW_CODE, vr.RW_SHORT_NAME, vr.RW_NAME
        , f.ORG_ID, f.SHORTNAME org_shortname, f.NAME org_name
    FROM ssp.STATIONS s
    INNER JOIN nsi.V_RAILWAY_SYSDATE vr
        ON s.ROADID = vr.RW_CODE
    INNER JOIN ssp.ORG_FILIAL f
        ON s.BRANCH_ID = f.ORG_ID AND vr.RW_CODE = f.RW_CODE
    """,
        con=engine_ora,
    )

    report_df = df.merge(dim_st_rw_org_df, how="left", left_on="st_code", right_on="st_code")

    return report_df


def add_station_code_for_from_and_to(engine_ora: Engine, df: DataFrame):
    station_df = df[["Код станции ГО", "Станция выполнения ГО"]].rename(
        columns={"Код станции ГО": "st_code", "Станция выполнения ГО": "st_name"}
    )
    station_df.drop_duplicates(subset=["st_code"], inplace=True)
    dim_station_df = pd.read_sql("SELECT st_name, st_code FROM ssp.STATIONS", con=engine_ora)
    dim_station_df = pd.concat([station_df, dim_station_df], ignore_index=True, sort=False)
    dim_station_df.drop_duplicates(subset=["st_name"], inplace=True)

    df = df.merge(
        dim_station_df.add_prefix("from_"), how="left", left_on="Станция отправления тек.", right_on="from_st_name"
    )
    df = df.merge(
        dim_station_df.add_prefix("to_"), how="left", left_on="Станция назначения след.", right_on="to_st_name"
    )
    df.rename(columns={"from_st_code": "st_code_from", "to_st_code": "st_code_to"}, inplace=True)

    df.loc[df["Тип операции"] == "Погрузка", "st_code_from"] = df["Код станции ГО"]
    df.loc[df["Тип операции"] == "Выгрузка", "st_code_to"] = df["Код станции ГО"]

    df.loc[df["st_code_from"].isnull(), "st_code_from"] = df["Станция отправления тек."]
    df.loc[df["st_code_to"].isnull(), "st_code_to"] = df["Станция назначения след."]

    # "Станция выполнения ГО",
    return df


def add_sap_client_in_spr(db: Session, engine: Engine, df: DataFrame):
    client_df = df[["client_sap_id", "client"]]
    client_df.drop_duplicates(subset=["client_sap_id", "client"], inplace=True)
    mapping_df = pd.read_sql("""select * from mapping_client_cognos_sap;""", con=engine)
    # remove old records of DB
    client_df["client_sap_id"] = client_df["client_sap_id"].astype(str)  # int64 --> str
    client_df = client_df.merge(
        mapping_df.add_prefix("_"), how="left", left_on="client_sap_id", right_on="_client_sap_id"
    )

    # add updates row
    client_df["_id"] = client_df["_id"].fillna(0)
    filter_df = (client_df["_id"] == 0) | (client_df["client"] != client_df["_client"])
    client_df = client_df.loc[filter_df]

    # save_df_to_model_via_csv(engine=engine, df=client_df, cols=client_df.columns, model_class=MappingClientCognosToSAP)
    result = save_df_with_unique(
        db,
        engine,
        "mapping_client_cognos_sap",
        client_df,
        cols=["client_sap_id", "client"],
        unique_cols=["client_sap_id"],
        is_update_exist=True,
    )
    return result


def add_info_by_client_sap_id(engine: Engine, df: DataFrame):
    # НЕВЕРНО!!! нужно из оракла брать
    mapping_df = pd.read_sql("""select * from mapping_client_cognos_sap;""", con=engine)
    new_df = df.merge(mapping_df, how="left", left_on="client_sap_id", right_on="client_sap_id")
    return new_df


def load_mapping_client_cognos_sap(db: Session, engine: Engine, file_name: str):
    content_path = OsCls.get_import_path("content")
    dim_clients_df = (
        pd.read_excel(
            OsCls.join_path(content_path, "Маппинг Клиенты Cognos to SAP.xlsx"),
            skiprows=1,
            dtype={
                "ID ASU text": "string",
                "ID SAP text": "string",
                "ID ASU code": "string",
            },
            usecols=["ID ASU text", "ID SAP text", "Сокр Клиент"],
        )
        .rename(columns={"ID ASU text": "client_cognos_id", "ID SAP text": "client_sap_id", "Сокр Клиент": "client"})
        .dropna(subset=["client_cognos_id"])
    )

    print(dim_clients_df.shape)
    print(dim_clients_df.head())

    return dim_clients_df

    dim_extra_clients_df = pd.read_excel(
        OsCls.join_path(content_path, "Нашли 80 прц Клиентов по Объёму.xlsx"),
        sheet_name="Маппинг Cognos to SAP",
        usecols=["Cognos ID", "SAP ID", "SAP Наименование Сокр", "Мрк"],
        dtype={
            "Cognos ID": "string",
            "SAP ID": "string",
        },
    ).rename(
        columns={
            "Cognos ID": "cognos_client_id",
            "SAP ID": "sap_client_id",
            "SAP Наименование Сокр": "Сокр Клиент",
        }
    )

    dim_extra_clients_df = (
        dim_extra_clients_df.loc[
            dim_extra_clients_df["Мрк"].notnull() & dim_extra_clients_df["cognos_client_id"].notnull()
        ].drop(columns=["Мрк"])
        # .astype(dtype={"cognos_client_id": "int"})
    )

    print(dim_extra_clients_df.shape)
    print(dim_extra_clients_df.head())

    report_df = report_df.astype(dtype={"Id клиента": "string"}).merge(
        pd.concat([dim_clients_df, dim_extra_clients_df]).rename(
            columns={"sap_client_id": "Клиент ID SAP", "Сокр Клиент": "Клиент Наименование"}
        ),
        how="inner",
        left_on="Id клиента",
        right_on="cognos_client_id",
    )

    report_columns += ["Клиент ID SAP", "Клиент Наименование"]

    print(report_df.shape)
    print(report_df.head(5))


async def load_season_coefficient_old(
    db: Session, engine: Engine, season_coefficient_name: str, uploaded_file: UploadFile
):
    content = await uploaded_file.read()  # async read
    report_df = pd.read_excel(content)

    type_operation_df = pd.read_sql("select name as type_operation, id from type_operation", con=engine)
    print(type_operation_df)
    report_df = report_df.merge(type_operation_df, how="inner", left_on="Тип операции", right_on="type_operation")
    report_df.rename(columns=MAPPING_SEASONAL_COEFFICIENT, inplace=True)

    seasonal_coefficient = SeasonCoefficientCreate(name=season_coefficient_name)
    season_coefficient_db = create_season_coefficient(db, seasonal_coefficient)
    report_df["head_id"] = season_coefficient_db.id

    cols = ["head_id", "rps_short", "type_operation_id"] + [f"Coefficient_{index:02}" for index in range(1, 13)]
    season_coefficient_body_list = [SeasonCoefficientBodyCreate(**el) for el in report_df[cols].to_dict("records")]
    result = create_season_coefficient_body_list(db, season_coefficient_body_list)

    return result

    # columns_for_rename = {f"СК{index:02}": f"Coefficient_{index:02}" for index in range(1, 13)}
    # report_df.rename(columns=columns_for_rename, inplace=True)

    return f"Added {len(report_df.index)} records."
    return report_df[cols]

    type_operation_df = get_season_coefficient_body_df(postgre_eng, parameters.seasonal_coefficient_id)
    season_coeffs_df.rename(
        columns={"rps_short": "РПС Наименование Сокр", "type_operation": "Операция тип"}, inplace=True
    )

    report_df = report_df.reset_index().merge(season_coeffs_df, on=["РПС Наименование Сокр", "Операция тип"])

    report_df.rename(columns=MAPPING_SEASONAL_COEFFICIENT, inplace=True)

    # season_coeffs_df = pd.read_excel("W:/PoC проекты/2022-01 Расчёт целевых ТОУ/01 Материалы от Заказчика/СК 2022.XLSX")
    # season_coeffs_df = season_coeffs_df.rename(
    #     columns={"Род вагона": "РПС Наименование Сокр", "Тип операции": "Операция тип"})
    print(report_df)
    return f"Added {len(report_df.index)} records."

    # report_df.columns = report_df.columns.str.strip()
    # print(report_df.shape)
    # print(report_df.head(5))

    # sc_list = []
    # writer = pd.ExcelWriter("СК.xlsx", engine="xlsxwriter")
    #
    # for p in ["1y", "2y", "3y"]:
    #     sc_list.append(
    #         sc_df.pivot(
    #             index=["РПС Наименование Сокр", "Операция тип"],
    #             columns=["month"],
    #             values=f"idle_{p}",
    #         )
    #         .rename_axis(None, axis=1)
    #         .rename(columns={i: f"СК{i:02d}" for i in range(1, 13)})
    #         # .reset_index()
    #     )
    #
    #     sc_list[-1].to_excel(writer, sheet_name=p, startrow=0, header=True, index=True)
    #
    # writer.save()
    # writer.close()
