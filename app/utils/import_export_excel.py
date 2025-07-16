from io import StringIO

import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from tqdm import tqdm

from app.core.crud import delete_facts
from app.core.models import Fact
from app.utils.utils_df import MAPPING_NAME_COGNOS
from app.utils.utils_os import OsCls


def load_cognos_files(db: Session, engine: Engine, engine_ora: Engine, files_list: list, is_overwrite=True):
    report_list = list()
    for report in tqdm(files_list):
        content = report.file.read()
        report_df = pd.read_excel(
            content,
            usecols=[
                "Отчетная дата",
                "Код станции ГО",
                "Станция выполнения ГО",
                "Дорога выполнения ГО",
                "Грузоотправитель на станции выполнения ГО ",  # Пробел на конце!
                "Грузополучатель на станции выполнения ГО",
                "Id клиента",
                "Наименование клиента",
                "Тип операции",
                "№ вагона",
                "Род вагона",
                "№ накладной тек.",
                "Наименование груза тек.",
                "Дата прибытия тек.",
                "№ накладной след.",
                "Наименование груза след.",
                "Дата приема след.",
                "Сдвоенная операция",
                "Факт ваг-сут простоя",
                "Ваг-сут простоя для сдвоенных",
            ],
        )
        report_df.columns = report_df.columns.str.strip()
        report_list.append(report_df.copy())

    raw_report_df = pd.concat(report_list)
    print(raw_report_df.shape)
    print(raw_report_df.head(5))
    # raw_report_df.to_pickle(report_raw_file)

    # Raw data processing
    clear_report_df = raw_report_df.copy()

    # Обработка "Код станции ГО"
    clear_report_df.loc[clear_report_df["Код станции ГО"].notnull(), "Код станции ГО"] = clear_report_df.loc[
        clear_report_df["Код станции ГО"].notnull(), "Код станции ГО"
    ].apply(lambda code: str(int(code)).zfill(5))

    station_df = clear_report_df[["Станция выполнения ГО", "Код станции ГО"]].groupby(["Код станции ГО"]).first()
    # save_df_to_model_via_csv(engine=engine, df=station_df, model_class=Station)

    # Обработка "Id клиента"
    clear_report_df.loc[clear_report_df["Id клиента"].isnull(), "Id клиента"] = -1
    clear_report_df["Id клиента"] = clear_report_df["Id клиента"].astype("int")

    # Удаление незначащих пробелов
    clear_report_df = clear_report_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    print(clear_report_df.shape)
    print(clear_report_df.head(5))
    # clear_report_df.to_pickle(report_clear_file)

    # Adding columns

    new_report_df = clear_report_df.copy()
    report_columns = list()
    new_report_df["Отчётная дата"] = new_report_df["Отчетная дата"]
    new_report_df["Период"] = new_report_df["Отчётная дата"].dt.strftime("%Y-%m")
    report_columns += ["Отчётная дата", "Период"]

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

    new_report_df["Станция выполнения ГО код"] = new_report_df["Код станции ГО"].map(
        dim_station_df.set_index("st_code5")["st_code6"]
    )

    new_report_df["Грузоотправитель на станции выполнения ГО код"] = ""
    new_report_df["Грузополучатель на станции выполнения ГО код"] = ""

    report_columns += [
        "Станция выполнения ГО код",
        "Грузоотправитель на станции выполнения ГО код",
        "Грузоотправитель на станции выполнения ГО",
        "Грузополучатель на станции выполнения ГО код",
        "Грузополучатель на станции выполнения ГО",
    ]

    mapping_df = pd.read_sql("""select * from mapping_client_cognos_sap""", con=engine)
    new_report_df = new_report_df.astype(dtype={"Id клиента": "string"}).merge(
        mapping_df,
        how="inner",
        left_on="Id клиента",
        right_on="client_cognos_id",
    )

    report_columns += ["client_sap_id", "client"]

    print(new_report_df.shape)
    print(new_report_df.head(5))

    new_report_df["Операция тип ВПС"] = ""
    new_report_df["Операция тип"] = new_report_df["Тип операции"]

    report_columns += ["Операция тип ВПС", "Операция тип"]

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

    new_report_df = new_report_df.merge(dim_rod_df, how="inner", left_on="Род вагона", right_on="shortname")

    new_report_df["Вагон №"] = new_report_df["№ вагона"]

    new_report_df["РПС код"] = new_report_df["rod_id"]
    new_report_df["РПС Наименование Сокр"] = new_report_df["shortname"]
    new_report_df["РПС Наименование Полн"] = new_report_df["name"]

    report_columns += ["Вагон №", "РПС код", "РПС Наименование Сокр", "РПС Наименование Полн"]

    print(new_report_df.shape)
    print(new_report_df.head(5))

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

    new_report_df = new_report_df.merge(
        dim_freight_df.add_prefix("cur_"),
        how="left",
        left_on="Наименование груза тек.",
        right_on="cur_Наименование груза",
    )
    new_report_df["Группа груза, номер тек."] = new_report_df["cur_gg_number"]
    new_report_df["Группа груза Наименование Сокр тек."] = new_report_df["cur_gg_name"]
    new_report_df["Груз Наименование Сокр тек."] = new_report_df["cur_fr_short_name"]
    new_report_df["Груз Наименование Полн тек."] = new_report_df["cur_fr_name"].fillna(
        new_report_df["Наименование груза тек."]
    )
    new_report_df["Груз ЕТСНГ код тек."] = new_report_df["cur_fr_code_etsng"]

    new_report_df = new_report_df.merge(
        dim_freight_df.add_prefix("next_"),
        how="left",
        left_on="Наименование груза след.",
        right_on="next_Наименование груза",
    )
    new_report_df["Группа груза, номер след."] = new_report_df["next_gg_number"]
    new_report_df["Группа груза Наименование Сокр след."] = new_report_df["next_gg_name"]
    new_report_df["Груз Наименование Сокр след."] = new_report_df["next_fr_short_name"]
    new_report_df["Груз Наименование Полн след."] = new_report_df["next_fr_name"].fillna(
        new_report_df["Наименование груза след."]
    )
    new_report_df["Груз ЕТСНГ код след."] = new_report_df["next_fr_code_etsng"]

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

    print(new_report_df.shape)
    print(new_report_df.head(5))

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

    dim_freight_unmatched_df

    cur_filter = new_report_df["Груз Наименование Полн тек."].notnull() & new_report_df["Груз ЕТСНГ код тек."].isnull()

    new_report_df.loc[cur_filter, "Группа груза, номер тек."] = new_report_df.loc[
        cur_filter, "Наименование груза тек."
    ].map(dim_freight_unmatched_df["Группа груза, номер"])
    new_report_df.loc[cur_filter, "Группа груза Наименование Сокр тек."] = new_report_df.loc[
        cur_filter, "Наименование груза тек."
    ].map(dim_freight_unmatched_df["Группа груза Наименование Сокр"])
    new_report_df.loc[cur_filter, "Груз Наименование Сокр тек."] = new_report_df.loc[
        cur_filter, "Наименование груза тек."
    ].map(dim_freight_unmatched_df["Груз Наименование Сокр"])
    new_report_df.loc[cur_filter, "Груз Наименование Полн тек."] = new_report_df.loc[
        cur_filter, "Наименование груза тек."
    ].map(dim_freight_unmatched_df["Груз Наименование Полн"])
    new_report_df.loc[cur_filter, "Груз ЕТСНГ код тек."] = new_report_df.loc[cur_filter, "Наименование груза тек."].map(
        dim_freight_unmatched_df["Груз ЕТСНГ код"]
    )

    next_filter = (
        new_report_df["Груз Наименование Полн след."].notnull() & new_report_df["Груз ЕТСНГ код след."].isnull()
    )

    new_report_df.loc[next_filter, "Группа груза, номер след."] = new_report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Группа груза, номер"])
    new_report_df.loc[next_filter, "Группа груза Наименование Сокр след."] = new_report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Группа груза Наименование Сокр"])
    new_report_df.loc[next_filter, "Груз Наименование Сокр след."] = new_report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Груз Наименование Сокр"])
    new_report_df.loc[next_filter, "Груз Наименование Полн след."] = new_report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Груз Наименование Полн"])
    new_report_df.loc[next_filter, "Груз ЕТСНГ код след."] = new_report_df.loc[
        next_filter, "Наименование груза след."
    ].map(dim_freight_unmatched_df["Груз ЕТСНГ код"])

    print(new_report_df.shape)
    print(new_report_df.head())

    # Анализ пропусков
    pd.concat(
        [
            new_report_df.loc[
                new_report_df["Груз Наименование Полн тек."].notnull() & new_report_df["Груз ЕТСНГ код тек."].isnull(),
                ["Груз Наименование Полн тек.", "Отчетная дата"],
            ].rename(columns={"Груз Наименование Полн тек.": "Наименование груза"}),
            new_report_df.loc[
                new_report_df["Груз Наименование Полн след."].notnull()
                & new_report_df["Груз ЕТСНГ код след."].isnull(),
                ["Груз Наименование Полн след.", "Отчетная дата"],
            ].rename(columns={"Груз Наименование Полн след.": "Наименование груза"}),
        ]
    ).groupby(["Наименование груза"])[["Отчетная дата"]].count().rename(
        columns={"Отчетная дата": "Кол-во пропусков"}
    ).sort_values(
        by="Кол-во пропусков", ascending=False
    )

    new_report_df["Простои Факт, ваг-сут"] = new_report_df["Факт ваг-сут простоя"]
    # new_report_df["Простои Расчётный, ваг-сут"] = None
    new_report_df["Простои Факт ВПС, ваг-сут"] = None
    new_report_df["Признак: Используем в ВПС"] = None
    new_report_df["Простои Сдвоенные, ваг-сут"] = new_report_df["Ваг-сут простоя для сдвоенных"]
    # new_report_df["Простои Факт без округления, ваг-сут"] = None

    report_columns += [
        "Сдвоенная операция",
        "Простои Факт, ваг-сут",
        # "Простои Расчётный, ваг-сут",
        "Простои Факт ВПС, ваг-сут",
        "Признак: Используем в ВПС",
        "Простои Сдвоенные, ваг-сут",
        # "Простои Факт без округления, ваг-сут",
    ]

    # Сохранение добавленных столбцов
    # new_report_df[report_columns].to_pickle(report_file)

    print(new_report_df[report_columns].shape)
    print(new_report_df[report_columns].head(5))

    report_df = new_report_df[report_columns].copy()

    print(report_df.shape)
    print(report_df.head())

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

    save_df_to_model_via_csv(engine=engine, df=report_df, cols=report_df.columns, model_class=Fact)
    # df_to_new_table(db, engine, report_df, table_name="fact_cognos")
    return f"Added {len(report_df.index)} records {deleted_rec}."


def load_sap_files(db: Session, engine: Engine, engine_ora: Engine, files_list: list, is_overwrite=True):
    report_list = list()
    for report in tqdm(files_list):
        content = report.file.read()
        report_df = pd.read_excel(
            content,
            skiprows=12,
            skipfooter=1,
            engine="openpyxl",
            usecols=[
                "Отчётная дата",
                "Код станции ГО",
                "Станция выполнения ГО",
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
            ],
        )
        report_df.columns = report_df.columns.str.replace("\n", " ").str.strip()
        report_list.append(report_df.copy())

    if not report_list:
        return
    raw_report_df = pd.concat(report_list)
    # raw_report_df.to_pickle(report_raw_file)

    print(raw_report_df.shape)
    print(raw_report_df.head(5))

    # Обработка сырых данных
    clear_report_df = raw_report_df.copy()

    # Добавление незначащих нулей
    for col in ["Код станции ГО", "Код груза ЕТСНГ тек.", "Код груза ЕТСНГ след."]:
        # clear_report_df[col] = clear_report_df[col].astype("str")
        clear_report_df.loc[clear_report_df[col].notnull(), col] = (
            clear_report_df.loc[clear_report_df[col].notnull(), col].astype("str").str.split(".").str[0].str.zfill(6)
        )

    clear_report_df["№ вагона"] = clear_report_df["№ вагона"].astype("int").astype(str)
    clear_report_df["Факт ваг-сут простоя"] = clear_report_df["Факт ваг-сут простоя"].astype(float)
    clear_report_df["Ваг-сут простоя для сдвоенных"] = clear_report_df["Ваг-сут простоя для сдвоенных"].astype(float)

    # Обработка "id клиента SAP"
    clear_report_df.loc[clear_report_df["id клиента SAP"].isnull(), "id клиента SAP"] = -1
    clear_report_df["id клиента SAP"] = clear_report_df["id клиента SAP"].astype("int")

    # Удаление незначащих пробелов
    clear_report_df = clear_report_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # clear_report_df.to_pickle(report_clear_file)

    print(clear_report_df.shape)
    print(clear_report_df.head(5))

    # Добавление столбцов
    new_report_df = clear_report_df.copy()
    report_columns = list()

    new_report_df["Отчётная дата"] = new_report_df["Отчётная дата"]
    new_report_df["Период"] = new_report_df["Отчётная дата"].dt.strftime("%Y-%m")

    report_columns += ["Отчётная дата", "Период"]

    print(new_report_df.shape)
    print(new_report_df.head(5))

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

    new_report_df = new_report_df.merge(dim_st_rw_org_df, how="left", left_on="Код станции ГО", right_on="st_code")

    new_report_df["Станция выполнения ГО код"] = new_report_df["Код станции ГО"]
    new_report_df["Станция выполнения ГО"] = new_report_df["Станция выполнения ГО"]

    new_report_df["Дорога выполнения ГО код"] = new_report_df["rw_code"]
    new_report_df["Дорога выполнения ГО Сокр"] = new_report_df["Дорога выполнения ГО"]
    new_report_df["Дорога выполнения ГО Полн"] = new_report_df["rw_name"]

    new_report_df["Филиал ГО ID"] = new_report_df["org_id"]
    new_report_df["Филиал ГО Сокр"] = new_report_df["org_shortname"]
    new_report_df["Филиал ГО Полн"] = new_report_df["org_name"]

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

    print(new_report_df.shape)
    print(new_report_df.head(5))

    # Анализ пропусков
    new_report_df.loc[
        new_report_df["Дорога выполнения ГО код"].isnull(),
        # ["Станция выполнения ГО", "Код станции ГО"],
    ].groupby(["Станция выполнения ГО", "Код станции ГО"])["Отчётная дата"].count()

    new_report_df["Грузоотправитель на станции выполнения ГО код"] = ""
    new_report_df["Грузополучатель на станции выполнения ГО код"] = ""

    report_columns += [
        "Грузоотправитель на станции выполнения ГО код",
        "Грузоотправитель на станции выполнения ГО",
        "Грузополучатель на станции выполнения ГО код",
        "Грузополучатель на станции выполнения ГО",
    ]

    new_report_df["Клиент ID SAP"] = new_report_df["id клиента SAP"]
    new_report_df["Клиент Наименование"] = new_report_df["Наименование клиента"]

    report_columns += ["Клиент ID SAP", "Клиент Наименование"]

    new_report_df["Операция тип ВПС"] = ""
    new_report_df["Операция тип"] = new_report_df["Тип операции"]

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

    new_report_df = new_report_df.merge(dim_rod_df, how="inner", left_on="Род вагона", right_on="name")

    new_report_df["Вагон №"] = new_report_df["№ вагона"]

    new_report_df["РПС код"] = new_report_df["rod_id"]
    new_report_df["РПС Наименование Сокр"] = new_report_df["shortname"]
    new_report_df["РПС Наименование Полн"] = new_report_df["name"]

    report_columns += ["Вагон №", "РПС код", "РПС Наименование Сокр", "РПС Наименование Полн"]

    print(new_report_df.shape)
    print(new_report_df.head(5))

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

    new_report_df = new_report_df.merge(
        dim_freight_df.add_prefix("cur_"), how="left", left_on="Код груза ЕТСНГ тек.", right_on="cur_fr_code_etsng"
    )
    new_report_df["Группа груза, номер тек."] = new_report_df["cur_gg_number"]
    new_report_df["Группа груза Наименование Сокр тек."] = new_report_df["cur_gg_name"]
    new_report_df["Груз Наименование Сокр тек."] = new_report_df["cur_fr_short_name"]
    new_report_df["Груз Наименование Полн тек."] = new_report_df["cur_fr_name"].fillna(
        new_report_df["Наименование груза тек."]
    )
    new_report_df["Груз ЕТСНГ код тек."] = new_report_df["Код груза ЕТСНГ тек."]

    new_report_df = new_report_df.merge(
        dim_freight_df.add_prefix("next_"), how="left", left_on="Код груза ЕТСНГ след.", right_on="next_fr_code_etsng"
    )
    new_report_df["Группа груза, номер след."] = new_report_df["next_gg_number"]
    new_report_df["Группа груза Наименование Сокр след."] = new_report_df["next_gg_name"]
    new_report_df["Груз Наименование Сокр след."] = new_report_df["next_fr_short_name"]
    new_report_df["Груз Наименование Полн след."] = new_report_df["next_fr_name"].fillna(
        new_report_df["Наименование груза след."]
    )
    new_report_df["Груз ЕТСНГ код след."] = new_report_df["Код груза ЕТСНГ след."]

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

    print(new_report_df.shape)
    print(new_report_df.head(5))

    # Анализ пропусков
    pd.concat(
        [
            new_report_df.loc[
                new_report_df["Груз Наименование Полн тек."].notnull() & new_report_df["Груз ЕТСНГ код тек."].isnull(),
                ["Груз Наименование Полн тек.", "Отчётная дата"],
            ].rename(columns={"Груз Наименование Полн тек.": "Наименование груза"}),
            new_report_df.loc[
                new_report_df["Груз Наименование Полн след."].notnull()
                & new_report_df["Груз ЕТСНГ код след."].isnull(),
                ["Груз Наименование Полн след.", "Отчётная дата"],
            ].rename(columns={"Груз Наименование Полн след.": "Наименование груза"}),
        ]
    ).groupby(["Наименование груза"])[["Отчётная дата"]].count().rename(
        columns={"Отчётная дата": "Кол-во пропусков"}
    ).sort_values(
        by="Кол-во пропусков", ascending=False
    )

    # Сдвоенная опреация\Простои
    new_report_df["Простои Факт, ваг-сут"] = new_report_df["Факт ваг-сут простоя"]
    # new_report_df["Простои Расчётный, ваг-сут"] = None
    new_report_df["Простои Факт ВПС, ваг-сут"] = None
    new_report_df["Признак: Используем в ВПС"] = None
    new_report_df["Простои Сдвоенные, ваг-сут"] = new_report_df["Ваг-сут простоя для сдвоенных"]
    # new_report_df["Простои Факт без округления, ваг-сут"] = None

    report_columns += [
        "Сдвоенная операция",
        "Простои Факт, ваг-сут",
        # "Простои Расчётный, ваг-сут",
        "Простои Факт ВПС, ваг-сут",
        "Признак: Используем в ВПС",
        "Простои Сдвоенные, ваг-сут",
        # "Простои Факт без округления, ваг-сут",
    ]

    # Сохранение добавленных столбцов
    # new_report_df[report_columns].to_pickle(report_file)

    print(new_report_df[report_columns].shape)
    print(new_report_df[report_columns].head(5))

    report_df = new_report_df[report_columns].copy()

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
    report_df["load_from"] = "SAP"
    report_df = add_info_by_station_cod(engine_ora, report_df)

    # delete previously uploaded records from this period
    deleted_rec = (
        delete_facts(db=db, date_min=report_df["date_rep"].min(), date_max=report_df["date_rep"].max(), load_from="SAP")
        if is_overwrite
        else 0
    )

    save_df_to_model_via_csv(engine=engine, df=report_df, cols=report_df.columns, model_class=Fact)
    # df_to_new_table(db, engine, report_df, table_name="fact_sap")
    return f"Added {len(report_df.index)} records {deleted_rec}."


def save_df_to_model_via_csv(engine: Engine, df, cols, model_class=None, db_table=None):
    # fastest way to insert into model raw data via CSV file
    assert model_class or db_table, "model_class or db_table should be provided"
    if model_class:
        db_table = model_class.__tablename__
        cols_from_model = set(map(lambda x: str(x).split(".")[-1], model_class.__table__.columns))
        cols = list(cols_from_model.intersection(cols))

        for col in cols:
            if str(model_class.__dict__[col].type).startswith("VARCHAR("):
                df[col] = df[col].fillna("")
                max_len_db_col = model_class.__dict__[col].type.length
                max_len_df_col = max(df[col].apply(str).apply(len))
                if max_len_df_col > max_len_db_col:
                    df[col] = df[col].str.slice(0, max_len_db_col - 1)
                    print(f"{col} max len ={max_len_df_col}")

    output = StringIO()
    df[cols].to_csv(output, sep="\t", header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    fake_conn = engine.raw_connection()
    fake_cur = fake_conn.cursor()
    fake_cur.copy_from(output, db_table, null="", columns=cols)
    fake_conn.commit()


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

    new_report_df = df.merge(dim_st_rw_org_df, how="left", left_on="st_code", right_on="st_code")
    return new_report_df


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

    new_report_df = new_report_df.astype(dtype={"Id клиента": "string"}).merge(
        pd.concat([dim_clients_df, dim_extra_clients_df]).rename(
            columns={"sap_client_id": "Клиент ID SAP", "Сокр Клиент": "Клиент Наименование"}
        ),
        how="inner",
        left_on="Id клиента",
        right_on="cognos_client_id",
    )

    report_columns += ["Клиент ID SAP", "Клиент Наименование"]

    print(new_report_df.shape)
    print(new_report_df.head(5))
