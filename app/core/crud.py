import datetime
from calendar import monthrange
from time import sleep
from typing import Optional

import pandas as pd
from fastapi import HTTPException, UploadFile
from pandas import DataFrame
from psycopg2 import Date
from sqlalchemy import and_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..api.deps import get_db
from ..settings import (
    AMOUNT_OPERATION,
    CALC_STATUSES,
    CALC_TYPE_MERGE,
    PARSED_CONFIG,
    AmountOperationEnum,
    CalcStateEnum,
    CalcTypeMergeEnum,
    MyLogTypeEnum,
)
from ..utils.utils import get_info_from_excel, table_writer
from ..utils.utils_df import MAPPING_SEASONAL_COEFFICIENT, MAPPING_SEASONAL_COEFFICIENT_REVERSE
from . import models, schemas
from .schemas import SeasonCoefficientBodyCreate, SeasonCoefficientCreate


# db_local = next(get_db())


def create_rps(db: Session, rps: schemas.RpsCreate):
    db_rps = models.Rps(**rps.dict())
    db.add(db_rps)
    db.commit()
    db.refresh(db_rps)
    return db_rps


def get_rps_list(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Rps).offset(skip).limit(limit).all()


def get_rps(db: Session, rps_short: str):
    return db.query(models.Rps).filter(models.Rps.rps_short == rps_short).first()


def delete_rps(db: Session, rps_short: str):
    db_rps = get_rps(db, rps_short)
    db.delete(db_rps)
    db.commit()
    return {"message": "OK"}


def import_rps_from_ora(db: Session, engine: Engine, engine_ora: Engine):
    rps_df = pd.read_sql(
        """
    SELECT
        m.ROD_ID
        , m.SHORTNAME
        , m.NAME
    FROM ssp.MODELS m
    ORDER BY m.ROD_ID
    """,
        con=engine_ora,
    ).rename(columns={"rod_id": "rps_cod", "shortname": "rps_short", "name": "rps"})

    rps_df.to_sql("rps_tmp", con=engine, if_exists="replace", index=False)
    # Update an existing
    updated = db.execute(
        "UPDATE rps AS r "
        "SET rps_short = t.rps_short, rps_cod = t.rps_cod, rps = t.rps "
        "FROM rps_tmp AS t "
        "WHERE r.rps_short = t.rps_short and (r.rps != t.rps or r.rps_cod != t.rps_cod)"
    ).rowcount
    # Add new row
    added = db.execute(
        "INSERT INTO rps SELECT * FROM rps_tmp WHERE rps_short NOT IN (SELECT rps_short FROM rps)"
    ).rowcount
    db.execute("DROP TABLE rps_tmp CASCADE;")
    db.commit()

    return {"message": f"Updated: {updated} rec, added: {added} rec."}


def get_type_operation_list(db: Session):
    return db.query(models.TypeOperation).all()


def get_type_operation_df(engine: Engine):
    type_operation_df = pd.read_sql("select name as type_operation, id from type_operation", con=engine)
    return type_operation_df


def create_season_coefficient(db: Session, season_coefficient: schemas.SeasonCoefficientCreate):
    db_result = models.SeasonalCoefficient(**season_coefficient.dict())
    db.add(db_result)
    db.commit()
    db.refresh(db_result)
    return db_result


def update_season_coefficient(db: Session, season_coefficient_id: int, season_coefficient):
    update_kwargs = schemas.SeasonCoefficientCreate(**season_coefficient.dict())
    db.query(models.SeasonalCoefficient).filter(models.SeasonalCoefficient.id == season_coefficient_id).update(
        update_kwargs.dict()
    )
    db.commit()
    return get_season_coefficient(db, season_coefficient_id)


def get_season_coefficient_list(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.SeasonalCoefficient).offset(skip).limit(limit).all()


def get_season_coefficient(db: Session, season_coefficient_id: int):
    result = db.query(models.SeasonalCoefficient).filter(models.SeasonalCoefficient.id == season_coefficient_id).first()
    if not result:
        raise HTTPException(status_code=404, detail=f"Seasonal coefficient with id={season_coefficient_id} not found")
    return result


def delete_season_coefficient(db: Session, season_coefficient_id: int):
    check_season_coefficient_can_delete(db, season_coefficient_id)
    db_result = get_season_coefficient(db, season_coefficient_id)
    db.delete(db_result)
    db.commit()
    return {"message": "OK"}


def check_season_coefficient_can_delete(db: Session, season_coefficient_id: int):
    result = db.execute(select(models.CalcTOU).where(models.CalcTOU.seasonal_coefficient_id == season_coefficient_id))
    if bool(result.first()):
        raise HTTPException(
            status_code=406,
            detail=f"Seasonal coefficient with id={season_coefficient_id} " f"cannot be deleted (used in calc_tou)",
        )


def check_season_coefficient_can_update(db: Session, season_coefficient_id: int):
    # result = db.query(models.CalcTOU).filter(models.CalcTOU.seasonal_coefficient_id == season_coefficient_id).filter(
    #     models.CalcTOU.status != CalcStateEnum.new)     #.first()
    result = db.execute(
        select(models.CalcTOU).where(
            and_(
                models.CalcTOU.seasonal_coefficient_id == season_coefficient_id,
                models.CalcTOU.status != CalcStateEnum.new,
            )
        )
    )
    if bool(result.first()):
        raise HTTPException(
            status_code=406,
            detail=f"Seasonal coefficient with id={season_coefficient_id} "
            f"cannot be updated (used in calc_tou with status!=NEW)",
        )


def check_calc_tou_can_start(db: Session, calc_tou_id: int):
    db_calc_tou = get_calc_tou(db, calc_tou_id)
    if db_calc_tou.status != CalcStateEnum.new:
        raise HTTPException(
            status_code=422,
            detail=f"The attempt to calculate the TOU was rejected "
            f"(status = {db_calc_tou.status.value}, but need {CalcStateEnum.new.value})",
        )
    result = db.execute(
        f"SELECT COUNT(distinct(date_rep)) FROM fact "
        f"where date_rep between '{db_calc_tou.date_from}' and '{db_calc_tou.date_to}'"
    ).first()[0]
    days = (db_calc_tou.date_to - db_calc_tou.date_from).days + 1
    if int(result) != days:
        raise HTTPException(
            status_code=422,
            detail=f"To calculate the TOU, you need a fact for {days} days, and there is only {result} "
            f"(period {db_calc_tou.date_from} - {db_calc_tou.date_to})",
        )


def fact_fully_loaded_slow_background(interval_sec: int):
    while True:
        print(f"Background task started! (every {interval_sec} sec)")
        fact_fully_loaded_slow(next(get_db()), 1990, 2100)
        print("Background task finished!")
        # block for the interval
        sleep(interval_sec)
        # perform the task


def fact_fully_loaded_slow(db: Session, year_from: int = 1990, year_to: int = 2100):
    result = db.execute(
        f"SELECT count(distinct(date_rep)), date_part('month', date_rep) as month_,  "
        f"date_part('year', date_rep) as year_ "
        f"FROM fact where date_rep between '{year_from}-01-01' and '{year_to}-12-31' group by year_, month_"
    ).fetchall()
    result = [
        (el["year_"], el["month_"])
        for el in result
        if el["count"] == monthrange(int(el["year_"]), int(el["month_"]))[1]
    ]
    db.execute("TRUNCATE calc_tou_loaded RESTART IDENTITY;")
    # db.query(models.CalcTouLoaded).delete(synchronize_session="fetch")
    db.commit()
    for el in result:
        record = models.CalcTouLoaded(**{"year": el[0], "month": el[1]})
        db.add(record)
    db.commit()
    return result


def fact_fully_loaded(db: Session, year_from: int = 2000, year_to: int = 2100):
    result = db.query(models.CalcTouLoaded).filter(models.CalcTouLoaded.year.between(year_from, year_to)).all()
    return [(el.year, el.month) for el in result]


def create_season_coefficient_body(db: Session, season_coefficient_body: schemas.SeasonCoefficientBodyCreate):
    check_season_coefficient_can_update(db, season_coefficient_body.head_id)
    try:
        db_result = models.SeasonalCoefficientBody(**season_coefficient_body.dict())
        db.add(db_result)
        db.commit()
        db.refresh(db_result)
    except Exception as err:
        raise HTTPException(status_code=409, detail=f"Error: {err}")
    return db_result


def create_season_coefficient_body_list(db: Session, season_coefficient_body_list: list):
    for season_coefficient_body in season_coefficient_body_list:
        create_season_coefficient_body(db, season_coefficient_body)
    return {"message": f"Added {len(season_coefficient_body_list)} row"}


def update_season_coefficient_body(db: Session, season_coefficient_body_id: int, season_coefficient_body):
    check_season_coefficient_can_update(db, season_coefficient_body.head_id)
    try:
        update_kwargs = schemas.SeasonCoefficientBodyCreate(**season_coefficient_body.dict())
        db.query(models.SeasonalCoefficientBody).filter(
            models.SeasonalCoefficientBody.id == season_coefficient_body_id
        ).update(update_kwargs.dict())
        db.commit()
    except Exception as err:
        raise HTTPException(status_code=409, detail=f"Error: {err}")
    return get_season_coefficient_body(db, season_coefficient_body_id)


def get_season_coefficient_body_list(db: Session, season_coefficient_id):
    return (
        db.query(models.SeasonalCoefficientBody)
        .filter(models.SeasonalCoefficientBody.head_id == season_coefficient_id)
        .all()
    )


def get_season_coefficient_body_df(engine: Engine, season_coefficient_id: int):
    season_coefficients_df = pd.read_sql(
        f"select * from seasonal_coefficient_body where head_id={season_coefficient_id}", con=engine
    )
    type_operation_df = pd.read_sql("select name as type_operation, id from type_operation", con=engine)
    season_coefficients_df = season_coefficients_df.merge(
        type_operation_df, how="inner", left_on="type_operation_id", right_on="id"
    )

    columns_for_rename = {f"Coefficient_{index:02}": f"СК{index:02}" for index in range(1, 13)}
    season_coefficients_df.rename(columns=columns_for_rename, inplace=True)
    cols = ["rps_short", "type_operation"] + [f"СК{index:02}" for index in range(1, 13)]
    return season_coefficients_df[cols]


def get_season_coefficient_body(db: Session, season_coefficient_body_id: int):
    result = (
        db.query(models.SeasonalCoefficientBody)
        .filter(models.SeasonalCoefficientBody.id == season_coefficient_body_id)
        .first()
    )
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"Seasonal coefficient with id={season_coefficient_body_id} not found"
        )
    return result


def delete_season_coefficient_body(db: Session, season_coefficient_body_id: int):
    db_result = get_season_coefficient_body(db, season_coefficient_body_id)
    check_season_coefficient_can_update(db, db_result.head_id)
    db.delete(db_result)
    db.commit()
    return {"message": "OK"}


def get_facts(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    date_min: Date = "1900-01-01",
    date_max: Date = "2099-01-01",
    load_from: str = "Cognos",
):
    return (
        db.query(models.Fact)
        .filter(models.Fact.load_from == load_from and models.Fact.date_rep.between(date_min, date_max))
        .offset(skip)
        .limit(limit)
        .all()
    )


def delete_facts(db: Session, date_min: Date, date_max: Date, load_from: str):
    amount_del_rec = (
        db.query(models.Fact)
        .filter(models.Fact.load_from == load_from, models.Fact.date_rep.between(date_min, date_max))
        .delete(synchronize_session="fetch")
    )
    # delete_from_table_cmd = f"delete from fact where date_rep >= '{date_min}' " \
    #                         f"and date_rep <= '{date_max}' and load_from = 'Cognos';"
    # db.execute(delete_from_table_cmd)
    db.commit()
    return {"message": f"removed {amount_del_rec} records"}


def get_mapping_client_cogmnos_sap(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.MappingClientCognosToSAP).offset(skip).limit(limit).all()


def delete_mapping_client_cogmnos_sap(db: Session, df: DataFrame):
    amount_del_rec = (
        db.query(models.MappingClientCognosToSAP)
        .filter(models.MappingClientCognosToSAP.client_cognos_id.in_(df.client_cognos_id))
        .delete(synchronize_session="fetch")
    )
    db.commit()
    amount_del_rec += (
        db.query(models.MappingClientCognosToSAP)
        .filter(models.MappingClientCognosToSAP.client_sap_id.in_(df.client_sap_id))
        .delete(synchronize_session="fetch")
    )
    db.commit()
    return {"message": f"removed {amount_del_rec} records"}


# @transact(session=db_local)
def create_calc_tou(
    db: Session,
    calc_tou: schemas.CalcTouCreate,
    rps_for_save: list,
    type_operation_for_save: list[int],
    station_for_save: list,
    username: str = "",
):
    try:
        get_season_coefficient(db, calc_tou.seasonal_coefficient_id)  # for simplest test fields
        db_calc_tou = models.CalcTOU(**calc_tou.dict())
        if username:
            db_calc_tou.user = username
        # db_calc_tou.status = CalcStateEnum.new
        # db_calc_tou.amount_operation = AmountOperationEnum.two
        db.add(db_calc_tou)
        db.commit()
    except Exception as err:
        raise HTTPException(status_code=409, detail=f"Error: {err}")
    calc_tou_update_rps(db, db_calc_tou.id, rps_for_save)
    calc_tou_update_type_operation(db, db_calc_tou.id, type_operation_for_save)
    calc_tou_update_station(db, db_calc_tou.id, station_for_save)
    # db.refresh(db_calc_tou)
    return get_calc_tou(db, db_calc_tou.id)


def update_calc_tou(
    db: Session,
    calc_tou_id: int,
    calc_tou: schemas.CalcTouCreate,
    rps_for_save: Optional[list] = None,
    type_operation_for_save: Optional[list[int]] = None,
    station_for_save: Optional[list] = None,
    username: str = "",
):
    update_kwargs = schemas.CalcTouCreate(**calc_tou.dict())
    if username:
        update_kwargs.user = username
    db.query(models.CalcTOU).filter(models.CalcTOU.id == calc_tou_id).update(update_kwargs.dict())
    db.commit()
    if not rps_for_save is None:
        calc_tou_update_rps(db, calc_tou_id, rps_for_save)
    if not type_operation_for_save is None:
        calc_tou_update_type_operation(db, calc_tou_id, type_operation_for_save)
    if not station_for_save is None:
        calc_tou_update_station(db, calc_tou_id, station_for_save)
    return get_calc_tou(db, calc_tou_id)


def copy_calc_tou(
    db: Session,
    calc_tou_id: int,
    new_name: Optional[str] = None,
    username: str = "",
):
    old_calc_tou = get_calc_tou(db, calc_tou_id)
    if not old_calc_tou:
        raise HTTPException(status_code=404, detail=f"CalcTou with id={calc_tou_id} not found")
    new_calc_tou = schemas.CalcTouCreate(**old_calc_tou.__dict__)
    new_calc_tou.name = new_name if new_name else new_calc_tou.name + " (copy)"
    new_calc_tou.status = CalcStateEnum.new
    if username:
        new_calc_tou.user = username
    new_calc_tou.parent_id = calc_tou_id
    return create_calc_tou(
        db,
        new_calc_tou,
        [el.rps_short for el in old_calc_tou.rps_list],
        [el.type_operation_id for el in old_calc_tou.type_operation_list],
        [el.st_code for el in old_calc_tou.station_list],
    )


def get_calc_tou_list(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.CalcTOU).offset(skip).limit(limit).all()


def get_calc_tou(db: Session, calc_tou_id: int):
    result = db.query(models.CalcTOU).filter(models.CalcTOU.id == calc_tou_id).first()
    if result is None:
        raise HTTPException(status_code=404, detail="CalcTOU not found")
    log = get_log(db=db, parent_id=calc_tou_id, parent_name="calc_tou")
    result.log = log.msg if log else ""
    result.seasonal_coefficient_name = result.seasonal_coefficient.name
    return result


def delete_calc_tou(db: Session, calc_tou_id: int):
    db_calc_tou = get_calc_tou(db, calc_tou_id)
    calc_tou_delete_rps(db, calc_tou_id)
    calc_tou_delete_station(db, calc_tou_id)
    calc_tou_delete_type_operation(db, calc_tou_id)
    db.delete(db_calc_tou)
    db.commit()
    return {"message": "OK"}


def calc_tou_delete_rps(db: Session, calc_tou_id: int):
    db.query(models.CalcTouLinkRps).filter(models.CalcTouLinkRps.calc_tou_id == calc_tou_id).delete(
        synchronize_session="fetch"
    )
    db.commit()


def calc_tou_delete_type_operation(db: Session, calc_tou_id: int):
    db.query(models.CalcTouLinkTypeOperation).filter(models.CalcTouLinkTypeOperation.calc_tou_id == calc_tou_id).delete(
        synchronize_session="fetch"
    )
    db.commit()


def calc_tou_delete_station(db: Session, calc_tou_id: int):
    db.query(models.CalcTouLinkStation).filter(models.CalcTouLinkStation.calc_tou_id == calc_tou_id).delete(
        synchronize_session="fetch"
    )
    db.commit()


def calc_tou_update_rps(db: Session, calc_tou_id: int, rps_list: list):
    calc_tou_delete_rps(db, calc_tou_id)
    result_list = [{"calc_tou_id": calc_tou_id, "rps_short": rps_short} for rps_short in rps_list]
    db.bulk_insert_mappings(models.CalcTouLinkRps, result_list)
    db.commit()


def calc_tou_update_type_operation(db: Session, calc_tou_id: int, type_operation_list: list):
    calc_tou_delete_type_operation(db, calc_tou_id)
    result_list = [
        {"calc_tou_id": calc_tou_id, "type_operation_id": type_operation_id}
        for type_operation_id in type_operation_list
    ]
    db.bulk_insert_mappings(models.CalcTouLinkTypeOperation, result_list)
    db.commit()


def calc_tou_update_station(db: Session, calc_tou_id: int, station_list: list):
    calc_tou_delete_station(db, calc_tou_id)
    result_list = [{"calc_tou_id": calc_tou_id, "st_code": st_code} for st_code in station_list]
    db.bulk_insert_mappings(models.CalcTouLinkStation, result_list)
    db.commit()


def get_branches(db_ora: Session, locator: str = "", limit: int = 10):
    return db_ora.execute(
        "SELECT DISTINCT ORG_ID as id, SHORTNAME AS name FROM ssp.ORG_FILIAL "
        f"WHERE UPPER(SHORTNAME) LIKE '%{locator.upper()}%' AND ROWNUM <= {limit} ORDER BY SHORTNAME"
    ).fetchall()


def get_branch_by_id(db_ora: Session, branch_id: int = None):
    if not id:
        return ""
    return db_ora.execute(f"SELECT DISTINCT SHORTNAME AS name FROM ssp.ORG_FILIAL WHERE ORG_ID = {branch_id}").first()


def get_stations(db_ora: Session, locator: str = "", limit: int = 10):
    return db_ora.execute(
        "SELECT s.ST_CODE, (s.ST_NAME || ' (' || f.SHORTNAME || ' - ' || vr.RW_SHORT_NAME || ')') AS ST_NAME "
        "FROM ssp.STATIONS s "
        "INNER JOIN nsi.V_RAILWAY_SYSDATE vr ON s.ROADID = vr.RW_CODE "
        "INNER JOIN ssp.ORG_FILIAL f ON s.BRANCH_ID = f.ORG_ID AND vr.RW_CODE = f.RW_CODE "
        f"WHERE UPPER(s.ST_NAME) LIKE '%{locator.upper()}%' AND ROWNUM <= {limit}"
    ).fetchall()


def get_stations_by_calc_tou_id(db: Session, db_ora: Session, calc_tou_id: int = 0, station_list: list[int] = []):
    if calc_tou_id:
        calc_tou_link_station = (
            db.query(models.CalcTouLinkStation).filter(models.CalcTouLinkStation.calc_tou_id == calc_tou_id).all()
        )
        station_list = [el.st_code for el in calc_tou_link_station]
    if len(station_list):
        station_list = ", ".join(map(lambda x: f"'{x}'", station_list))
        return db_ora.execute(
            "SELECT s.ST_CODE, (s.ST_NAME || ' (' || f.SHORTNAME || ' - ' || vr.RW_SHORT_NAME || ')') AS ST_NAME "
            "FROM ssp.STATIONS s "
            "INNER JOIN nsi.V_RAILWAY_SYSDATE vr ON s.ROADID = vr.RW_CODE "
            "INNER JOIN ssp.ORG_FILIAL f ON s.BRANCH_ID = f.ORG_ID AND vr.RW_CODE = f.RW_CODE "
            f"WHERE s.ST_CODE IN ({station_list})"
        ).fetchall()
    else:
        return []


def get_calc_tou_spr(db: Session, db_ora: Session):
    rps = get_rps_list(db)
    station = get_stations(db_ora)
    type_operation = get_type_operation_list(db)
    branch = get_branches(db_ora=db_ora, limit=100)
    return {
        "rps": {item.rps_short: (item.rps_cod, item.rps) for item in rps},
        "status": {item.value: CALC_STATUSES[item.value] for item in CalcStateEnum},
        "amount_operation": {item.value: AMOUNT_OPERATION[item.value] for item in AmountOperationEnum},
        "type_merge": {item.value: CALC_TYPE_MERGE[item.value] for item in CalcTypeMergeEnum},
        "group_data": ["РОСКГ", "РОС1С2КГ"],
        "branch": branch,
        "station": station,
        "type_operation": type_operation,
        "amount_year_period": list(range(1, 6)),
    }


def create_new_spr_if_empty(db: Session, engine: Engine, engine_ora: Engine):
    import_rps_from_ora(db, engine, engine_ora)
    result = db.query(models.TypeOperation).all()
    if not result:
        result_list = [{"name": name} for name in ["Погрузка", "Выгрузка"]]
        db.bulk_insert_mappings(models.TypeOperation, result_list)
        db.commit()
    result = db.query(models.SeasonalCoefficient).all()
    if not result:
        create_new_season_coefficient_automatic(db)


# noinspection PyUnusedLocal
def create_new_season_coefficient_automatic(db: Session):
    db_season_coefficient = models.SeasonalCoefficient(**{"name": "Базовый набор сезонных коэффициентов"})
    db.add(db_season_coefficient)
    db.commit()
    result = db.query(models.TypeOperation).all()
    id_load = [el.id for el in result if el.name == "Погрузка"][0]
    id_unload = [el.id for el in result if el.name == "Выгрузка"][0]
    # fmt: off
    tmp_head = ['head_id', 'rps_short', 'type_operation_id', 'Coefficient_01', 'Coefficient_02',
                'Coefficient_03', 'Coefficient_04', 'Coefficient_05', 'Coefficient_06', 'Coefficient_07',
                'Coefficient_08', 'Coefficient_09', 'Coefficient_10', 'Coefficient_11', 'Coefficient_12']
    tmp_value = [[db_season_coefficient.id, 'ПВ', id_load,
                  1.03, 1.02, 0.98, 0.98, 1.02, 1.00, 1.01, 0.99, 0.97, 0.94, 1.02, 1.04]]
    tmp_value += [[db_season_coefficient.id, 'КР', id_load,
                   1.20, 1.18, 0.99, 0.85, 0.95, 0.93, 0.92, 0.97, 0.97, 0.93, 1.07, 1.04]]
    tmp_value += [[db_season_coefficient.id, 'ПЛ', id_load,
                   1.29, 1.08, 0.91, 0.83, 0.97, 0.92, 0.94, 0.90, 0.90, 0.92, 1.15, 1.19]]
    tmp_value += [[db_season_coefficient.id, 'ЦМВ', id_load,
                   1.82, 1.27, 1.06, 0.81, 0.84, 0.76, 0.8, 0.78, 0.69, 0.83, 0.95, 1.38]]
    tmp_value += [[db_season_coefficient.id, 'ПВ', id_unload,
                   1.02, 1.02, 0.99, 0.94, 0.95, 0.97, 1.01, 1.05, 1.04, 1.03, 0.99, 0.99]]
    tmp_value += [[db_season_coefficient.id, 'КР', id_unload,
                   1.14, 1.00, 1.01, 0.94, 1.00, 0.97, 0.98, 0.98, 0.99, 0.95, 0.99, 1.05]]
    tmp_value += [[db_season_coefficient.id, 'ПЛ', id_unload,
                   1.12, 0.98, 0.98, 0.92, 0.95, 0.95, 1.10, 1.14, 1.08, 0.92, 0.92, 0.94]]
    tmp_value += [[db_season_coefficient.id, 'ЦМВ', id_unload,
                   1.26, 1.05, 0.98, 0.93, 0.95, 0.91, 0.92, 0.90, 0.93, 0.97, 1.07, 1.13]]
    # fmt: on
    result_list = [{head: value for head, value in zip(tmp_head, value)} for value in tmp_value]
    db.bulk_insert_mappings(models.SeasonalCoefficientBody, result_list)
    db.commit()
    return {
        "message": f"Created new set Seasonal coefficients ({db_season_coefficient.name} - {len(result_list)} rows)"
    }


# class UserRepository:
#     def init(self, session_factory: Callable[..., AbstractContextManager[Session]]) -> None:
#         self.session_factory = session_factory
#
#     def get_all(self) -> Iterator[User]:
#         with self.session_factory() as session:
#             return session.query(User).all()
#
#     def get_by_id(self, user_id: int) -> User:
#         with self.session_factory() as session:
#             user = session.query(User).filter(User.id == user_id).first()
#             if not user:
#                 raise UserNotFoundError(user_id)
#             return user
#
#     def add(self, email: str, password: str, is_active: bool = True) -> User:
#         with self.session_factory() as session:
#             user = User(email=email, hashed_password=password, is_active=is_active)
#             session.add(user)
#             session.commit()
#             session.refresh(user)
#             return user
#
#     def delete_by_id(self, user_id: int) -> None:
#         with self.session_factory() as session:
#             entity: User = session.query(User).filter(User.id == user_id).first()
#             if not entity:
#                 raise UserNotFoundError(user_id)
#             session.delete(entity)
#             session.commit()


async def create_calc_tou_external(
    db: Session, calc_tou_external: schemas.CalcTouExternalCreate, uploaded_file: UploadFile
):
    content = await uploaded_file.read()  # async read
    db_file_storage = models.FileStorage(
        file_name=uploaded_file.filename,
        file_body=content,
    )
    db.add(db_file_storage)
    db.commit()

    msg = get_info_from_excel(content)

    db_calc_tou_external = models.CalcTouExternal(**calc_tou_external.dict())
    db_calc_tou_external.file_storage_id = db_file_storage.id
    db.add(db_calc_tou_external)
    db.commit()
    write_log(db=db, parent_id=db_calc_tou_external.id, parent_name="calc_tou_external", msg=f"Created ({msg})")
    return get_calc_tou_external(db, db_calc_tou_external.id)


def get_calc_tou_external_list(db: Session, engine: Engine, skip: int = 0, limit: int = 100):
    result = db.query(models.CalcTouExternal).offset(skip).limit(limit).all()
    file_storage_df = get_file_storage_df(engine)
    for el in result:
        if el.file_storage_id:
            el.file_storage_name = file_storage_df.loc[file_storage_df["id"] == el.file_storage_id]["file_name"].values[
                0
            ]
    return result


def get_calc_tou_external(db: Session, calc_tou_external_id: int):
    result = db.query(models.CalcTouExternal).filter(models.CalcTouExternal.id == calc_tou_external_id).first()
    if result is None:
        raise HTTPException(status_code=404, detail="CalcTouExternal not found")
    if result.file_storage_id:
        result.file_storage_name = db.execute(
            f"select file_name from file_storage where id = {result.file_storage_id}"
        ).first()[0]
    log = get_log(db=db, parent_id=calc_tou_external_id, parent_name="calc_tou_external")
    result.log = log.msg if log else ""
    return result


def update_calc_tou_external(
    db: Session,
    calc_tou_external_id: int,
    calc_tou_external: schemas.CalcTouExternalCreate,
):
    update_kwargs = schemas.CalcTouExternalCreate(**calc_tou_external.dict())
    db.query(models.CalcTouExternal).filter(models.CalcTouExternal.id == calc_tou_external_id).update(
        update_kwargs.dict()
    )
    db.commit()
    return get_calc_tou_external(db, calc_tou_external_id)


async def update_calc_tou_external_file(db: Session, calc_tou_external_id: int, uploaded_file: UploadFile):
    calc_tou_external = get_calc_tou_external(db, calc_tou_external_id)
    content = await uploaded_file.read()  # async read
    if calc_tou_external.file_storage_id is None:
        raise HTTPException(status_code=404, detail="FileStorage with ID=NULL not found")
    try:
        db.query(models.FileStorage).filter(models.FileStorage.id == calc_tou_external.file_storage_id).update(
            {"file_name": uploaded_file.filename, "file_body": content}
        )
        db.commit()
        msg = get_info_from_excel(content)
        write_log(db=db, parent_id=calc_tou_external_id, parent_name="calc_tou_external", msg=f"Update file ({msg})")

    except Exception as err:
        raise HTTPException(status_code=409, detail=f"Error: {err}")

    return get_calc_tou_external(db, calc_tou_external_id)


def delete_calc_tou_external(db: Session, calc_tou_external_id: int):
    db_calc_tou_external = get_calc_tou_external(db, calc_tou_external_id)
    db.delete(db_calc_tou_external)
    db.commit()
    return {"message": "OK"}


def get_file_storage_df(engine: Engine):
    file_storage_df = pd.read_sql("select id, file_name from file_storage", con=engine)
    return file_storage_df


def get_log_list(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Log).offset(skip).limit(limit).all()


def get_log(db: Session, log_id: int = None, parent_id: int = None, parent_name: str = None):
    if not log_id and not (parent_id and parent_name):
        raise HTTPException(
            status_code=404,
            detail=f"For create Log (log_id={log_id}) need not empty "
            f"parent_id ({parent_id}) and parent_name ({parent_name})",
        )
    if log_id:
        result = db.query(models.Log).filter(models.Log.id == log_id).first()
        if result is None:
            raise HTTPException(status_code=404, detail=f"Log with ID={log_id} not found")
    else:
        result = (
            db.query(models.Log)
            .filter(models.Log.parent_id == parent_id)
            .filter(models.Log.parent_name == parent_name)
            .first()
        )
    return result


def write_log(
    db: Session,
    log_id: int = None,
    parent_id: int = None,
    parent_name: str = None,
    type: MyLogTypeEnum = MyLogTypeEnum.INFO,
    msg: str = "",
    is_append: bool = True,
    is_with_time: bool = True,
    username: str = "",
):
    if not username:
        username = PARSED_CONFIG.username
    if is_with_time and msg:
        msg = f'{str(datetime.datetime.today()).split(".", 2)[0]} ({username}) - {msg}'
    db_log = get_log(db, log_id, parent_id, parent_name)
    if not db_log:
        if not parent_id or not parent_name:
            raise HTTPException(
                status_code=404,
                detail=f"For create Log (log_id={log_id}) need not empty "
                f"parent_id ({parent_id}) and parent_name ({parent_name})",
            )
        db_log = models.Log(parent_id=parent_id, parent_name=parent_name, type=type, msg=msg)
        db.add(db_log)
        db.commit()
        return db_log

    log_update_dict = {}
    log_update_dict["type"] = type if type else db_log.type
    log_update_dict["msg"] = f"{db_log.msg}\n{msg}" if is_append else msg

    db.query(models.Log).filter(models.Log.id == db_log.id).update(log_update_dict)

    # db_log.update(log_update_dict)
    db.commit()
    return db_log

    if log_id:
        result = db.query(models.Log).filter(models.Log.id == log_id).first()
        if result is None:
            raise HTTPException(status_code=404, detail=f"Log with ID={log_id} not found")
    else:
        result = (
            db.query(models.Log)
            .filter(models.Log.parent_id == parent_id)
            .filter(models.Log.parent_name == parent_name)
            .first()
        )
    return result


async def import_season_coefficient(
    db: Session, engine: Engine, season_coefficient_name: str, uploaded_file: UploadFile
):
    try:
        content = await uploaded_file.read()  # async read
        report_df = pd.read_excel(content)

        type_operation_df = get_type_operation_df(engine)
        report_df = report_df.merge(type_operation_df, how="inner", left_on="Тип операции", right_on="type_operation")
        report_df.rename(columns=MAPPING_SEASONAL_COEFFICIENT, inplace=True)

        seasonal_coefficient = SeasonCoefficientCreate(name=season_coefficient_name)
        season_coefficient_db = create_season_coefficient(db, seasonal_coefficient)
        report_df["head_id"] = season_coefficient_db.id

        cols = ["head_id", "rps_short", "type_operation_id"] + [f"Coefficient_{index:02}" for index in range(1, 13)]
        season_coefficient_body_list = [SeasonCoefficientBodyCreate(**el) for el in report_df[cols].to_dict("records")]
        create_season_coefficient_body_list(db, season_coefficient_body_list)
        result = get_season_coefficient(db, season_coefficient_db.id)
    except:
        raise HTTPException(status_code=422, detail=f"Incorrect file format")

    return result


async def export_season_coefficient(engine: Engine, season_coefficient_id: int):
    report_df = get_season_coefficient_body_df(engine, season_coefficient_id)
    report_df.rename(columns=MAPPING_SEASONAL_COEFFICIENT_REVERSE, inplace=True)
    stream = table_writer(dataframes={"Sheet1": report_df}, param="xlsx")
    return stream  # .read()
