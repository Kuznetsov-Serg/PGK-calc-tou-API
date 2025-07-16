import datetime
import logging
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.util import asyncio
from starlette.responses import StreamingResponse
from tqdm import tqdm

from app.api.deps import get_db, get_db_ora, get_engine, get_engine_ora
from app.auth.crud import check_token, write_user_history
from app.core import crud, models, schemas
from app.core.crud import delete_mapping_client_cogmnos_sap, import_rps_from_ora
from app.core.models import MappingClientCognosToSAP
from app.settings import EXCEL_MEDIA_TYPE, PARSED_CONFIG, MyLogTypeEnum
from app.utils.calc_tou import calc_tou
from app.utils.load_cognos_sap import load_cognos_file, load_fact_from_pickle, load_sap_file
from app.utils.utils import save_df_to_model_via_csv, transliteration

# to include app api use next line
# from app.service_name.api.v1 import router as service_name_router

router = APIRouter(prefix=PARSED_CONFIG.API_PREFIX, tags=["Main"], dependencies=[Depends(check_token)])
router_test = APIRouter(prefix=PARSED_CONFIG.API_PREFIX, tags=["For test"])
# router = APIRouter(prefix="/api")
# router.include_router(auth_router)

logger = logging.getLogger()


@router_test.get("/health_check/", name="A simple test that the application responds (live).")
def health_check() -> Any:
    return {"message": "OK"}


@router_test.get("/postgresql-check/", name="Test that the app has connected to PostgreSQL.")
def postgresql_check(db: Session = Depends(get_db), engine: Engine = Depends(get_engine)) -> Any:
    # df = pd.read_sql("select * from public.fact where date_rep between '2022-05-01' and '2022-05-31'", con=engine)
    # df.to_excel("fact_2022_05.xlsx", index=False)
    db.execute("SELECT 1 x")
    return {"message": "OK"}


@router_test.get("/ora-check/", name="Test that the app has connected to Oracle (Komandor).")
def oracle_check1(db: Session = Depends(get_db_ora)) -> Any:
    db.execute("SELECT 1 FROM dual")
    return {"message": "OK"}


@router_test.get(
    "/ora-get-row/", name="Test that the app has connected to Oracle (Komandor) and can get rows from Table."
)
async def oracle_check2(db: Session = Depends(get_db_ora)) -> Any:
    return db.execute(
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
        ) WHERE rn = 1 and ROWNUM <= 10
        """
    ).fetchall()


@router.post("/create-new-spr-if-empty/", name="Create new spr (type operation, seasonal coefficients) if they empty.")
def create_new_spr_if_empty(
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
):
    write_user_history(db=db, message='Launched "create_new_spr_if_empty"')
    # logger.info(f"User {PARSED_CONFIG.username} launched create_new_spr_if_empty")
    return crud.create_new_spr_if_empty(db, engine, engine_ora)


@router.post(
    "/create-new-season-coefficient-automatic/",
    name="Create a set of seasonal coefficients, even if there are already previously created ones.)",
)
def create_new_season_coefficient_automatic(db: Session = Depends(get_db)):
    write_user_history(db=db, message='Called "create_new_season_coefficient_automatic"')
    return crud.create_new_season_coefficient_automatic(db)


@router.post("/load-mapping-client-cognos-sap/")
async def load_mapping_client_cognos_sap(
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    uploaded_file: UploadFile = File(...),
):
    # temp = NamedTemporaryFile(delete=False)     # temp.name - full file_name
    username = PARSED_CONFIG.username
    time_start = datetime.datetime.now()
    print(f'Start function in {time_start.strftime("%H:%M:%S")}')

    content = await uploaded_file.read()  # async read
    try:
        mapping_df = (
            pd.read_excel(
                content,  # async read
                skiprows=1,
                dtype={
                    "ID ASU text": "string",
                    "ID SAP text": "string",
                    "ID ASU code": "string",
                },
                usecols=["ID ASU text", "ID SAP text", "Сокр Клиент"],
            ).rename(
                columns={"ID ASU text": "client_cognos_id", "ID SAP text": "client_sap_id", "Сокр Клиент": "client"}
            )
            # .dropna(subset=["client_cognos_id"])
        )
        mapping_df["client_cognos_id"] = mapping_df["client_cognos_id"].fillna("*" + mapping_df["client_sap_id"])
    except Exception as e:
        try:
            mapping_df = pd.read_excel(
                content,
                sheet_name="Маппинг Cognos to SAP",
                usecols=["Cognos ID", "SAP ID", "SAP Наименование Сокр", "Мрк"],
                dtype={
                    "Cognos ID": "string",
                    "SAP ID": "string",
                },
            ).rename(
                columns={"Cognos ID": "client_cognos_id", "SAP ID": "client_sap_id", "SAP Наименование Сокр": "client"}
            )
            mapping_df = (
                mapping_df.loc[mapping_df["Мрк"].notnull() & mapping_df["client_cognos_id"].notnull()].drop(
                    columns=["Мрк"]
                )
                # .astype(dtype={"cognos_client_id": "int"})
            )
        except Exception as err:
            raise HTTPException(status_code=409, detail=f"Error: {err}")

    print(mapping_df.shape)
    print(mapping_df.head())

    # remove duplicates
    mapping_df = (
        mapping_df[mapping_df.columns]
        .groupby(["client_cognos_id"])
        .first()
        .reset_index()
        .groupby(["client_sap_id"])
        .first()
        .reset_index()
    )

    delete_mapping_client_cogmnos_sap(db, mapping_df)
    save_df_to_model_via_csv(
        engine=engine, df=mapping_df, cols=mapping_df.columns, model_class=MappingClientCognosToSAP
    )
    time_finish = datetime.datetime.now()
    print(
        f'Finished function in {time_finish.strftime("%H:%M:%S")} '
        f'(execution period {str(time_finish - time_start).split(".", 2)[0]})'
    )
    write_user_history(
        db=db,
        username=username,
        message=f'Called "load_mapping_client_cognos_sap" from file="{uploaded_file.filename}"',
    )
    return {"Result": "OK"}


@router.post("/load-cognos-and-sap-from-pickle/", include_in_schema=False)
async def load_cognos_and_sap_from_pickle(
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
) -> Any:
    username = PARSED_CONFIG.username
    result = load_fact_from_pickle(db, engine)
    write_user_history(db=db, username=username, message=f'Called "load_cognos_and_sap_from_pickle" ({result})')
    return result


@router.post("/load-cognos-excel/", include_in_schema=False, name="Load fact from cognos (excel-file)")
async def load_cognos(
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
    uploaded_file: UploadFile = File(...),
    is_overwrite=True,
) -> Any:
    username = PARSED_CONFIG.username
    result = await load_cognos_file(db, engine, engine_ora, uploaded_file, is_overwrite)
    write_user_history(
        db=db, username=username, message=f'Called "load-cognos-excel" from file="{uploaded_file.filename}" ({result})'
    )
    return result


@router.post("/load-cognos-excel-list/", name="Load fact from cognos (excel-files)")
async def load_cognos_list(
    files_list: list[UploadFile],
    is_overwrite=True,
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
) -> Any:
    loop = asyncio.get_running_loop()
    username = PARSED_CONFIG.username
    result_all = []
    for report in tqdm(files_list):
        uploaded_file = report.file
        # result = loop.run_until_complete(load_cognos_file(db, engine, engine_ora, uploaded_file, is_overwrite))
        # result = await load_cognos_file(db, engine, engine_ora, uploaded_file, is_overwrite)
        result = await asyncio.gather(
            # loop.run_until_complete(load_cognos_file(db, engine, engine_ora, uploaded_file, is_overwrite))
            loop.run_in_executor(None, load_cognos_file, db, engine, engine_ora, uploaded_file, is_overwrite),
        )
        result_all.append(result)
        crud.fact_fully_loaded_slow(db)  # for actualisation CalcTouLoaded
        write_user_history(
            db=db,
            username=username,
            message=f'Called "load-cognos-excel-list" from file="{report.filename}" ({result})',
        )
    return result_all


@router.post("/load-sap-excel/", include_in_schema=False, name="Load Fact from SAP (Excel-file).")
async def load_sap(
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
    uploaded_file: UploadFile = File(...),
    is_overwrite=True,
    token=Depends(check_token),
) -> Any:
    result = await load_sap_file(db, engine, engine_ora, uploaded_file, is_overwrite)
    write_user_history(
        db=db, username=token["sub"], message=f'Called "load-sap-excel" from file="{uploaded_file.filename}" ({result})'
    )
    return result


@router.post("/load-sap-excel-list/", name="Load fact from SAP (excel-files)...")
async def load_sap_list(
    files_list: list[UploadFile],
    is_overwrite=True,
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
) -> Any:
    pass

    loop = asyncio.get_running_loop()
    username = PARSED_CONFIG.username
    result_all = []
    for report in tqdm(files_list):
        uploaded_file = report.file
        result = await asyncio.gather(
            loop.run_in_executor(None, load_sap_file, db, engine, engine_ora, uploaded_file, is_overwrite),
        )
        result_all.append(result)
        crud.fact_fully_loaded_slow(db)  # for actualisation CalcTouLoaded
        write_user_history(
            db=db, username=username, message=f'Called "load-sap-excel-list" from file="{report.filename}" ({result})'
        )
    return result_all


@router.get(
    "/fact-fully-loaded/",
    name="Get a list of months with years in which the Fact has already been fully loaded (all days of the month)",
)
def fact_fully_loaded(db: Session = Depends(get_db), year_from: int = 1990, year_to: int = 2100):
    return crud.fact_fully_loaded(db, year_from, year_to)


@router.get(
    "/fact-fully-loaded-slow/",
    name="Get a list of months with years in which the Fact has already been fully loaded (all days of the month)",
)
def fact_fully_loaded_slow(db: Session = Depends(get_db), year_from: int = 1990, year_to: int = 2100):
    return crud.fact_fully_loaded_slow(db, year_from, year_to)


@router.post("/import-rps-from-ora/", name="Import the RPS from Oracle (komandor)")
def import_rps(
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
) -> Any:
    username = PARSED_CONFIG.username
    msg = import_rps_from_ora(db, engine, engine_ora)
    write_user_history(db=db, username=username, message=f'Called "import-rps-from-ora" ({msg})')
    return {"Message": f"{msg}"}


@router.post("/rps/", response_model=schemas.RpsBase)
def create_rps(rps: schemas.RpsCreate, db: Session = Depends(get_db)):
    db_rps = crud.get_rps(db, rps_short=rps.rps_short)
    if db_rps:
        raise HTTPException(status_code=400, detail="RPS already registered")
    logger.info(f'User {PARSED_CONFIG.username} launched "create_rps" (rps_short={rps.rps_short})')
    return crud.create_rps(db=db, rps=rps)


@router.get("/rps-list/", response_model=list[schemas.Rps])
def read_rps_list(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    rps = crud.get_rps_list(db, skip=skip, limit=limit)
    return rps


@router.get("/rps/{rps_short}", name="Read single RPS by short name", response_model=schemas.RpsBase)
def read_rps(rps_short: str, db: Session = Depends(get_db)):
    db_rps = crud.get_rps(db, rps_short=rps_short)
    if db_rps is None:
        raise HTTPException(status_code=404, detail="RPS not found")
    return db_rps


@router.delete("/rps/{rps_short}")
def delete_rps(rps_short: str, db: Session = Depends(get_db)):
    db_rps = crud.get_rps(db, rps_short=rps_short)
    if db_rps is None:
        raise HTTPException(status_code=404, detail="RPS not found")
    logger.info(f'User {PARSED_CONFIG.username} launched "delete_rps" (rps_short={rps_short})')
    return crud.delete_rps(db=db, rps_short=rps_short)


@router.get("/type-operation/", response_model=list[schemas.TypeOperation])
def read_type_operation_list(db: Session = Depends(get_db)):
    return crud.get_type_operation_list(db)


@router.post("/season-coefficient/", response_model=schemas.SeasonCoefficient)
def create_season_coefficient(season_coefficient: schemas.SeasonCoefficientCreate, db: Session = Depends(get_db)):
    logger.info(
        f'User {PARSED_CONFIG.username} launched "create_season_coefficient" (season_coefficient={season_coefficient})'
    )
    return crud.create_season_coefficient(db, season_coefficient)


@router.post(
    "/import-season-coefficient/",
    name="Import set seasonal coefficients (excel-file).",
    response_model=schemas.SeasonCoefficient,
)
async def import_season_coefficient(
    season_coefficient_name: str,
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    uploaded_file: UploadFile = File(...),
) -> Any:
    result = await crud.import_season_coefficient(db, engine, season_coefficient_name, uploaded_file)
    logger.info(f'User {PARSED_CONFIG.username} launched "import_season_coefficient" (result={result})')
    return result


@router.get("/export-season-coefficient/{season_coefficient_id}", name="Export set seasonal coefficients (excel-file).")
async def export_season_coefficient(season_coefficient_id: int, engine: Engine = Depends(get_engine)):
    file_season_coefficient = await crud.export_season_coefficient(engine, season_coefficient_id)
    response = StreamingResponse(iter([file_season_coefficient.getvalue()]), media_type=EXCEL_MEDIA_TYPE)
    file_name = f"season-coefficient.xlsx"
    response.headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
    response.headers["Access-Control-Expose-Headers"] = "Content-Disposition"
    return response


@router.patch("/season-coefficient/{season_coefficient_id}", response_model=schemas.SeasonCoefficientBase)
def update_season_coefficient(
    season_coefficient_id: int,
    season_coefficient: schemas.SeasonCoefficientCreate,
    db: Session = Depends(get_db),
):
    logger.info(
        f'User {PARSED_CONFIG.username} launched "update_season_coefficient" '
        f"(season_coefficient_id={season_coefficient_id})"
    )
    return crud.update_season_coefficient(db, season_coefficient_id, season_coefficient)


@router.get("/season-coefficient/", response_model=list[schemas.SeasonCoefficient])
def read_season_coefficient_list(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    result = crud.get_season_coefficient_list(db, skip=skip, limit=limit)
    return result


@router.get("/season-coefficient/{season_coefficient_id}", response_model=schemas.SeasonCoefficient)
def read_season_coefficient(season_coefficient_id: int, db: Session = Depends(get_db)):
    return crud.get_season_coefficient(db, season_coefficient_id)


@router.delete("/season-coefficient/{season_coefficient_id}")
def delete_season_coefficient(season_coefficient_id: int, db: Session = Depends(get_db)):
    logger.info(
        f'User {PARSED_CONFIG.username} launched "delete_season_coefficient" '
        f"(season_coefficient_id={season_coefficient_id})"
    )
    return crud.delete_season_coefficient(db, season_coefficient_id)


@router.post("/season-coefficient-body/", response_model=schemas.SeasonCoefficientBody)
def create_season_coefficient_body(
    season_coefficient_body: schemas.SeasonCoefficientBodyCreate,
    db: Session = Depends(get_db),
):
    db_result = crud.get_season_coefficient(db, season_coefficient_body.head_id)
    if db_result is None:
        raise HTTPException(status_code=404, detail="Season coefficient not created - can't create Body!!!")
    return crud.create_season_coefficient_body(db, season_coefficient_body)


@router.post("/season-coefficient-body-list/")
def create_season_coefficient_body_list(
    season_coefficient_body_list: list[schemas.SeasonCoefficientBodyCreate],
    db: Session = Depends(get_db),
):
    for season_coefficient_body in season_coefficient_body_list:
        db_result = crud.get_season_coefficient(db, season_coefficient_body.head_id)
        if db_result is None:
            raise HTTPException(
                status_code=404,
                detail=f"Season coefficient for head_id="
                f"{season_coefficient_body.head_id} not created - can't create Body!!!",
            )
    return crud.create_season_coefficient_body_list(db, season_coefficient_body_list)


@router.patch("/season-coefficient-body/{season_coefficient_body_id}", response_model=schemas.SeasonCoefficientBody)
def update_season_coefficient_body(
    season_coefficient_body_id: int,
    season_coefficient_body: schemas.SeasonCoefficientBodyBase,
    db: Session = Depends(get_db),
):
    return crud.update_season_coefficient_body(db, season_coefficient_body_id, season_coefficient_body)


@router.get(
    "/season-coefficient-body-list/{season_coefficient_id}", response_model=list[schemas.SeasonCoefficientBodyView]
)
def read_season_coefficient_body_list(season_coefficient_id: int, db: Session = Depends(get_db)):
    result = crud.get_season_coefficient_body_list(db, season_coefficient_id)
    return result


@router.get("/season-coefficient-body/{season_coefficient_body_id}", response_model=schemas.SeasonCoefficientBodyView)
def read_season_coefficient_body(season_coefficient_body_id: int, db: Session = Depends(get_db)):
    db_result = crud.get_season_coefficient_body(db, season_coefficient_body_id)
    if db_result is None:
        raise HTTPException(status_code=404, detail="Season coefficient body not found")
    return db_result


@router.delete("/season-coefficient-body/{season_coefficient_body_id}")
def delete_season_coefficient_body(season_coefficient_body_id: int, db: Session = Depends(get_db)):
    db_result = crud.get_season_coefficient_body(db, season_coefficient_body_id)
    if db_result is None:
        raise HTTPException(status_code=404, detail="Season coefficient body not found")
    return crud.delete_season_coefficient_body(db, season_coefficient_body_id)


@router.put("/calc-tou/", response_model=schemas.CalcTou)
def create_calc_tou(
    calc_tou: schemas.CalcTouCreate,
    db: Session = Depends(get_db),
    db_ora: Session = Depends(get_db_ora),
    rps_for_save: list[str] = [],
    type_operation_for_save: list[int] = [],
    station_for_save: list[str] = [],
):
    username = PARSED_CONFIG.username
    if not crud.get_season_coefficient(db, calc_tou.seasonal_coefficient_id):
        raise HTTPException(status_code=404, detail="Season coefficient not found")
    result = crud.get_stations_by_calc_tou_id(db=db, db_ora=db_ora, station_list=station_for_save)
    if len(result) != len(station_for_save):
        raise HTTPException(status_code=404, detail="Trouble in station_for_save")
    result = crud.create_calc_tou(db, calc_tou, rps_for_save, type_operation_for_save, station_for_save, username)
    logger.info(f'User {PARSED_CONFIG.username} launched "create_calc_tou" (result={result})')
    return result


@router.patch("/calc-tou/{calc_tou_id}", response_model=schemas.CalcTou)
def update_calc_tou(
    calc_tou_id: int,
    calc_tou: schemas.CalcTouCreate,
    db: Session = Depends(get_db),
    db_ora: Session = Depends(get_db_ora),
    rps_for_save: Optional[list] = None,
    type_operation_for_save: Optional[list[int]] = None,
    station_for_save: Optional[list] = None,
):
    username = PARSED_CONFIG.username
    if not crud.get_season_coefficient(db, calc_tou.seasonal_coefficient_id):
        raise HTTPException(status_code=404, detail="Season coefficient not found")
    if station_for_save:
        result = crud.get_stations_by_calc_tou_id(db=db, db_ora=db_ora, station_list=station_for_save)
        if len(result) != len(station_for_save):
            raise HTTPException(status_code=404, detail="Trouble in station_for_save")
    return crud.update_calc_tou(
        db, calc_tou_id, calc_tou, rps_for_save, type_operation_for_save, station_for_save, username
    )


@router.post("/calc-tou-copy/{calc_tou_id}", response_model=schemas.CalcTou)
def copy_calc_tou(
    calc_tou_id: int,
    new_name: Optional[str] = None,
    db: Session = Depends(get_db),
):
    username = PARSED_CONFIG.username
    return crud.copy_calc_tou(db, calc_tou_id, new_name, username)


@router.get("/calc-tou/", response_model=list[schemas.CalcTou])
def read_calc_tou_list(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    calc_tou_list = crud.get_calc_tou_list(db, skip=skip, limit=limit)
    for el in calc_tou_list:
        el.seasonal_coefficient_name = el.seasonal_coefficient.name
    return calc_tou_list


@router.get("/calc-tou/{calc_tou_id}", response_model=schemas.CalcTou)
def read_calc_tou(calc_tou_id: int, db: Session = Depends(get_db), db_ora: Session = Depends(get_db_ora)):
    result = crud.get_calc_tou(db, calc_tou_id=calc_tou_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"CalcTOU with ID={calc_tou_id} not found")
    # result.seasonal_coefficient_name = result.seasonal_coefficient.name
    station_list_ext = crud.get_stations_by_calc_tou_id(db, db_ora, calc_tou_id)
    if station_list_ext:
        for el in result.station_list:
            el.st_name = ([el2.st_name for el2 in station_list_ext if el2.st_code == el.st_code] + [""])[0]
    # log = crud.get_log(db=db, parent_id=calc_tou_id, parent_name="calc_tou")
    # result.log = log.msg if log else ""
    return result


@router.delete("/calc-tou/{calc_tou_id}")
def delete_calc_tou(calc_tou_id: int, db: Session = Depends(get_db)):
    logger.info(f'User {PARSED_CONFIG.username} launched "delete_calc_tou" (calc_tou_id={calc_tou_id})')
    return crud.delete_calc_tou(db=db, calc_tou_id=calc_tou_id)


@router.get("/get-branches/", name="Get list of branches with filter (part of name) from Oracle (komandor)")
def get_branches(db_ora: Session = Depends(get_db_ora), locator: str = "", limit: int = 10):
    return crud.get_branches(db_ora, locator, limit)


@router.get("/get-stations/", name="Get list of stations with filter (part of name) from Oracle (komandor)")
def get_stations(db_ora: Session = Depends(get_db_ora), locator: str = "", limit: int = 10):
    return crud.get_stations(db_ora, locator, limit)


@router.get("/get-stations-by-calc-tou-id/")
def get_stations_by_calc_tou_id(
    db: Session = Depends(get_db), db_ora: Session = Depends(get_db_ora), calc_tou_id: int = 0
):
    return crud.get_stations_by_calc_tou_id(db, db_ora, calc_tou_id)


@router.get(
    "/calc-tou-spr/",
    name="Returns directories (rpc, status, amount_operation, group_data, branches, station, "
    "type_operation, amount_year_period) for useful filling in the calc_tou form",
)
def get_calc_tou_spr(db: Session = Depends(get_db), db_ora: Session = Depends(get_db_ora)):
    return crud.get_calc_tou_spr(db, db_ora)


# async def download_xlsx(file_type: str = "xlsx"):
#     if file_type in ("xlsx", "csv"):
#         d = {"col1": [1, 2], "col2": [3, 4]}
#         df = pd.DataFrame(data=d)
#         stream = table_writer(dataframes={"sheet 001": df}, param=file_type)
#         response = StreamingResponse(iter([stream.getvalue()]), media_type=EXCEL_MEDIA_TYPE)
#         response.headers["Content-Disposition"] = f"attachment; filename=bids_report.{file_type}"
#         return response
#     else:
#         raise HTTPException(status_code=400, detail="file type not allowed")


@router.put("/calc-tou-start/{calc_tou_id}", name="Starting the calculation of calc_tou (on background).")
def calc_tou_start(
    calc_tou_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    engine: Engine = Depends(get_engine),
    engine_ora: Engine = Depends(get_engine_ora),
):
    username = PARSED_CONFIG.username
    crud.check_calc_tou_can_start(db, calc_tou_id)
    background_tasks.add_task(calc_tou, db, engine, engine_ora, calc_tou_id, username)
    message = "The calculation of the TOU has started in the background"
    write_user_history(db=db, username=username, message=f'Called "calc-tou-start" ({message})')
    return {"message": message}


@router.post("/calc-tou-external/", response_model=schemas.CalcTouExternal)
async def calc_tou_external_create(
    calc_tou_external: schemas.CalcTouExternalCreate = Depends(),
    uploaded_file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    username = PARSED_CONFIG.username
    result = await crud.create_calc_tou_external(db, calc_tou_external, uploaded_file)
    write_user_history(
        db=db, username=username, message=f'Called "calc_tou_external_create" from file="{uploaded_file.filename}"'
    )
    return result


@router.get("/calc-tou-external/", response_model=list[schemas.CalcTouExternal])
def read_calc_tou_external_list(
    skip: int = 0, limit: int = 100, db: Session = Depends(get_db), engine: Engine = Depends(get_engine)
):
    calc_tou_external_list = crud.get_calc_tou_external_list(db, engine, skip=skip, limit=limit)
    return calc_tou_external_list


@router.get("/calc-tou-external/{calc_tou_external_id}", response_model=schemas.CalcTouExternal)
def read_calc_tou_external(calc_tou_external_id: int, db: Session = Depends(get_db)):
    result = crud.get_calc_tou_external(db, calc_tou_external_id=calc_tou_external_id)
    return result


@router.patch("/calc-tou-external/{calc_tou_external_id}", response_model=schemas.CalcTouExternal)
def update_calc_tou_external(
    calc_tou_external_id: int,
    calc_tou_external: schemas.CalcTouExternalCreate,
    db: Session = Depends(get_db),
):
    return crud.update_calc_tou_external(db, calc_tou_external_id, calc_tou_external)


@router.patch("/calc-tou-external/update-file/{calc_tou_external_id}", response_model=schemas.CalcTouExternal)
async def calc_tou_external_update_file(
    calc_tou_external_id: int,
    uploaded_file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    username = PARSED_CONFIG.username
    result = await crud.update_calc_tou_external_file(db, calc_tou_external_id, uploaded_file)
    write_user_history(
        db=db, username=username, message=f'Called "calc_tou_external_create" from file="{uploaded_file.filename}"'
    )
    return result


@router.delete("/calc-tou-external/{calc_tou_external_id}")
def delete_calc_tou_external(calc_tou_external_id: int, db: Session = Depends(get_db)):
    return crud.delete_calc_tou_external(db, calc_tou_external_id)


@router.post("/load-file-in-db/", response_model=schemas.FileStorage)
async def load_file_in_db(
    db: Session = Depends(get_db), engine: Engine = Depends(get_engine), uploaded_file: UploadFile = File(...)
):
    # temp = NamedTemporaryFile(delete=False)     # temp.name - full file_name
    content = await uploaded_file.read()  # async read
    db_file_storage = models.FileStorage(
        file_name=uploaded_file.filename,
        file_body=content,
    )
    db.add(db_file_storage)
    db.commit()
    return db_file_storage
    # return {"Result": "OK"}


@router.get("/download-file-from-db/{file_storage_id}")
async def download_file_from_db(file_storage_id: int, db: Session = Depends(get_db)):
    file_storage = db.query(models.FileStorage).filter(models.FileStorage.id == file_storage_id).first()
    response = StreamingResponse(iter([file_storage.file_body]), media_type=EXCEL_MEDIA_TYPE)
    file_name = transliteration(file_storage.file_name)
    response.headers["Content-Disposition"] = f'attachment; filename="{file_name}"'
    response.headers["Access-Control-Expose-Headers"] = "Content-Disposition"
    return response


@router.get("/log-list/", response_model=list[schemas.Log])
def read_log_list(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    log_list = crud.get_log_list(db, skip=skip, limit=limit)
    return log_list


@router.get("/log/", response_model=schemas.Log)
def read_log(
    log_id: Optional[int] = Query(None, title="ID log", description="ID log (it is used if not empty)"),
    parent_id: Optional[int] = Query(None, title="ID parent of log", description="it is used if log_id is empty"),
    parent_name: Optional[str] = Query(
        None, title="name parent of log", description='it is used if log_id is empty ("calc_tou", for example)'
    ),
    db: Session = Depends(get_db),
):
    return crud.get_log(db, log_id, parent_id, parent_name)


@router.put("/log/", response_model=schemas.Log)
def write_log(
    log_id: Optional[int] = Query(None, title="ID log", description="ID log (it is used if not empty)"),
    parent_id: Optional[int] = Query(None, title="ID parent of log", description="it is used if log_id is empty"),
    parent_name: Optional[str] = Query(
        None, title="name parent of log", description='it is used if log_id is empty ("calc_tou", for example)'
    ),
    type: MyLogTypeEnum = MyLogTypeEnum.INFO,
    msg: Optional[str] = Query("", title="message"),
    is_append: Optional[bool] = Query(True, description="True - add a message to the previous, False - overwrite"),
    is_with_time: Optional[bool] = Query(True, description="True - add datetime to begin msg"),
    db: Session = Depends(get_db),
):
    return crud.write_log(db, log_id, parent_id, parent_name, type, msg, is_append, is_with_time)


# @router.get("/get_xlsx/")
# async def download_xlsx(file_type: str = "xlsx"):
#     if file_type in ("xlsx", "csv"):
#         d = {"col1": [1, 2], "col2": [3, 4]}
#         df = pd.DataFrame(data=d)
#         stream = table_writer(dataframes={"sheet 001": df}, param=file_type)
#         response = StreamingResponse(iter([stream.getvalue()]), media_type=EXCEL_MEDIA_TYPE)
#         response.headers["Content-Disposition"] = f"attachment; filename=bids_report.{file_type}"
#         return response
#     else:
#         raise HTTPException(status_code=400, detail="file type not allowed")
