import datetime
from typing import Optional

from pydantic import BaseModel

from app.settings import AmountOperationEnum, CalcStateEnum, CalcTypeMergeEnum, MyLogTypeEnum


class OurBaseModel(BaseModel):
    class Config:
        orm_mode = True


class RpsBase(OurBaseModel):
    rps_cod: int
    rps_short: str
    rps: str

    class Config:
        orm_mode = True


class RpsCreate(RpsBase):
    pass


class Rps(RpsBase):
    # rps_cod: int
    pass


class TypeOperationBase(OurBaseModel):
    name: str


class TypeOperationCreate(TypeOperationBase):
    pass


class TypeOperation(TypeOperationBase):
    id: int


class SeasonCoefficientBase(OurBaseModel):
    name: str
    coefficient_list: Optional[list] = []


class SeasonCoefficientName(OurBaseModel):
    name: str


class SeasonCoefficientCreate(OurBaseModel):
    name: str = "Наименование набора сезонных коэффициентов"


class SeasonCoefficient(SeasonCoefficientBase):
    id: int


class SeasonCoefficientBodyBase(OurBaseModel):
    head_id: int
    rps_short: str
    type_operation_id: int
    Coefficient_01: float
    Coefficient_02: float
    Coefficient_03: float
    Coefficient_04: float
    Coefficient_05: float
    Coefficient_06: float
    Coefficient_07: float
    Coefficient_08: float
    Coefficient_09: float
    Coefficient_10: float
    Coefficient_11: float
    Coefficient_12: float


class SeasonCoefficientBodyView(SeasonCoefficientBodyBase):
    type_operation: Optional[TypeOperation] = None


# class SeasonCoefficientBodyUpdate(SeasonCoefficientBodyBase):
#     head_id: Union[int, None] = Query(default=None, include_in_schema=False)
#     rps_short: Optional[str] = None
#     type_operation_id: Optional[int] = None


class SeasonCoefficientBodyCreate(SeasonCoefficientBodyBase):
    rps_short: str = "МВЗ"


class SeasonCoefficientBody(SeasonCoefficientBodyView):
    id: int


class CalcTouLinkRps(OurBaseModel):
    id: int
    calc_tou_id: int
    rps_short: str


class CalcTouLinkStation(OurBaseModel):
    id: int
    calc_tou_id: int
    st_code: str
    st_name: Optional[str] = ""


class CalcTouLinkTypeOperation(OurBaseModel):
    id: int
    calc_tou_id: int
    type_operation_id: int
    # type_operation: Optional[str] = ""
    type_operation: Optional[TypeOperation] = None


class CalcTouBase(OurBaseModel):
    date: datetime.date
    user: Optional[str] = None
    name: str
    status: CalcStateEnum
    base_year: int
    date_from: datetime.date
    date_to: datetime.date
    branch_id: Optional[int] = None
    date_merge_start: Optional[datetime.date] = None
    type_merged: Optional[CalcTypeMergeEnum] = CalcTypeMergeEnum.not_merged
    amount_operation: AmountOperationEnum
    exclude_from: Optional[float] = None
    exclude_to: Optional[float] = None
    exclude_volumes_traffic_less: Optional[float] = None
    amount_year_period: int
    seasonal_coefficient_id: int
    group_data: str
    parent_id: Optional[int] = None
    # parent_id: Union[int, None] = None


class CalcTouCreate(CalcTouBase):
    date = datetime.date.today()
    date_from = datetime.date(datetime.datetime.today().year, 1, 1)
    date_to = datetime.date(datetime.datetime.today().year, datetime.datetime.today().month, 1) - datetime.timedelta(
        days=1
    )
    base_year: int = datetime.datetime.today().year
    # date_merge_start: None   #datetime.date(datetime.datetime.today().year, 1, 1)
    user: str = "admin"
    name: str = "Название расчёта"
    # status: CalcStateEnum = "NEW"
    amount_operation: AmountOperationEnum = "loading/unloading"
    exclude_from: Optional[float] = 30
    exclude_to: Optional[float] = 0.4
    exclude_volumes_traffic_less: Optional[float] = 0.001
    amount_year_period: int = 5
    group_data: str = "РОСКГ"


class CalcTou(CalcTouCreate):
    id: int
    file_storage_id: Optional[int] = None
    # seasonal_coefficient: SeasonCoefficientName
    seasonal_coefficient_name: Optional[str] = None
    rps_list: list[CalcTouLinkRps]
    station_list: list[CalcTouLinkStation]
    rps_list: list
    type_operation_list: list[CalcTouLinkTypeOperation]
    log: Optional[str] = ""


class CalcTouExternalBase(OurBaseModel):
    date: datetime.date
    user: Optional[str] = None
    name: str
    note: Optional[str] = None
    date_from: datetime.date
    date_to: datetime.date


class CalcTouExternalCreate(CalcTouExternalBase):
    date = datetime.date.today()
    user: str = "admin"
    name: str = "Название внешнего расчёта"
    note: str = ""
    date_from = datetime.date(datetime.datetime.today().year, 1, 1)
    date_to = datetime.date(datetime.datetime.today().year, datetime.datetime.today().month, 1) - datetime.timedelta(
        days=1
    )


class CalcTouExternal(CalcTouExternalCreate):
    id: int
    file_storage_id: int
    file_storage_name: Optional[str] = None
    log: Optional[str] = ""


class StationBase(OurBaseModel):
    cod: str
    name: str


class StationCreate(StationBase):
    pass


class FileStorage(OurBaseModel):
    id: int
    file_name: str
    # file_body: bytes
    # file_len: len(file_body)


class Log(OurBaseModel):
    id: int
    parent_id: int
    parent_name: str
    type: MyLogTypeEnum
    msg: str
