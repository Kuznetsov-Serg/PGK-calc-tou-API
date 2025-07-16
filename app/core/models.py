import datetime

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import BYTEA, ENUM
from sqlalchemy.orm import relationship

from app.core.database import Base
from app.settings import AmountOperationEnum, CalcStateEnum, CalcTypeMergeEnum, MyLogTypeEnum


class MappingClientCognosToSAP(Base):
    __tablename__ = "mapping_client_cognos_sap"
    __table_args__ = {
        "comment": "Маппинг Клиенты Cognos to SAP",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    client_cognos_id = Column(String(10), unique=True, index=True, comment="ИД Клиента в Cognos")
    client_sap_id = Column(String(10), unique=True, index=True, comment="ИД Клиента в SAP")
    client = Column(String(160), comment="Наименование")

    UniqueConstraint("client_cognos_id", "client_sap_id")


# class FactOld(Base):
#     __tablename__ = "fact_old"
#     __table_args__ = {
#         "comment": "Факт ТОУ из Cognos и for SAP",
#     }
#
#     id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
#     date_rep = Column(Date, comment="Отчётная дата")
#     date_accept_next = Column(Date, comment="Дата приема след.")
#     period = Column(String(7), comment="Период")
#     load_from = Column(String(10), comment="Загружено из SUP/Cognos")
#     st_code = Column(String(16), comment="Станция выполнения ГО код")
#     st_name = Column(String(80), comment="Станция выполнения ГО")
#     rw_code = Column(String(12), index=True, comment="Дорога выполнения ГО код")
#     rw_short_name = Column(String(80), comment="Дорога выполнения ГО Сокр")
#     rw_name = Column(String(80), comment="Дорога выполнения ГО Полн")
#     org_id = Column(Numeric, index=True, comment="Филиал ГО ID")
#     org_shortname = Column(String(80), comment="Филиал ГО Сокр")
#     org_name = Column(String(160), comment="Филиал ГО Полн")
#     shipper_cod = Column(String(12), comment="Грузоотправитель на станции выполнения ГО код")
#     shipper = Column(String(80), comment="Грузоотправитель на станции выполнения ГО")
#     consignee_cod = Column(String(12), comment="Грузополучатель на станции выполнения ГО код")
#     consignee = Column(String(80), comment="Грузополучатель на станции выполнения ГО код")
#     client_sap_id = Column(String(10), comment="Клиент ID SAP")
#     client = Column(String(160), comment="Клиент Наименование")
#     type_op_vps = Column(Date, comment="Операция тип ВПС")
#     type_op = Column(String(10), comment="Тип операции")
#     wagon_num = Column(BigInteger, index=True, comment="№ вагона")
#     rps_cod = Column(BigInteger, index=True, comment="РПС код")
#     rps_short = Column(String(80), comment="РПС Наименование Сокр")
#     rps = Column(String(80), comment="РПС Наименование Полн")
#     invoice_num_current = Column(String(20), comment="№ накладной тек.")
#     cargo_group_num = Column(Float, comment="Группа груза, номер тек.")
#     cargo_group_short = Column(String(160), comment="Группа груза Наименование Сокр тек.")
#     cargo_current_short = Column(String(80), comment="Груз Наименование Сокр тек.")
#     cargo_current = Column(String(160), comment="Груз Наименование Полн тек.")
#     cargo_etsng_cod_current = Column(String(16), comment="Груз ЕТСНГ код тек.")
#     date_arrival_current = Column(Date, comment="Дата прибытия тек.")
#     invoice_num_next = Column(String(20), index=True, comment="№ накладной след.")
#     cargo_group_num_next = Column(Float, comment="Группа груза, номер след.")
#     cargo_group_next_short = Column(String(80), comment="Группа груза Наименование Сокр след.")
#     cargo_next_short = Column(String(80), comment="Наименование груза след.")
#     cargo_next = Column(String(160), comment="Груз Наименование Полн след.")
#     cargo_etsng_cod_next = Column(Float, comment="Груз ЕТСНГ код след.")
#     date_accept_next = Column(Date, comment="Дата приема след.")
#     double_operation = Column(String(6), comment="Сдвоенная операция")
#     parking_fact = Column(Numeric, comment="Простои Факт, ваг-сут")
#     parking_fact_vps = Column(Date, comment="Простои Факт ВПС, ваг-сут")
#     use_in_vps = Column(Date, comment="Признак: Используем в ВПС")
#     parking_for_double = Column(Numeric, comment="Простои Сдвоенные, ваг-сут")
#     cargo_group_go_num = Column(Numeric, comment="Группа груза ГО, номер")
#     cargo_group_go_short = Column(String(80), comment="Группа груза ГО Наименование Сокр")
#     cargo_go_short = Column(String(80), comment="Груз ГО Наименование Сокр")
#     cargo_go = Column(String(160), comment="Груз ГО Наименование Полн")
#     cargo_etsng_go = Column(String(16), comment="Груз ЕТСНГ ГО код")


class Fact(Base):
    __tablename__ = "fact"
    __table_args__ = {
        "comment": "Факт ТОУ из Cognos и for SAP",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    date_rep = Column(Date, comment="Отчётная дата")
    load_from = Column(String(10), comment="Загружено из SAP/Cognos")
    st_code = Column(String(16), comment="Станция выполнения ГО код")
    st_code_from = Column(String(16), comment="Станция отправления код")
    st_code_to = Column(String(16), comment="Станция назначения код")
    org_id = Column(Numeric, index=True, comment="Филиал ГО ID")
    client_sap_id = Column(String(10), comment="Клиент ID SAP")
    type_op = Column(String(10), comment="Тип операции")
    wagon_num = Column(BigInteger, index=True, comment="№ вагона")
    rps_short = Column(String(10), comment="РПС Наименование Сокр")
    cargo_group_num = Column(Integer, comment="Группа груза, номер тек.")
    parking_fact = Column(Numeric, comment="Простои Факт, ваг-сут")


class Rps(Base):
    __tablename__ = "rps"
    __table_args__ = {
        "comment": "Справочник типов вагонов",
    }

    rps_cod = Column(BigInteger, comment="РПС код")
    rps_short = Column(String(10), primary_key=True, index=True, comment="РПС Наименование Сокр")
    rps = Column(String(80), comment="РПС Наименование Полн")

    calc_tou_list = relationship("CalcTouLinkRps", backref="rps")


class TypeOperation(Base):
    __tablename__ = "type_operation"
    __table_args__ = {
        "comment": "Справочник типов операций",
    }

    id = Column(Integer, primary_key=True, autoincrement=True, comment="ID")
    name = Column(String(20), index=True, unique=True, comment="Тип операции")


class CalcTOU(Base):
    __tablename__ = "calc_tou"
    __table_args__ = {
        "comment": "Перечень расчетов ТОУ",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    date = Column(Date, comment="Дата расчета")
    user = Column(String(20), comment="Автор (пользователь создавший расчет)")
    name = Column(String(80), comment="Наименование")
    status = Column(ENUM(CalcStateEnum), default="NEW", comment="Статус")
    base_year = Column(Integer, comment="Базовый период (год)")
    date_from = Column(Date, comment="Начальная дата")
    date_to = Column(Date, comment="Конечная дата")
    branch_id = Column(BigInteger, nullable=True, comment="фильтр по Филиалу ГО (ИД)")
    date_merge_start = Column(Date, nullable=True, comment="Дата начала слияния")
    type_merged = Column(ENUM(CalcTypeMergeEnum), default="NOT_MERGED", comment="тип алгоритма объединения")
    amount_operation = Column(ENUM(AmountOperationEnum), default="loading/unloading", comment="Количествo операций")
    exclude_from = Column(Numeric(precision=8, scale=2), default=30, comment="Исключены из расчета выбросы от")
    exclude_to = Column(Numeric(precision=8, scale=2), default=0.4, comment="Исключены из расчета выбросы до")
    exclude_volumes_traffic_less = Column(
        Numeric(precision=8, scale=4),
        default=0.001,
        comment="Исключены незначительные объемы перевозок филиала менее %",
    )
    amount_year_period = Column(SmallInteger, default=5, comment="Количество годовых периодов расчета")
    seasonal_coefficient_id = Column(
        ForeignKey("seasonal_coefficient.id", ondelete="RESTRICT"),
        nullable=False,
        comment="ID набора сезонных коэффициентов",
    )
    group_data = Column(String(10), default="РОСКГ", comment="Группировка данных")
    parent_id = Column(BigInteger, nullable=True, comment="ID родительского расчёта")
    file_storage_id = Column(BigInteger, nullable=True, comment="ID результата расчета в хранилище")

    seasonal_coefficient = relationship("SeasonalCoefficient", backref="calc_tou")
    rps_list = relationship("CalcTouLinkRps", cascade="all,delete", backref="calc_tou")
    station_list = relationship("CalcTouLinkStation", cascade="all,delete", backref="calc_tou")
    type_operation_list = relationship("CalcTouLinkTypeOperation", cascade="all,delete", backref="calc_tou")


class CalcTouLoaded(Base):
    __tablename__ = "calc_tou_loaded"
    __table_args__ = {
        "comment": "Перечень месяцев по годам для которых Fact полностью загружен (все дни месяца)",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    year = Column(SmallInteger, comment="Год")
    month = Column(SmallInteger, comment="Месяц")


class CalcTouLinkRps(Base):
    __tablename__ = "calc_tou_link_rps"
    __table_args__ = {
        "comment": "Список типов вагонов в расчетах ТОУ",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    calc_tou_id = Column(ForeignKey("calc_tou.id"), comment="ID calc_tou")
    rps_short = Column(ForeignKey("rps.rps_short"), comment="РПС Наименование Сокр")


class CalcTouLinkTypeOperation(Base):
    __tablename__ = "calc_tou_link_type_operation"
    __table_args__ = {
        "comment": "Список типов операций в расчетах ТОУ",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    calc_tou_id = Column(ForeignKey("calc_tou.id"), comment="ID calc_tou")
    type_operation_id = Column(ForeignKey("type_operation.id"), comment="ИД Типа операции")

    type_operation = relationship("TypeOperation", viewonly=True, lazy="joined")


class CalcTouLinkStation(Base):
    __tablename__ = "calc_tou_link_station"
    __table_args__ = {
        "comment": "Список станций в расчетах ТОУ",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    calc_tou_id = Column(ForeignKey("calc_tou.id"), comment="ID calc_tou")
    st_code = Column(String(16), comment="Станция выполнения ГО код")


class SeasonalCoefficient(Base):
    __tablename__ = "seasonal_coefficient"
    __table_args__ = {
        "comment": "Таблица сезонных коэффициентов (заголовки)",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    name = Column(String(80), comment="Название набора СК")

    coefficient_list = relationship("SeasonalCoefficientBody", cascade="all,delete", backref="seasonal_coefficient")


class SeasonalCoefficientBody(Base):
    __tablename__ = "seasonal_coefficient_body"
    __table_args__ = (
        UniqueConstraint("head_id", "rps_short", "type_operation_id", name="_customer_location_uc"),
        {"comment": "Таблица сезонных коэффициентов (строки)"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    head_id = Column(ForeignKey("seasonal_coefficient.id"), comment="ID заголовка набора")
    rps_short = Column(ForeignKey("rps.rps_short"), comment="РПС Наименование Сокр")
    type_operation_id = Column(ForeignKey("type_operation.id"), comment="ИД Типа операции")
    Coefficient_01 = Column(Numeric, comment="Сезонный коэффициент на январь")
    Coefficient_02 = Column(Numeric, comment="Сезонный коэффициент на февраль")
    Coefficient_03 = Column(Numeric, comment="Сезонный коэффициент на март")
    Coefficient_04 = Column(Numeric, comment="Сезонный коэффициент на апрель")
    Coefficient_05 = Column(Numeric, comment="Сезонный коэффициент на май")
    Coefficient_06 = Column(Numeric, comment="Сезонный коэффициент на июнь")
    Coefficient_07 = Column(Numeric, comment="Сезонный коэффициент на июль")
    Coefficient_08 = Column(Numeric, comment="Сезонный коэффициент на август")
    Coefficient_09 = Column(Numeric, comment="Сезонный коэффициент на сентябрь")
    Coefficient_10 = Column(Numeric, comment="Сезонный коэффициент на октябрь")
    Coefficient_11 = Column(Numeric, comment="Сезонный коэффициент на ноябрь")
    Coefficient_12 = Column(Numeric, comment="Сезонный коэффициент на декабрь")

    type_operation = relationship("TypeOperation", viewonly=True, lazy="joined")


class CalcTouExternal(Base):
    __tablename__ = "calc_tou_external"
    __table_args__ = {
        "comment": "Перечень расчетов ТОУ загруженных из сторонних систем",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    date = Column(Date, comment="Дата расчета")
    name = Column(String(80), comment="Наименование")
    date_from = Column(Date, comment="Начальная дата")
    date_to = Column(Date, comment="Конечная дата")
    note = Column(String(240), comment="Наименование")
    user = Column(String(20), comment="Автор (пользователь создавший расчет)")
    file_storage_id = Column(BigInteger, nullable=True, comment="ID результата расчета в хранилище")


class FileStorage(Base):
    __tablename__ = "file_storage"
    __table_args__ = {
        "comment": "Таблица хранения результатов расчетов",
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    file_name = Column(String(80), comment="Наименование файла")
    file_body = Column(BYTEA(), comment="Тело файла")


class Log(Base):
    __tablename__ = "log"
    __table_args__ = (
        UniqueConstraint("parent_id", "parent_name"),
        {"comment": "Таблица истории обработки"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    date = Column(Date, default=datetime.date.today, comment="Дата создания")
    parent_id = Column(BigInteger, comment="ID родительского объекта")
    parent_name = Column(String(20), comment="Наименование типа объекта/таблицы")
    type = Column(ENUM(MyLogTypeEnum), default="start", comment="Статус")
    msg = Column(Text, comment="Сообщение")

    @classmethod
    def write(cls, msg_type, msg, *args):
        cls.objects.create(type=msg_type, msg=msg + " " + " ".join([str(s) for s in args]))
