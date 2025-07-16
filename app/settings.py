import enum
import os
from functools import lru_cache
from logging.config import dictConfig
from pathlib import Path
from typing import Any, Optional

from pydantic import AnyHttpUrl, BaseModel, PositiveInt, StrictStr

from app.utils.utils import merge, read_yaml

PROJ_ROOT = Path(__file__).parent.parent
config_env_var = "TOU_CONFIG_PATH"
DEFAULT_PATH = PROJ_ROOT / "config" / "tou_local.yaml"
# DEFAULT_PATH = PROJ_ROOT / "config" / "tou_default.yaml"
TOU_CONFIG_PATH = PROJ_ROOT / "config" / "docker.yaml"
STATIC_PATH = PROJ_ROOT / "static"
ENV_PATH = Path(os.environ.get(config_env_var) or "")
TEMPLATES_PATH = PROJ_ROOT / "templates"
# TEMPLATES_PATH_VIEWS = TEMPLATES_PATH / 'views'

# MAP_DIR = PROJ_ROOT / "assembled_map"
# MAP_NAME = 'map.json'
# MAP_PATH = MAP_DIR / MAP_NAME
LoggingConfig = dict[str, Any]


class EmptyCustomConfig(Exception):
    def __init__(self, path: Path):
        self.path = path

    def __str__(self) -> str:
        return f"Config file {self.path} is empty"


# class SMTPClient(ConnectionConfig):
#     MAIL_USERNAME: StrictStr
#     MAIL_PASSWORD: StrictStr
#     MAIL_PORT: PositiveInt = 465
#     MAIL_SERVER: str
#     MAIL_TLS: bool = False
#     MAIL_SSL: bool = True
#     MAIL_DEBUG: conint(gt=-1, lt=2) = 0  # type: ignore
#     MAIL_FROM: EmailStr
#     MAIL_FROM_NAME: Optional[str] = None
#     SUPPRESS_SEND: conint(gt=-1, lt=2) = 0  # type: ignore
#     USE_CREDENTIALS: bool = True
#     VALIDATE_CERTS: bool = True
#


class ServerConfiguration(BaseModel):
    host: StrictStr
    port: PositiveInt
    workers: int
    root_url: Optional[StrictStr] = None


class DependencyConfiguration(BaseModel):
    dsn: StrictStr


class DatabaseConfiguration(DependencyConfiguration):
    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASS: str
    DB_NAME: str


class CeleryConfiguration(DependencyConfiguration):
    tasks: StrictStr


class JWTConfiguration(BaseModel):
    jwt_secret: str
    jwt_algorithm: str = "RS256"
    jwt_access_token_days: int = 2


class Configuration(BaseModel):
    PROJECT_NAME: StrictStr
    PROJECT_VERSION: str
    PROJECT_ENVIRONMENT: str
    BACKEND_CORS_ORIGINS: list[AnyHttpUrl] = []
    API_PREFIX: str = ""

    AUTHORISE_BY_TOKEN: bool = False
    AUTHORISE_BY_WHITE_LIST: bool = False

    GZIP_MINIMUM_SIZE: int = 500
    SENTRY: bool = False
    # SENTRY_DSN: str
    # REDIS: bool = False
    # REDIS_HOST: str

    # server: ServerConfiguration
    logging: LoggingConfig
    database: DatabaseConfiguration
    database_ora: DatabaseConfiguration
    # smtp_client: SMTPClient
    # celery: CeleryConfiguration
    # flower: DependencyConfiguration
    jwt: JWTConfiguration
    ldap_server: StrictStr = "10.144.52.13"
    username: str = None


@lru_cache
def load_configuration(path: Path = "") -> Configuration:
    arg_path = Path(path)
    default_config = read_yaml(DEFAULT_PATH)

    custom_config_path = (arg_path.is_file() and arg_path) or (ENV_PATH.is_file() and ENV_PATH)
    if custom_config_path:
        custom_config = read_yaml(custom_config_path)

        if not custom_config:
            raise EmptyCustomConfig(path=custom_config_path)
        config_data = merge(default_config, custom_config)
    else:
        config_data = default_config

    return Configuration(**config_data)


def setup_logging(logging_config: LoggingConfig):
    dictConfig(logging_config)


def dump_config(config: Configuration) -> str:
    return config.json(indent=2, sort_keys=True)


PARSED_CONFIG = load_configuration()

# COOKIE_EXPIRATION_TIME = datetime.datetime.now() + datetime.timedelta(days=1000)
# COOKIE_EXPIRATION_DATE = COOKIE_EXPIRATION_TIME.strftime("%a, %d %b %Y %H:%M:%S GMT")
# CURRENT_YEAR_INT = datetime.datetime.today().year
EXCEL_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class CalcStateEnum(enum.Enum):
    new = "NEW"
    in_process = "IN_PROCESS"
    done = "DONE"
    deleted = "DELETED"


class CalcTypeMergeEnum(enum.Enum):
    not_merged = "NOT_MERGED"
    combination = "COMBINATION"
    replace = "REPLACE"
    substitution1 = "SUBSTITUTION_1"
    substitution2 = "SUBSTITUTION_2"


class AmountOperationEnum(enum.Enum):
    two = "loading/unloading"
    three = "loading/unloading/shifting"


class MyLogTypeEnum(enum.Enum):
    START = "start"
    FINISH = "finish"
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


CALC_STATUSES = {
    CalcStateEnum.new.value: "Новый расчет",
    CalcStateEnum.in_process.value: "В процессе",
    CalcStateEnum.done.value: "Расчет выполнен",
    CalcStateEnum.deleted.value: "Удалён",
}


CALC_TYPE_MERGE = {
    CalcTypeMergeEnum.not_merged.value: "Без слияния.",
    CalcTypeMergeEnum.combination.value: "Комбинирование расчетов с добавлением новых направлений из «источника» в «назначение»",
    CalcTypeMergeEnum.replace.value: "Автозамена строк расчета новым расчетом при выполнении ранее поставленной цели.",
    CalcTypeMergeEnum.substitution1.value: "Подстановка новых расчетных данных в «назначение» из «источника» начиная с заданного периода",
    CalcTypeMergeEnum.substitution2.value: "Подстановка новых расчетных данных в «назначение» из «источника» начиная от заданного начального периода до конечного периода.",
}


AMOUNT_OPERATION = {
    AmountOperationEnum.two.value: "расчет по погрузке/выгрузке",
    AmountOperationEnum.three.value: "расчет по погрузке/выгрузке/сдвойке",
}

LOG_TYPES = {
    MyLogTypeEnum.START: "Стартовал",
    MyLogTypeEnum.FINISH: "Завершено",
    MyLogTypeEnum.DEBUG: "debug",
    MyLogTypeEnum.INFO: "info",
    MyLogTypeEnum.WARNING: "warning",
    MyLogTypeEnum.ERROR: "error",
}
