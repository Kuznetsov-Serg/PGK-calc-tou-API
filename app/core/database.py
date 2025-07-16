import logging
from contextlib import contextmanager
from functools import wraps
from typing import Any, Generator

import psycopg2
from fastapi import HTTPException
from sqlalchemy import create_engine, orm
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import Session, sessionmaker

from app.settings import PARSED_CONFIG

logger = logging.getLogger()

#
# try:
#     # Устанавливаем соединение с postgres
#     connection = psycopg2.connect(
#         user=PARSED_CONFIG.database.DB_USER, password=PARSED_CONFIG.database.DB_PASS,
#         host=PARSED_CONFIG.database.DB_HOST, port=PARSED_CONFIG.database.DB_PORT
#     )
#     connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#     # Создаем курсор для выполнения операций с базой данных
#     cursor = connection.cursor()
#     # Создаем базу данных
#     cursor.execute("create database " + PARSED_CONFIG.database.DB_NAME)
#     # Закрываем соединение
#     cursor.close()
#     connection.close()
# except Exception as db_exc:
#     logger.error("Exception creating database: " + str(db_exc))

ConnectionLocal = psycopg2.connect(
    user=PARSED_CONFIG.database.DB_USER,
    password=PARSED_CONFIG.database.DB_PASS,
    host=PARSED_CONFIG.database.DB_HOST,
    port=PARSED_CONFIG.database.DB_PORT,
    database=PARSED_CONFIG.database.DB_NAME,
)
EnginePostresql = create_engine(PARSED_CONFIG.database.dsn, pool_pre_ping=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=EnginePostresql)

EngineOracle = create_engine(PARSED_CONFIG.database_ora.dsn, pool_pre_ping=True)
SessionLocalOra = sessionmaker(autocommit=False, autoflush=False, bind=EngineOracle)


# Base = declarative_base()
@as_declarative()
class Base:
    @declared_attr
    def __tablename__(cls) -> str:
        # pylint: disable=no-self-argument
        return cls.__name__.lower()


class Database:
    def init(self, db_url: str) -> None:
        self._engine = create_engine(db_url, echo=True)
        self._session_factory = orm.scoped_session(
            orm.sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine,
            ),
        )

    def create_database(self) -> None:
        Base.metadata.create_all(self._engine)

    @contextmanager
    def session(self) -> Generator[Session, Any, Any]:
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            logger.exception("Session rollback because of exception")
            session.rollback()
            raise
        finally:
            # session.expunge_all()
            session.close()


def exception_arg_parser(e: str):
    return e.split()[10].replace(")", "").replace("(", "").split("=")


def transact(session: Session):
    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):

            try:
                f = func(*args, **kwargs)

            except HTTPException as e:
                session.rollback()
                session.close()
                raise HTTPException(status_code=e.status_code, detail=e.detail, headers=e.headers)

            except IntegrityError as e:
                session.rollback()
                session.close()
                exc_details = exception_arg_parser(e.args[0])
                raise HTTPException(
                    status_code=400,
                    detail={"Duplicate values not allowed": {"key": exc_details[0], "value": exc_details[1]}},
                ) from e

            except Exception as e:
                session.rollback()
                session.close()
                raise HTTPException(status_code=400, detail={"Invalid request": e.args}) from e

            return f

        return inner

    return wrapper
