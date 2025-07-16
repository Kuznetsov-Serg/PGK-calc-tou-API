import datetime

from sqlalchemy import VARCHAR, BigInteger, Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.core.database import Base


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True, comment="ID")
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)

    user_history = relationship("UserHistory", cascade="all,delete", backref="user")


class UserHistory(Base):
    __tablename__ = "user_history"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, comment="ID")
    date_time = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, comment="Дата/время")
    user_id = Column(Integer, ForeignKey("user.id"), nullable=False, comment="ID пользователя")
    description = Column(String, index=True)

    # user = relationship("User", cascade="all,delete", back_populates="user_history")


class WhiteList(Base):
    __tablename__ = "white_list"
    __table_args__ = {"comment": "Логины в нижнем регистре которые допущены к системе"}
    # id = Column("id", UUID, server_default=text("uuid_generate_v4()"), primary_key=True)
    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="ID")
    login = Column("login", VARCHAR(1024))
