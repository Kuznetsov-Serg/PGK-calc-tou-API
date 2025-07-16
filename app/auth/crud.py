import logging
from datetime import datetime, timedelta

import ldap
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from app.auth import models, schemas
from app.auth.router import oauth2_scheme
from app.settings import PARSED_CONFIG

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logger = logging.getLogger()


def ldap_auth(username, password):
    conn = ldap.initialize(f"ldap://{PARSED_CONFIG.ldap_server}")
    conn.protocol_version = 3
    conn.set_option(ldap.OPT_REFERRALS, 0)
    try:
        conn.simple_bind_s(f"{username}@pgk.rzd", password)
    except ldap.INVALID_CREDENTIALS:
        return {"status": "error", "message": "Неверный логин или пароль"}
    except ldap.SERVER_DOWN:
        return {"status": "error", "message": "Сервер авторизации недоступен"}
    except ldap.LDAPError as e:
        if type(e.message) == dict and e.message.has_key("desc"):
            return {"status": "error", "message": "Other LDAP error: " + e.message["desc"]}
        else:
            return {"status": "error", "message": "Other LDAP error: " + e}
    finally:
        conn.unbind_s()
    return {"status": "ok", "message": "Успешно"}


def create_token(data: dict = None, *, expires_delta: timedelta = None):
    iat = datetime.utcnow()
    if expires_delta:
        expire = iat + expires_delta
    else:
        expire = iat + timedelta(days=PARSED_CONFIG.jwt.jwt_access_token_days)

    to_encode = {"iat": iat, "exp": expire}

    if data is not None:
        to_encode |= data

    encoded_jwt = jwt.encode(to_encode, PARSED_CONFIG.jwt.jwt_secret, algorithm=PARSED_CONFIG.jwt.jwt_algorithm)
    return encoded_jwt


def check_token(token: str = Depends(oauth2_scheme), exc_status=status.HTTP_401_UNAUTHORIZED):
    credentials_exception = HTTPException(
        status_code=exc_status,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # print(f"check_token token={token}")
        token_decoded = jwt.decode(token, PARSED_CONFIG.jwt.jwt_secret, algorithms=[PARSED_CONFIG.jwt.jwt_algorithm])
        PARSED_CONFIG.username = token_decoded.get("sub", "NoAuthorised")
        return token_decoded
    except jwt.InvalidTokenError as exc:
        raise credentials_exception


def ldap_check(user_auth_model: OAuth2PasswordRequestForm):
    if PARSED_CONFIG.AUTHORISE_BY_TOKEN:
        pgk_cred = ldap_auth(user_auth_model.username, user_auth_model.password)
        if pgk_cred["status"] == "error":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=pgk_cred["message"],
                headers={"WWW-Authenticate": "Bearer"},
            )


async def check_on_white_list(db: Session, user_auth_model: OAuth2PasswordRequestForm):
    if PARSED_CONFIG.AUTHORISE_BY_WHITE_LIST:
        if not await check_login_on_whitelist(db, user_auth_model.username.lower()):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Недопустимый логин. Права этому пользователю еще не были выданы.",
            )


async def check_login_on_whitelist(db: Session, login: str) -> bool:
    result = db.execute(select(models.WhiteList).where(models.WhiteList.login == login))
    # result = await db.execute(select(models.WhiteList).where(models.WhiteList.login == login))
    return bool(result.first())


async def get_current_user(db: Session, token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, PARSED_CONFIG.jwt.jwt_secret, algorithms=[PARSED_CONFIG.jwt.jwt_algorithm])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = schemas.TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_or_create_user(db, token_data.username)
    if user is None:
        raise credentials_exception
    return user


def get_user(db: Session, user_id: int):
    return db.query(models.User).filter(models.User.id == user_id).first()


def get_or_create_user(db: Session, username, password: str = "NULL"):
    result = db.query(models.User).filter(models.User.username == username.lower()).first()
    return result if result else create_user(db, username, password)


def create_user(db: Session, username: str, password: str):
    try:
        db_result = models.User(
            username=username.lower(), email=username + "@pgk.ru", hashed_password=encrypt_password(password)
        )
        db.add(db_result)
        db.commit()
    except Exception as err:
        raise HTTPException(status_code=409, detail=f"Error: {err}")
    return db_result


def write_user_history(db: Session, username: str = None, password: str = "NULL", message: str = ""):
    try:
        if not username:
            username = PARSED_CONFIG.username
        db_user = get_or_create_user(db, username, password)
        db_result = models.UserHistory(user_id=db_user.id, description=message)
        db.add(db_result)
        db.commit()
        logger.info(f'{str(datetime.now()).split(".", 2)[0]} - User {db_user.username} message="{message}"')
        # print(f'{str(datetime.now()).split(".", 2)[0]} - User {db_user.username} message="{message}"')
    except Exception as err:
        msg = f"Error in the adding process write_user_history " f"(user_name={username}): {err}"
        logger.error(msg)
        raise HTTPException(status_code=409, detail=msg)
    return db_result


def get_user_by_email(db: Session, email: str):
    return db.query(models.User).filter(models.User.email == email).first()


def get_users(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.User).offset(skip).limit(limit).all()


def verify_password(plain_password, hashed_password):
    # print('plain_password=', plain_password)
    # print('hashed_password=', hashed_password)
    # hashed_password = get_password_hash(plain_password)
    # print(f'verify={pwd_context.verify(plain_password, hashed_password)} get_password_hash={hashed_password}')
    # hashed_password = encrypt_password(plain_password)
    # print(f'verify={pwd_context.verify(plain_password, hashed_password)} encrypt_password={hashed_password}')
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def encrypt_password(password):
    return pwd_context.encrypt(password)


def authenticate_user(db: Session, username: str, password: str):
    user = get_or_create_user(db, username, password)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user
