from typing import Union

from pydantic import BaseModel


class OurBaseModel(BaseModel):
    class Config:
        orm_mode = True


class Token(OurBaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class UserBase(OurBaseModel):
    username: str
    email: Union[str, None] = None
    # full_name: Union[str, None] = None
    is_active: Union[bool, None] = None


class UserCreate(UserBase):
    password: str


class User(UserBase):
    id: int
    hashed_password: Union[str, None] = None
