from typing import Dict, List
from pydantic import BaseModel, Field


class ClickhouseConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    secure: bool


class AppConfig(BaseModel):
    clickhouse: ClickhouseConfig
