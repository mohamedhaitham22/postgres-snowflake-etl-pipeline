"""Configuration module for Shipping ETL.
Loads env vars (optionally via python-dotenv if installed) and prompts for blank passwords.
"""
from __future__ import annotations
import os
import getpass
from dataclasses import dataclass

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str
    schema: str
    fail_fast: bool

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str

@dataclass(frozen=True)
class AppConfig:
    postgres: PostgresConfig
    snowflake: SnowflakeConfig


def _prompt_if_blank(value: str, prompt: str) -> str:
    return value if value else getpass.getpass(prompt)


def load_config() -> AppConfig:
    pg_pw = os.environ.get("PG_PW", "")
    sf_pw = os.environ.get("SF_PASSWORD", "")
    pg = PostgresConfig(
        host=os.environ.get("PG_HOST", "put_your_host"),
        port=int(os.environ.get("PG_PORT", "put_your_port")),
        db=os.environ.get("PG_DB", "put_your_db"),
        user=os.environ.get("PG_USER", "put_your_user"),
        password=_prompt_if_blank(pg_pw, "Postgres password: "),
        schema=os.environ.get("PG_SCHEMA", "put_your_schema"),
        fail_fast=os.environ.get("FAIL_FAST", "1") in ("1","true","True"),
    )
    sf = SnowflakeConfig(
        account=os.environ.get("SF_ACCOUNT", "put_your_account"),
        user=os.environ.get("SF_USER", "put_your_user"),
        password=_prompt_if_blank(sf_pw, "Snowflake password: "),
        role=os.environ.get("SF_ROLE", "put_your_role"),
        warehouse=os.environ.get("SF_WAREHOUSE", "put_your_warehouse"),
        database=os.environ.get("SF_DATABASE", "put_your_database"),
        schema=os.environ.get("SF_SCHEMA", "put_your_schema"),
    )
    return AppConfig(postgres=pg, snowflake=sf)

__all__ = ["load_config","AppConfig","PostgresConfig","SnowflakeConfig"]
