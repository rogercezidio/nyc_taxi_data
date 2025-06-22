from __future__ import annotations
import os, re
from pathlib import Path
from dataclasses import dataclass
from typing import Tuple, FrozenSet, Optional
from pyspark.sql import SparkSession, functions as F, types as T     
from dotenv import load_dotenv, find_dotenv

def _load_env() -> None:
    candidates = []

    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parent / ".env")

    candidates.append(Path.cwd() / ".env")

    dotenv_auto = find_dotenv(usecwd=True)
    if dotenv_auto:
        candidates.append(Path(dotenv_auto))

    for env_path in candidates:
        if env_path and env_path.is_file():
            load_dotenv(env_path, override=True)
            print(f".env carregado de {env_path}")
            break

_load_env()

def _secret(key: str, scope: str = "cfg") -> str | None:
    """Lê `dbutils.secrets.get(scope, key)` somente se dbutils existir."""
    dbutils = globals().get("dbutils")
    try:
        return dbutils.secrets.get(scope, key) if dbutils else None
    except Exception:
        return None


def _cfg(key: str, *, default: str | None = None, required: bool = False) -> str:
    """
    Precedência:
      1) variável de ambiente (via .env ou cluster env)
      2) Databricks secret (`cfg/<key>`)
      3) default explícito
    """
    val = os.getenv(key) or _secret(key.lower()) or default
    if required and not val:
        raise ValueError(
            f"Configuração obrigatória '{key}' não encontrada "
            "(defina em .env, variável de ambiente ou secret scope)."
        )
    return val

# 2.  Configurações globais
BUCKET_BASE = _cfg("BUCKET_BASE", required=True)
TAG         = _cfg("TAG",         required=True)
CATALOG     = _cfg("CATALOG",     required=True)
CREDS_NAME  = _cfg("CREDS_NAME",  required=True)

# 3.  Spark utils
def _get_spark() -> Optional[SparkSession]:
    """Devolve a SparkSession ativa ou cria uma nova (se possível)."""
    try:
        s = SparkSession.getActiveSession()
        return s or SparkSession.builder.getOrCreate()
    except Exception: 
        return None


def table_exists(name: str) -> bool:
    """Testa, com segurança, se uma tabela Delta/Spark existe."""
    spark = _get_spark()
    return bool(spark and spark.catalog.tableExists(name))


# 4.  Helpers 
def _schema(layer: str) -> str:
    return f"{CATALOG}.{layer}"                   

def _root(layer: str, artef: str = "volumes") -> str:
    if layer == "raw":
        return f"/Volumes/{CATALOG}/raw/{TAG}_ingest"
    return f"{BUCKET_BASE}/{layer}/{artef}/{TAG}/"


# 5.  Constantes e metadados das camadas
TAXI_TYPES: Tuple[str, ...] = ("yellow", "green", "fhv", "fhvhv")


@dataclass(frozen=True)
class RAW:
    YEARS     : Tuple[int, ...] = tuple(range(2023, 2026))
    MONTHS    : Tuple[str, ...] = tuple(f"{m:02d}" for m in range(1, 13))
    BASE_URL  : str            = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    DEST_ROOT : str            = _root("raw")
    CHUNK     : int            = 8 * 1024 * 1024


@dataclass(frozen=True)
class BRONZE:
    SCHEMA: str = _schema("bronze")
    ROOT: str = _root("bronze")
    QUARANTINE: str = _root("quarentine")
    RAW_ROOT: str = RAW.DEST_ROOT

    REQUIRED_COLS: FrozenSet[str] = frozenset({
        "vendor_id", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"
    })
    COLS_FORCE_DOUBLE: FrozenSet[str] = frozenset({
        "passenger_count", "ratecodeid", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee", "trip_distance", "fare_amount"
    })


@dataclass(frozen=True)
class SILVER:
    SCHEMA: str = _schema("silver")
    ROOT: str = _root("silver")
    SRC_SCHEMA: str = BRONZE.SCHEMA


@dataclass(frozen=True)
class GOLD:
    SCHEMA: str = _schema("gold")
    ROOT: str = _root("gold")
    SILVER_SCHEMA: str = SILVER.SCHEMA
    TBL_DIM_DATE: str = f"{SCHEMA}.dim_date"
    TBL_FACT_TRIPS: str = f"{SCHEMA}.fact_trips"
