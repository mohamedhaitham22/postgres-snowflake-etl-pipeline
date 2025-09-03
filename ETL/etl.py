"""Core ETL logic separated from orchestration."""
from __future__ import annotations
import uuid
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine, text
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from config import AppConfig

TABLE_CUSTOMERS = "customers"
TABLE_SHIPS = "ships"
TABLE_PORTS = "ports"
TABLE_SHIPMENTS = "shipments"
TABLE_SHIPMENT_ITEMS = "shipment_items"

DIM_CUSTOMERS = "DIM_CUSTOMERS"
DIM_SHIPS = "DIM_SHIPS"
DIM_PORTS = "DIM_PORTS"
FACT_SHIPMENTS = "FACT_SHIPMENTS"

# Connections

def pg_engine(cfg: AppConfig):
    pg = cfg.postgres
    url = f"postgresql+psycopg2://{pg.user}:{pg.password}@{pg.host}:{pg.port}/{pg.db}"
    return create_engine(url, pool_pre_ping=True)

def sf_connect(cfg: AppConfig):
    sf = cfg.snowflake
    return snowflake.connector.connect(
        account=sf.account,
        user=sf.user,
        password=sf.password,
        role=sf.role,
        warehouse=sf.warehouse,
        database=sf.database,
        schema=sf.schema,
    )

# Extract

def extract_all(engine, schema: str) -> Dict[str, pd.DataFrame]:
    tables = [TABLE_CUSTOMERS, TABLE_SHIPS, TABLE_PORTS, TABLE_SHIPMENTS, TABLE_SHIPMENT_ITEMS]
    dfs = {}
    with engine.connect() as conn:
        for t in tables:
            fq = f'"{schema}"."{t}"'
            print(f"Extracting {fq} ...")
            dfs[t] = pd.read_sql(text(f"SELECT * FROM {fq}"), conn)
            print(f"  rows: {len(dfs[t])}")
    return dfs

# Transform

def transform_dimensions(dfs: Dict[str, pd.DataFrame]):
    cust = dfs[TABLE_CUSTOMERS].copy()
    ship = dfs[TABLE_SHIPS].copy()
    port = dfs[TABLE_PORTS].copy()
    for frame in (cust, ship, port):
        for col in frame.select_dtypes(include=['object']).columns:
            frame[col] = frame[col].str.strip()
    cust = cust.drop_duplicates(subset=['customer_id'])
    ship = ship.drop_duplicates(subset=['ship_id'])
    port = port.drop_duplicates(subset=['port_id'])
    return cust, ship, port

def transform_fact(dfs: Dict[str, pd.DataFrame]):
    shipments = dfs[TABLE_SHIPMENTS].copy()
    items = dfs[TABLE_SHIPMENT_ITEMS].copy()
    agg = items.groupby('shipment_id').agg(total_weight=('weight','sum'), total_cost=('cost','sum')).reset_index()
    fact = shipments.merge(agg, on='shipment_id', how='left')
    fact['total_weight'] = fact['total_weight'].fillna(0)
    fact['total_cost'] = fact['total_cost'].fillna(0)
    return fact

# Load helpers

def ensure_session(conn, cfg: AppConfig):
    sf = cfg.snowflake
    with conn.cursor() as cur:
        cur.execute(f'USE WAREHOUSE "{sf.warehouse}"')
        cur.execute(f'USE DATABASE "{sf.database}"')
        cur.execute(f'USE SCHEMA "{sf.schema}"')

def stage_dataframe(conn, df: pd.DataFrame, target_table: str, cfg: AppConfig) -> str:
    staging = f"STG_{target_table}_{uuid.uuid4().hex[:8]}".upper()
    print(f"Staging {len(df)} rows into temporary table {staging} ...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=staging,
        database=cfg.snowflake.database,
        schema=cfg.snowflake.schema,
        auto_create_table=True
    )
    if not success:
        raise RuntimeError(f"Failed staging load for {staging}")
    print(f"  staged rows: {nrows}")
    return staging

def merge_table(conn, staging_table: str, target_table: str, key_columns, update_columns):
    on_clause = " AND ".join([f"T.{k} = S.{k}" for k in key_columns])
    set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_columns])
    insert_cols = key_columns + update_columns
    insert_list = ", ".join(insert_cols)
    values_list = ", ".join([f"S.{c}" for c in insert_cols])
    sql = f"""
        MERGE INTO {target_table} T
        USING {staging_table} S
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_list}) VALUES ({values_list})
    """
    print(f"MERGE {target_table} ...")
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(f"DROP TABLE {staging_table}")

def load_dimensions(conn, cust, ship, port, cfg: AppConfig):
    cust = cust.rename(columns=str.upper)
    ship = ship.rename(columns=str.upper)
    port = port.rename(columns=str.upper)
    staging_c = stage_dataframe(conn, cust, DIM_CUSTOMERS+"_TMP", cfg)
    staging_s = stage_dataframe(conn, ship, DIM_SHIPS+"_TMP", cfg)
    staging_p = stage_dataframe(conn, port, DIM_PORTS+"_TMP", cfg)
    merge_table(conn, staging_c, DIM_CUSTOMERS, ["CUSTOMER_ID"], [c for c in cust.columns if c != "CUSTOMER_ID"])
    merge_table(conn, staging_s, DIM_SHIPS, ["SHIP_ID"], [c for c in ship.columns if c != "SHIP_ID"])
    merge_table(conn, staging_p, DIM_PORTS, ["PORT_ID"], [c for c in port.columns if c != "PORT_ID"])

def fetch_surrogate_mappings(conn):
    mapping = {}
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(f"SELECT CUSTOMER_ID, CUSTOMER_KEY FROM {DIM_CUSTOMERS}")
        mapping['customer'] = {row['CUSTOMER_ID']: row['CUSTOMER_KEY'] for row in cur.fetchall()}
        cur.execute(f"SELECT SHIP_ID, SHIP_KEY FROM {DIM_SHIPS}")
        mapping['ship'] = {row['SHIP_ID']: row['SHIP_KEY'] for row in cur.fetchall()}
        cur.execute(f"SELECT PORT_ID, PORT_KEY FROM {DIM_PORTS}")
        mapping['port'] = {row['PORT_ID']: row['PORT_KEY'] for row in cur.fetchall()}
    return mapping

def load_fact(conn, fact_df, mappings, cfg: AppConfig):
    f = fact_df.copy()
    f['customer_key'] = f['customer_id'].map(mappings['customer'])
    f['ship_key'] = f['ship_id'].map(mappings['ship'])
    f['origin_port_key'] = f['origin_port'].map(mappings['port'])
    f['destination_port_key'] = f['destination_port'].map(mappings['port'])
    before = len(f)
    f = f.dropna(subset=['customer_key','ship_key','origin_port_key','destination_port_key'])
    if before - len(f):
        print(f"Dropped {before - len(f)} fact rows due to missing dimension keys.")
    load_cols = ['shipment_id','customer_key','ship_key','origin_port_key','destination_port_key','shipment_date','delivery_date','status','total_weight','total_cost']
    f = f[load_cols]
    f.columns = [c.upper() for c in f.columns]
    staging = stage_dataframe(conn, f, FACT_SHIPMENTS+"_TMP", cfg)
    merge_table(conn, staging, FACT_SHIPMENTS, ["SHIPMENT_ID"], [
        'CUSTOMER_KEY','SHIP_KEY','ORIGIN_PORT_KEY','DESTINATION_PORT_KEY','SHIPMENT_DATE','DELIVERY_DATE','STATUS','TOTAL_WEIGHT','TOTAL_COST'
    ])

__all__ = [
    'pg_engine','sf_connect','extract_all','transform_dimensions','transform_fact',
    'ensure_session','load_dimensions','fetch_surrogate_mappings','load_fact'
]
