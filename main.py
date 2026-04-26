import os
from datetime import date, datetime, time
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery


PROJECT_ID = os.getenv("PROJECT_ID", "mgmt-545-coffee-shop-project")
BQ_DATASET = os.getenv("BQ_DATASET", "uncle_joes")

LOCATIONS_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.locations`"
MENU_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.menu_items`"

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*")
ALLOW_ORIGINS = [origin.strip() for origin in CORS_ORIGINS.split(",")]

app = FastAPI(title="Uncle Joe's Coffee Shop API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client(project=PROJECT_ID)


def clean_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def rows_to_dicts(rows):
    results = []

    for row in rows:
        row_dict = dict(row.items())
        clean_row = {}

        for key, value in row_dict.items():
            clean_row[key] = clean_value(value)

        results.append(clean_row)

    return results


@app.get("/")
def root():
    return {
        "message": "Uncle Joe's API is running",
        "docs": "/docs",
        "endpoints": [
            "/locations",
            "/locations/{location_id}",
            "/menu",
            "/menu/{item_id}",
        ],
    }


@app.get("/locations")
def get_locations(
    state: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    where_parts = []
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]

    if state:
        where_parts.append("UPPER(state) = UPPER(@state)")
        params.append(bigquery.ScalarQueryParameter("state", "STRING", state))

    if city:
        where_parts.append("UPPER(city) = UPPER(@city)")
        params.append(bigquery.ScalarQueryParameter("city", "STRING", city))

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        {where_clause}
        ORDER BY state, city, address_one
        LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    rows = client.query(sql, job_config=job_config).result()
    return rows_to_dicts(rows)


@app.get("/locations/{location_id}")
def get_location(location_id: str):
    sql = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE CAST(id AS STRING) = @location_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("location_id", "STRING", location_id)
        ]
    )

    rows = client.query(sql, job_config=job_config).result()
    results = rows_to_dicts(rows)

    if not results:
        raise HTTPException(status_code=404, detail="Location not found")

    return results[0]


@app.get("/menu")
def get_menu(
    category: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    where_clause = ""
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", limit),
        bigquery.ScalarQueryParameter("offset", "INT64", offset),
    ]

    if category:
        where_clause = "WHERE UPPER(category) = UPPER(@category)"
        params.append(bigquery.ScalarQueryParameter("category", "STRING", category))

    sql = f"""
        SELECT *
        FROM {MENU_TABLE}
        {where_clause}
        ORDER BY category, name, size
        LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(query_parameters=params)

    rows = client.query(sql, job_config=job_config).result()
    return rows_to_dicts(rows)


@app.get("/menu/{item_id}")
def get_menu_item(item_id: str):
    sql = f"""
        SELECT *
        FROM {MENU_TABLE}
        WHERE CAST(id AS STRING) = @item_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("item_id", "STRING", item_id)
        ]
    )

    rows = client.query(sql, job_config=job_config).result()
    results = rows_to_dicts(rows)

    if not results:
        raise HTTPException(status_code=404, detail="Menu item not found")

    return results[0]
    
@app.get("/menu/category/{category_name}")
def get_menu_by_category(
    category_name: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    sql = f"""
        SELECT *
        FROM {MENU_TABLE}
        WHERE UPPER(category) = UPPER(@category)
        ORDER BY name, size
        LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category_name),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
    )

    rows = client.query(sql, job_config=job_config).result()
    return rows_to_dicts(rows)

@app.get("/locations/city/{city_name}")
def get_locations_by_city(
    city_name: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    sql = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE LOWER(city) = LOWER(@city)
        ORDER BY city, state
        LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("city", "STRING", city_name),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
    )

    rows = client.query(sql, job_config=job_config).result()
    return rows_to_dicts(rows)

@app.get("/locations/state/{state_code}")
def get_locations_by_state(
    state_code: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    sql = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE LOWER(state) = LOWER(@state)
        ORDER BY city, state
        LIMIT @limit OFFSET @offset
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("state", "STRING", state_code),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
            bigquery.ScalarQueryParameter("offset", "INT64", offset),
        ]
    )

    rows = client.query(sql, job_config=job_config).result()
    return rows_to_dicts(rows)
