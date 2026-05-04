import os
from datetime import date, datetime, time
from decimal import Decimal

import bcrypt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel


PROJECT_ID = os.getenv("PROJECT_ID", "mgmt-545-coffee-shop-project")
BQ_DATASET = os.getenv("BQ_DATASET", "uncle_joes")

LOCATIONS_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.locations`"
MENU_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.menu_items`"
MEMBERS_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.members`"
ORDERS_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.orders`"
ORDER_ITEMS_TABLE = f"`{PROJECT_ID}.{BQ_DATASET}.order_items`"

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


class LoginRequest(BaseModel):
    email: str
    password: str


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
            "/locations/city/{city_name}",
            "/locations/state/{state_code}",
            "/menu",
            "/menu/{item_id}",
            "/menu/category/{category_name}",
            "/login",
            "/members/{member_id}/orders",
            "/members/{member_id}/points",
        ],
    }


@app.get("/locations")
def get_locations():
    sql = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        ORDER BY state, city, address_one
    """

    rows = client.query(sql).result()
    return rows_to_dicts(rows)


@app.get("/locations/{location_id}")
def get_location(location_id: str):
    sql = f"""
        SELECT *
        FROM {LOCATIONS_TABLE}
        WHERE CAST(id AS STRING) = @location_id
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


@app.get("/locations/city/{city_name}")
def get_locations_by_city(city_name: str):
    try:
        sql = f"""
            SELECT *
            FROM {LOCATIONS_TABLE}
            WHERE LOWER(city) = LOWER(@city)
            ORDER BY city, state, address_one
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city", "STRING", city_name),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        results = rows_to_dicts(rows)

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No locations found for city '{city_name}'.",
            )

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Server error while fetching locations: {str(e)}",
        )


@app.get("/locations/state/{state_code}")
def get_locations_by_state(state_code: str):
    try:
        sql = f"""
            SELECT *
            FROM {LOCATIONS_TABLE}
            WHERE LOWER(state) = LOWER(@state)
            ORDER BY city, address_one
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("state", "STRING", state_code),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        results = rows_to_dicts(rows)

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No locations found for state '{state_code}'.",
            )

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Server error while fetching locations: {str(e)}",
        )


@app.get("/menu")
def get_menu():
    sql = f"""
        SELECT *
        FROM {MENU_TABLE}
        ORDER BY category, name, size
    """

    rows = client.query(sql).result()
    return rows_to_dicts(rows)


@app.get("/menu/{item_id}")
def get_menu_item(item_id: str):
    sql = f"""
        SELECT *
        FROM {MENU_TABLE}
        WHERE CAST(id AS STRING) = @item_id
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
def get_menu_by_category(category_name: str):
    try:
        sql = f"""
            SELECT *
            FROM {MENU_TABLE}
            WHERE UPPER(category) = UPPER(@category)
            ORDER BY name, size
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("category", "STRING", category_name),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        results = rows_to_dicts(rows)

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No menu items found for category '{category_name}'.",
            )

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Server error while fetching menu items: {str(e)}",
        )


@app.post("/login")
def login(credentials: LoginRequest):
    try:
        sql = f"""
            SELECT 
                string_field_0 AS member_id,
                string_field_1 AS first_name,
                string_field_2 AS last_name,
                string_field_3 AS email,
                string_field_5 AS home_store,
                string_field_6 AS password_hash
            FROM {MEMBERS_TABLE}
            WHERE LOWER(string_field_3) = LOWER(@email)
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email", "STRING", credentials.email),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        results = rows_to_dicts(rows)

        if not results:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        member = results[0]
        stored_hash = member.get("password_hash")

        if not stored_hash:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        password_matches = bcrypt.checkpw(
            credentials.password.encode("utf-8"),
            stored_hash.encode("utf-8"),
        )

        if not password_matches:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        return {
            "success": True,
            "member": {
                "member_id": member["member_id"],
                "first_name": member["first_name"],
                "last_name": member["last_name"],
                "email": member["email"],
                "home_store": member["home_store"],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login error: {str(e)}")


@app.get("/members/{member_id}/orders")
def get_member_orders(member_id: str):
    try:
        sql = f"""
            SELECT
                o.order_id,
                o.order_date,
                o.order_total,
                o.store_id,
                l.city AS store_city,
                l.state AS store_state,
                l.address_one AS store_address,
                oi.id AS order_item_id,
                oi.menu_item_id,
                oi.item_name,
                oi.size,
                oi.quantity,
                oi.price
            FROM {ORDERS_TABLE} o
            LEFT JOIN {LOCATIONS_TABLE} l
                ON o.store_id = l.id
            LEFT JOIN {ORDER_ITEMS_TABLE} oi
                ON o.order_id = oi.order_id
            WHERE o.member_id = @member_id
            ORDER BY o.order_date DESC, o.order_id, oi.item_name
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("member_id", "STRING", member_id),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        flat_rows = rows_to_dicts(rows)

        orders = {}

        for row in flat_rows:
            order_id = row["order_id"]

            if order_id not in orders:
                orders[order_id] = {
                    "order_id": row["order_id"],
                    "order_date": row["order_date"],
                    "order_total": row["order_total"],
                    "store": {
                        "store_id": row["store_id"],
                        "city": row["store_city"],
                        "state": row["store_state"],
                        "address": row["store_address"],
                    },
                    "line_items": [],
                }

            if row["order_item_id"] is not None:
                orders[order_id]["line_items"].append(
                    {
                        "order_item_id": row["order_item_id"],
                        "menu_item_id": row["menu_item_id"],
                        "item_name": row["item_name"],
                        "size": row["size"],
                        "quantity": row["quantity"],
                        "price": row["price"],
                    }
                )

        return {
            "member_id": member_id,
            "orders": list(orders.values()),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching orders: {str(e)}")


@app.get("/members/{member_id}/points")
def get_member_points(member_id: str):
    try:
        sql = f"""
            SELECT 
                COALESCE(CAST(SUM(FLOOR(order_total)) AS INT64), 0) AS points
            FROM {ORDERS_TABLE}
            WHERE member_id = @member_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("member_id", "STRING", member_id),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        results = rows_to_dicts(rows)

        points = 0
        if results:
            points = results[0].get("points", 0)

        return {
            "member_id": member_id,
            "points": points,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating points: {str(e)}")
    # Add this Pydantic model at the top with the other models (after LoginRequest)
class OrderItemRequest(BaseModel):
    menu_item_id: str
    quantity: int
    price: float
    size: str


class CreateOrderRequest(BaseModel):
    member_id: str
    store_id: str
    items: list[OrderItemRequest]


# Add this endpoint to main.py
@app.post("/orders")
def create_order(order_request: CreateOrderRequest):
    try:
        # Calculate order total
        order_total = sum(item.price * item.quantity for item in order_request.items)
        
        # Insert order into ORDERS_TABLE
        insert_order_sql = f"""
            INSERT INTO {ORDERS_TABLE} (member_id, store_id, order_date, order_total)
            VALUES (@member_id, @store_id, CURRENT_TIMESTAMP(), @order_total)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("member_id", "STRING", order_request.member_id),
                bigquery.ScalarQueryParameter("store_id", "STRING", order_request.store_id),
                bigquery.ScalarQueryParameter("order_total", "FLOAT64", order_total),
            ]
        )
        
        client.query(insert_order_sql, job_config=job_config).result()
        
        # Get the newly created order_id
        get_order_id_sql = f"""
            SELECT order_id
            FROM {ORDERS_TABLE}
            WHERE member_id = @member_id
            ORDER BY order_date DESC
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("member_id", "STRING", order_request.member_id),
            ]
        )
        
        rows = client.query(get_order_id_sql, job_config=job_config).result()
        results = rows_to_dicts(rows)
        
        if not results:
            raise Exception("Failed to retrieve order ID")
        
        order_id = results[0]["order_id"]
        
        # Insert order items into ORDER_ITEMS_TABLE
        for item in order_request.items:
            insert_item_sql = f"""
                INSERT INTO {ORDER_ITEMS_TABLE} 
                (order_id, menu_item_id, item_name, size, quantity, price)
                VALUES (@order_id, @menu_item_id, @item_name, @size, @quantity, @price)
            """
            
            # Get item name from MENU_TABLE
            get_item_name_sql = f"""
                SELECT name
                FROM {MENU_TABLE}
                WHERE CAST(id AS STRING) = @menu_item_id
            """
            
            item_name_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("menu_item_id", "STRING", item.menu_item_id),
                ]
            )
            
            item_rows = client.query(get_item_name_sql, job_config=item_name_config).result()
            item_results = rows_to_dicts(item_rows)
            
            if not item_results:
                raise HTTPException(status_code=404, detail=f"Menu item {item.menu_item_id} not found")
            
            item_name = item_results[0]["name"]
            
            item_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("order_id", "STRING", order_id),
                    bigquery.ScalarQueryParameter("menu_item_id", "STRING", item.menu_item_id),
                    bigquery.ScalarQueryParameter("item_name", "STRING", item_name),
                    bigquery.ScalarQueryParameter("size", "STRING", item.size),
                    bigquery.ScalarQueryParameter("quantity", "INT64", item.quantity),
                    bigquery.ScalarQueryParameter("price", "FLOAT64", item.price),
                ]
            )
            
            client.query(insert_item_sql, job_config=item_config).result()
        
        return {
            "success": True,
            "order_id": order_id,
            "member_id": order_request.member_id,
            "store_id": order_request.store_id,
            "order_total": order_total,
            "item_count": len(order_request.items),
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating order: {str(e)}")
