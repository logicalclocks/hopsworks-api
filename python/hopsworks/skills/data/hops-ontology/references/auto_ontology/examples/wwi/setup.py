#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Create the sales example of the ontology factory: a small wholesale schema and the MySQL data source `sales_mysql` that reads it.

    python setup.py

The six `sales_*` tables go into the project's own online database, through the caller's online storage connector, because a member can create tables there and nowhere else.
Run it as a Data owner: a Data scientist's database user can only read.
It is idempotent: a missing table is created, an empty one is filled, and an existing data source is kept.
It prints the `sources` entry for ontology.yaml, as JSON.

The data source carries the caller's online database credentials with SHARED access, so every Data owner and Data scientist of the project can read them and write to the project's online database with them.
Build the example in a project made for it.
"""

from __future__ import annotations

import datetime
import json
import random
from urllib.parse import urlsplit


SOURCE = "sales_mysql"
TABLE_REGEX = "^sales_"
SEED = 7

TABLES = {
    "sales_state_provinces": """
        state_province_id INT NOT NULL PRIMARY KEY,
        name VARCHAR(64) NOT NULL,
        code CHAR(2) NOT NULL,
        country VARCHAR(64) NOT NULL,
        sales_territory VARCHAR(32) NOT NULL""",
    "sales_cities": """
        city_id INT NOT NULL PRIMARY KEY,
        name VARCHAR(64) NOT NULL,
        state_province_id INT NOT NULL,
        population INT NOT NULL,
        KEY (state_province_id),
        FOREIGN KEY (state_province_id) REFERENCES sales_state_provinces (state_province_id)""",
    "sales_customers": """
        customer_id INT NOT NULL PRIMARY KEY,
        name VARCHAR(128) NOT NULL,
        category VARCHAR(32) NOT NULL,
        city_id INT NOT NULL,
        credit_limit DECIMAL(10, 2) NOT NULL,
        account_opened DATE NOT NULL,
        KEY (city_id),
        FOREIGN KEY (city_id) REFERENCES sales_cities (city_id)""",
    "sales_products": """
        product_id INT NOT NULL PRIMARY KEY,
        name VARCHAR(128) NOT NULL,
        category VARCHAR(32) NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        stock_on_hand INT NOT NULL""",
    "sales_orders": """
        order_id INT NOT NULL PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        salesperson VARCHAR(64) NOT NULL,
        status VARCHAR(16) NOT NULL,
        KEY (customer_id),
        FOREIGN KEY (customer_id) REFERENCES sales_customers (customer_id)""",
    "sales_order_lines": """
        order_line_id INT NOT NULL PRIMARY KEY,
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        KEY (order_id),
        KEY (product_id),
        FOREIGN KEY (order_id) REFERENCES sales_orders (order_id),
        FOREIGN KEY (product_id) REFERENCES sales_products (product_id)""",
}

STATES = [
    (1, "California", "CA", "West"),
    (2, "Texas", "TX", "Southwest"),
    (3, "New York", "NY", "Northeast"),
    (4, "Florida", "FL", "Southeast"),
    (5, "Illinois", "IL", "Great Lakes"),
    (6, "Washington", "WA", "West"),
    (7, "Georgia", "GA", "Southeast"),
    (8, "Massachusetts", "MA", "Northeast"),
]
CITIES = [
    (1, "Los Angeles", 1, 3898747),
    (2, "Fresno", 1, 542107),
    (3, "Houston", 2, 2304580),
    (4, "Austin", 2, 961855),
    (5, "Buffalo", 3, 278349),
    (6, "Albany", 3, 99224),
    (7, "Miami", 4, 442241),
    (8, "Tampa", 4, 384959),
    (9, "Chicago", 5, 2746388),
    (10, "Springfield", 5, 114394),
    (11, "Seattle", 6, 737015),
    (12, "Spokane", 6, 228989),
    (13, "Atlanta", 7, 498715),
    (14, "Savannah", 7, 147780),
    (15, "Boston", 8, 675647),
    (16, "Worcester", 8, 206518),
]
CUSTOMER_NAMES = [
    "Tailspin Toys",
    "Wingtip Toys",
    "Northwind Gifts",
    "Contoso Novelty",
    "Fabrikam Retail",
]
CUSTOMER_CATEGORIES = [
    "Novelty Shop",
    "Supermarket",
    "Gift Store",
    "Computer Store",
    "Corporate",
]
# (name, category, unit price in cents, stock on hand)
PRODUCTS = [
    ("USB rocket launcher", "Novelty", 2500, 140),
    ("Ride on toy sedan car", "Toys", 23000, 12),
    ("Developer joke mug", "Mugs", 1300, 420),
    ("Halloween zombie mask", "Novelty", 1800, 95),
    ("Shipping carton 300x300", "Packaging", 150, 2600),
    ("Bubblewrap dispenser", "Packaging", 4200, 36),
    ("Plush shark slippers", "Clothing", 3200, 64),
    ("Furry animal socks", "Clothing", 500, 880),
    ("Superhero action jacket", "Clothing", 6000, 41),
    ("Remote control monster truck", "Toys", 7500, 27),
    ("DBA joke mug", "Mugs", 1300, 310),
    ("Packing tape 48mm", "Packaging", 300, 1900),
    ("Novelty chilli chocolates", "Novelty", 800, 230),
    ("Wooden puzzle cube", "Toys", 1500, 180),
    ("Office desk toy", "Novelty", 2200, 18),
    ("Black ceramic mug", "Mugs", 1100, 560),
    ("Kids hoodie", "Clothing", 3500, 72),
    ("Build your own robot kit", "Toys", 9800, 9),
    ("Gift wrap roll", "Packaging", 600, 740),
    ("Retro tin wind-up robot", "Toys", 4500, 33),
]
SALESPEOPLE = [
    "Kayla Woodcock",
    "Hudson Onslow",
    "Archer Lamble",
    "Lily Code",
    "Taj Shand",
]
STATUSES = ["Delivered", "Shipped", "Cancelled"]
STATUS_WEIGHTS = [80, 12, 8]
ORDERS = 150


def generate(seed: int = SEED) -> dict[str, list[tuple]]:
    """The rows of every table, the same for the same seed; prices are in cents."""
    rng = random.Random(seed)
    customers = []
    for customer_id in range(1, 41):
        city_id = rng.randint(1, len(CITIES))
        prefix = CUSTOMER_NAMES[(customer_id - 1) % len(CUSTOMER_NAMES)]
        customers.append(
            (
                customer_id,
                f"{prefix} ({CITIES[city_id - 1][1]} {customer_id})",
                rng.choice(CUSTOMER_CATEGORIES),
                city_id,
                rng.choice([5000, 10000, 20000, 50000]) * 100,
                datetime.date(2020, 1, 1)
                + datetime.timedelta(days=rng.randint(0, 1800)),
            )
        )
    products = [(i, *p) for i, p in enumerate(PRODUCTS, start=1)]
    orders, lines = [], []
    for order_id in range(1, ORDERS + 1):
        orders.append(
            (
                order_id,
                rng.randint(1, len(customers)),
                datetime.date(2025, 1, 1)
                + datetime.timedelta(days=rng.randint(0, 364)),
                rng.choice(SALESPEOPLE),
                rng.choices(STATUSES, STATUS_WEIGHTS)[0],
            )
        )
        for product in rng.sample(products, rng.randint(1, 4)):
            lines.append(
                (len(lines) + 1, order_id, product[0], rng.randint(1, 20), product[3])
            )
    return {
        "sales_state_provinces": [(*s[:3], "United States", s[3]) for s in STATES],
        "sales_cities": list(CITIES),
        "sales_customers": customers,
        "sales_products": products,
        "sales_orders": orders,
        "sales_order_lines": lines,
    }


def _stored(table: str, row: tuple) -> tuple:
    # Money is generated in cents and stored as DECIMAL(10, 2).
    cents = {"sales_customers": 4, "sales_products": 3, "sales_order_lines": 4}.get(
        table
    )
    if cents is None:
        return row
    return (*row[:cents], f"{row[cents] / 100:.2f}", *row[cents + 1 :])


def _connect(url, options: dict):
    import pymysql

    return pymysql.connect(
        host=url.hostname,
        port=url.port or 3306,
        user=options["user"],
        password=options["password"],
        database=url.path.lstrip("/"),
        autocommit=True,
    )


def _create_tables(conn, rows: dict[str, list[tuple]]) -> None:
    with conn.cursor() as cursor:
        for table, columns in TABLES.items():
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ({columns}) ENGINE=NDBCLUSTER"
            )
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            if cursor.fetchone()[0]:
                continue
            values = [_stored(table, row) for row in rows[table]]
            marks = ", ".join(["%s"] * len(values[0]))
            cursor.executemany(f"INSERT INTO {table} VALUES ({marks})", values)


def _create_source(fs, url, options: dict) -> None:
    from hopsworks_common.client.exceptions import RestAPIError
    from hsfs.storage_connector import SqlConnector

    try:
        fs.get_data_source(SOURCE)
        return
    except RestAPIError as exc:
        if getattr(exc.response, "status_code", None) != 404:
            raise
    fs.create_data_source(
        SqlConnector(
            id=None,
            name=SOURCE,
            featurestore_id=fs.id,
            description="The ontology factory's sales example: the sales_* tables of the project's online database",
            database_type=SqlConnector.MYSQL,
            host=url.hostname,
            port=url.port or 3306,
            database=url.path.lstrip("/"),
            user=options["user"],
            password=options["password"],
            credentials_mode=SqlConnector.SHARED,
        )
    )


def main() -> None:
    """Create the tables, fill them, create the data source, and print the sources entry."""
    import hopsworks

    fs = hopsworks.login().get_feature_store()
    options = fs.get_online_storage_connector().spark_options()
    url = urlsplit(options["url"].removeprefix("jdbc:"))
    with _connect(url, options) as conn:
        _create_tables(conn, generate())
    _create_source(fs, url, options)
    print(
        json.dumps(
            {
                "name": SOURCE,
                "schemas": [url.path.lstrip("/")],
                "table_regex": TABLE_REGEX,
            }
        )
    )


if __name__ == "__main__":
    main()
