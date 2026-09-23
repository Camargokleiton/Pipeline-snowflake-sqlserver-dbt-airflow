import argparse
import os
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal

import pyodbc
from dotenv import load_dotenv


load_dotenv()

SERVER = os.getenv("SQLSERVER_HOST", "localhost") + "," + os.getenv("MSSQL_PORT", "1433")
USER = os.getenv("SQLSERVER_USER", "sa")
PASSWORD = os.getenv("PASSWORD") or os.getenv("MSSQL_SA_PASSWORD", "")
DATABASE_NAME = os.getenv("DATABASE_NAME", "ERP_Ecommerce")
DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

CONN_STR_MASTER = (
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};UID={USER};PWD={PASSWORD};"
    "TrustServerCertificate=yes;"
)


def get_env_int(name, default):
    value = os.getenv(name, str(default))
    try:
        parsed_value = int(value)
    except ValueError as error:
        raise ValueError(f"{name} deve ser um número inteiro.") from error
    if parsed_value < 0:
        raise ValueError(f"{name} não pode ser negativo.")
    return parsed_value


def get_connection(autocommit=True, db_name=None):
    conn_str = CONN_STR_MASTER
    if db_name:
        conn_str += f"DATABASE={db_name};"

    last_error = None
    for attempt in range(10):
        try:
            return pyodbc.connect(conn_str, autocommit=autocommit)
        except pyodbc.Error as error:
            last_error = error
            print(f"Aguardando o SQL Server (tentativa {attempt + 1}/10)...")
            time.sleep(3)
    raise RuntimeError("Não foi possível conectar ao SQL Server.") from last_error


def ensure_schema():
    safe_database_name = DATABASE_NAME.replace("]", "]] ").replace(" ", "")
    with get_connection() as conn:
        conn.execute(
            f"IF DB_ID(N'{safe_database_name}') IS NULL "
            f"CREATE DATABASE [{safe_database_name}]"
        )

    statements = [
        """
        IF OBJECT_ID('customers', 'U') IS NULL
        CREATE TABLE customers (
            id_customer INT IDENTITY(1,1) PRIMARY KEY,
            full_name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE,
            cpf VARCHAR(14) NOT NULL UNIQUE,
            address VARCHAR(200) NOT NULL,
            city VARCHAR(50) NOT NULL,
            state VARCHAR(2) NOT NULL,
            zip_code VARCHAR(10) NOT NULL,
            created_at DATETIME2 DEFAULT GETDATE(),
            updated_at DATETIME2 DEFAULT GETDATE()
        )
        """,
        """
        IF OBJECT_ID('categories', 'U') IS NULL
        CREATE TABLE categories (
            id_category INT IDENTITY(1,1) PRIMARY KEY,
            category_name VARCHAR(50) NOT NULL,
            description VARCHAR(255)
        )
        """,
        """
        IF OBJECT_ID('products', 'U') IS NULL
        CREATE TABLE products (
            id_product INT IDENTITY(1,1) PRIMARY KEY,
            sku VARCHAR(30) NOT NULL UNIQUE,
            product_name VARCHAR(100) NOT NULL,
            id_category INT NOT NULL FOREIGN KEY REFERENCES categories(id_category),
            sale_price DECIMAL(10,2) NOT NULL,
            unit_cost DECIMAL(10,2) NOT NULL,
            current_stock INT NOT NULL DEFAULT 0,
            created_at DATETIME2 DEFAULT GETDATE(),
            updated_at DATETIME2 DEFAULT GETDATE()
        )
        """,
        """
        IF OBJECT_ID('orders', 'U') IS NULL
        CREATE TABLE orders (
            id_order INT IDENTITY(1,1) PRIMARY KEY,
            id_customer INT NOT NULL FOREIGN KEY REFERENCES customers(id_customer),
            order_date DATETIME2 NOT NULL DEFAULT GETDATE(),
            total_value DECIMAL(10,2) NOT NULL,
            shipping_value DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            current_status VARCHAR(30) NOT NULL DEFAULT 'CREATED',
            updated_at DATETIME2 DEFAULT GETDATE()
        )
        """,
        """
        IF OBJECT_ID('order_items', 'U') IS NULL
        CREATE TABLE order_items (
            id_item INT IDENTITY(1,1) PRIMARY KEY,
            id_order INT NOT NULL FOREIGN KEY REFERENCES orders(id_order),
            id_product INT NOT NULL FOREIGN KEY REFERENCES products(id_product),
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL,
            subtotal AS (quantity * unit_price)
        )
        """,
        """
        IF OBJECT_ID('payments', 'U') IS NULL
        CREATE TABLE payments (
            id_payment INT IDENTITY(1,1) PRIMARY KEY,
            id_order INT NOT NULL FOREIGN KEY REFERENCES orders(id_order),
            payment_method VARCHAR(30) NOT NULL,
            payment_status VARCHAR(30) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            payment_date DATETIME2
        )
        """,
    ]

    with get_connection(db_name=DATABASE_NAME) as conn:
        for statement in statements:
            conn.execute(statement)


def insert_fake_data(counts, seed=None):
    generator = random.Random(seed)
    run_id = datetime.now().strftime("%y%m%d%H%M%S")
    first_names = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Hugo"]
    last_names = ["Almeida", "Barbosa", "Cardoso", "Dias", "Ferreira", "Gomes", "Lima", "Souza"]
    email_domains = ["example.com", "test.com", "demo.com", "sample.com"]
    email_names = first_names + last_names
    email_names = [name.lower() for name in email_names]
    cities = [("Sao Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"), ("Curitiba", "PR")]
    methods = ["CREDIT_CARD", "PIX", "BANK_SLIP"]
    statuses = ["CREATED", "APPROVED", "PICKING", "SHIPPED", "DELIVERED", "CANCELLED"]
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Sports", "Books"]
    product_names = ["fogao", "geladeira", "microondas", "televisao", "celular", "notebook", "camiseta", "tenis"]

    with get_connection(autocommit=False, db_name=DATABASE_NAME) as conn:
        cursor = conn.cursor()
        category_ids = []
        for index in range(counts["categories"]):
            cursor.execute(
                "INSERT INTO categories (category_name, description) OUTPUT INSERTED.id_category VALUES (?, ?)",
                (f"{categories[index % len(categories)]} {index + 1}", "Categoria gerada automaticamente"),
            )
            category_ids.append(cursor.fetchone()[0])

        customer_ids = []
        for index in range(counts["customers"]):
            full_name = f"{generator.choice(first_names)} {generator.choice(last_names)}"
            city, state = generator.choice(cities)
            cursor.execute(
                """INSERT INTO customers
                (full_name, email, cpf, address, city, state, zip_code)
                OUTPUT INSERTED.id_customer VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    full_name,
                    f"cliente{run_id}_{index + 1}@{generator.choice(email_domains)}",
                    f"{generator.randint(10000000000, 99999999999):011d}",
                    f"Rua {generator.choice(last_names)}, {generator.randint(1, 9999)}",
                    city,
                    state,
                    f"{generator.randint(10000000, 99999999):08d}",
                ),
            )
            customer_ids.append(cursor.fetchone()[0])

        product_ids = []
        product_prices = {}
        for index in range(counts["products"]):
            sale_price = Decimal(generator.randint(20, 5000)).quantize(Decimal("0.01"))
            unit_cost = (sale_price * Decimal("0.65")).quantize(Decimal("0.01"))
            cursor.execute(
                """INSERT INTO products
                (sku, product_name, id_category, sale_price, unit_cost, current_stock)
                OUTPUT INSERTED.id_product VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    f"SKU-{run_id}-{index + 1:06d}",
                    f"{generator.choice(product_names).capitalize()}",
                    generator.choice(category_ids),
                    sale_price,
                    unit_cost,
                    generator.randint(0, 250),
                ),
            )
            product_id = cursor.fetchone()[0]
            product_ids.append(product_id)
            product_prices[product_id] = sale_price

        for _ in range(counts["orders"]):
            order_date = datetime.now() - timedelta(days=generator.randint(0, 365))
            shipping = Decimal(generator.choice([0, 12, 19, 29])).quantize(Decimal("0.01"))
            selected_products = generator.sample(product_ids, k=min(generator.randint(1, 4), len(product_ids)))
            items = [(product_id, generator.randint(1, 5), product_prices[product_id]) for product_id in selected_products]
            total = shipping + sum((quantity * price for _, quantity, price in items), Decimal("0.00"))
            status = generator.choice(statuses)
            cursor.execute(
                """INSERT INTO orders
                (id_customer, order_date, total_value, shipping_value, current_status)
                OUTPUT INSERTED.id_order VALUES (?, ?, ?, ?, ?)""",
                (generator.choice(customer_ids), order_date, total, shipping, status),
            )
            order_id = cursor.fetchone()[0]
            for product_id, quantity, price in items:
                cursor.execute(
                    "INSERT INTO order_items (id_order, id_product, quantity, unit_price) VALUES (?, ?, ?, ?)",
                    (order_id, product_id, quantity, price),
                )

            payment_status = "PENDING" if status == "CREATED" else generator.choice(["APPROVED", "REJECTED"])
            cursor.execute(
                """INSERT INTO payments
                (id_order, payment_method, payment_status, amount, payment_date)
                VALUES (?, ?, ?, ?, ?)""",
                (
                    order_id,
                    generator.choice(methods),
                    payment_status,
                    total,
                    None if payment_status == "PENDING" else order_date,
                ),
            )
        conn.commit()

    print(
        f"Inseridos: {counts['categories']} categorias, {counts['customers']} clientes, "
        f"{counts['products']} produtos e {counts['orders']} pedidos."
    )


def main():
    parser = argparse.ArgumentParser(description="Gera dados fake para o ERP_Ecommerce.")
    parser.add_argument("--categories", type=int, default=None, help="Sobrescreve FAKE_CATEGORIES.")
    parser.add_argument("--customers", type=int, default=None, help="Sobrescreve FAKE_CUSTOMERS.")
    parser.add_argument("--products", type=int, default=None, help="Sobrescreve FAKE_PRODUCTS.")
    parser.add_argument("--orders", type=int, default=None, help="Sobrescreve FAKE_ORDERS.")
    parser.add_argument("--seed", type=int, default=None, help="Seed para reproduzir a mesma carga.")
    args = parser.parse_args()
    counts = {
        "categories": args.categories if args.categories is not None else get_env_int("FAKE_CATEGORIES", 5),
        "customers": args.customers if args.customers is not None else get_env_int("FAKE_CUSTOMERS", 50),
        "products": args.products if args.products is not None else get_env_int("FAKE_PRODUCTS", 100),
        "orders": args.orders if args.orders is not None else get_env_int("FAKE_ORDERS", 200),
    }
    if counts["orders"] and (not counts["customers"] or not counts["products"]):
        raise ValueError("Para gerar pedidos, FAKE_CUSTOMERS e FAKE_PRODUCTS devem ser maiores que zero.")
    ensure_schema()
    insert_fake_data(counts, args.seed)


if __name__ == "__main__":
    main()
