from faker import Faker
from trino.dbapi import connect
import random
import datetime

fake = Faker()


def esc(x):
    if x is None:
        return "NULL"
    return str(x).replace("'", "''")


conn = connect(
    host="localhost",
    port=8080,
    user="python-loader",
    catalog="iceberg",
    schema="default",
)
cursor = conn.cursor()

NUM_CUSTOMERS = 20000
NUM_PRODUCTS = 5000
NUM_SALES = 50000
BATCH_SIZE = 1000


def create_tables():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id BIGINT,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            birthdate DATE,
            email VARCHAR,
            phone VARCHAR,
            address VARCHAR,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            postal_code VARCHAR,
            signup_date DATE,
            last_login TIMESTAMP,
            loyalty_points BIGINT,
            yearly_income DOUBLE,
            marital_status VARCHAR,
            num_children INTEGER,
            education_level VARCHAR,
            occupation VARCHAR,
            home_owner BOOLEAN
        )
        WITH (format='PARQUET')
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id BIGINT,
            name VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            brand VARCHAR,
            price DOUBLE,
            weight DOUBLE,
            manufacture_date DATE
        )
        WITH (format='PARQUET')
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            sale_id BIGINT,
            customer_id BIGINT,
            product_id BIGINT,
            sale_date DATE,
            sale_timestamp TIMESTAMP,
            quantity INTEGER,
            unit_price DOUBLE,
            discount DOUBLE,
            total_price DOUBLE,
            payment_method VARCHAR,
            store_id INTEGER,
            channel VARCHAR
        )
        WITH (format='PARQUET')
    """)

    print("✔ Tablas creadas (si no existían).")


def truncate_tables():
    cursor.execute("TRUNCATE TABLE customers")
    cursor.execute("TRUNCATE TABLE products")
    cursor.execute("TRUNCATE TABLE sales")
    print("✔ Tablas vaciadas.")


def insert_customers():
    print("Insertando customers...")

    for batch_start in range(0, NUM_CUSTOMERS, BATCH_SIZE):
        batch = []

        for _ in range(BATCH_SIZE):
            cid = random.randint(1, 10_000_000)

            birthdate = fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat()
            signup_date = fake.date_between(
                start_date="-10y", end_date="today"
            ).isoformat()
            last_login_dt = fake.date_time_between(start_date="-1y", end_date="now")
            last_login = last_login_dt.strftime("%Y-%m-%d %H:%M:%S")

            batch.append(
                (
                    cid,
                    fake.first_name(),
                    fake.last_name(),
                    random.choice(["M", "F"]),
                    birthdate,
                    fake.email(),
                    fake.phone_number(),
                    fake.address().replace("\n", " "),
                    fake.city(),
                    fake.state(),
                    fake.country(),
                    fake.postcode(),
                    signup_date,
                    last_login,
                    random.randint(0, 10000),
                    random.uniform(1000, 100000),
                    random.choice(["single", "married", "divorced"]),
                    random.randint(0, 5),
                    random.choice(["high_school", "college", "masters", "phd"]),
                    fake.job(),
                    random.choice([True, False]),
                )
            )

        values_sql = ", ".join(
            f"""(
                {r[0]},
                '{esc(r[1])}',
                '{esc(r[2])}',
                '{esc(r[3])}',
                DATE '{r[4]}',
                '{esc(r[5])}',
                '{esc(r[6])}',
                '{esc(r[7])}',
                '{esc(r[8])}',
                '{esc(r[9])}',
                '{esc(r[10])}',
                '{esc(r[11])}',
                DATE '{r[12]}',
                TIMESTAMP '{r[13]}',
                {r[14]},
                {r[15]},
                '{esc(r[16])}',
                {r[17]},
                '{esc(r[18])}',
                '{esc(r[19])}',
                {str(r[20]).lower()}
            )"""
            for r in batch
        )

        sql = f"""
        INSERT INTO customers (
            customer_id, first_name, last_name, gender, birthdate,
            email, phone, address, city, state, country, postal_code,
            signup_date, last_login, loyalty_points, yearly_income,
            marital_status, num_children, education_level, occupation, home_owner
        )
        VALUES {values_sql}
        """

        cursor.execute(sql)
        print(f"Inserted customer batch starting at {batch_start}")


def insert_products():
    print("Insertando products...")

    for batch_start in range(0, NUM_PRODUCTS, BATCH_SIZE):
        batch = []

        for _ in range(BATCH_SIZE):
            pid = random.randint(1, 1_000_000)
            batch.append(
                (
                    pid,
                    fake.word(),
                    fake.word(),
                    fake.word(),
                    fake.company(),
                    round(random.uniform(1, 1000), 2),
                    round(random.uniform(0.1, 10.0), 2),
                    fake.date_between(start_date="-3y", end_date="today").isoformat(),
                )
            )

        values_sql = ", ".join(
            f"""(
                {r[0]},
                '{esc(r[1])}',
                '{esc(r[2])}',
                '{esc(r[3])}',
                '{esc(r[4])}',
                {r[5]},
                {r[6]},
                DATE '{r[7]}'
            )"""
            for r in batch
        )

        sql = f"""
        INSERT INTO products (
            product_id, name, category, subcategory, brand,
            price, weight, manufacture_date
        )
        VALUES {values_sql}
        """

        cursor.execute(sql)
        print(f"Inserted product batch starting at {batch_start}")


def insert_sales():
    print("Insertando sales...")

    for batch_start in range(0, NUM_SALES, BATCH_SIZE):
        batch = []

        for _ in range(BATCH_SIZE):
            sid = random.randint(1, 1_000_000_000)
            cid = random.randint(1, 10_000_000)
            pid = random.randint(1, 1_000_000)

            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(5.0, 1000.0), 2)
            total = round(unit_price * quantity, 2)

            sale_date = fake.date_between(
                start_date="-1y", end_date="today"
            ).isoformat()
            sale_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            batch.append(
                (
                    sid,
                    cid,
                    pid,
                    sale_date,
                    sale_ts,
                    quantity,
                    unit_price,
                    round(random.uniform(0, 0.3), 2),
                    total,
                    random.choice(["card", "cash", "online"]),
                    random.randint(1, 50),
                    random.choice(["store", "online"]),
                )
            )

        values_sql = ", ".join(
            f"""(
                {r[0]},
                {r[1]},
                {r[2]},
                DATE '{r[3]}',
                TIMESTAMP '{r[4]}',
                {r[5]},
                {r[6]},
                {r[7]},
                {r[8]},
                '{esc(r[9])}',
                {r[10]},
                '{esc(r[11])}'
            )"""
            for r in batch
        )

        sql = f"""
        INSERT INTO sales (
            sale_id, customer_id, product_id, sale_date, sale_timestamp,
            quantity, unit_price, discount, total_price,
            payment_method, store_id, channel
        )
        VALUES {values_sql}
        """

        cursor.execute(sql)
        print(f"Inserted sales batch starting at {batch_start}")


if __name__ == "__main__":
    create_tables()  # crea tablas si no existen
    truncate_tables()  # vacía tablas antes de insertar

    insert_customers()
    insert_products()
    insert_sales()

    print("Carga completa.")
