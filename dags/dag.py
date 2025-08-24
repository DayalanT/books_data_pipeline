from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2

# -------------------------
# Extract: Scrape books
# -------------------------
def get_books_data(num_books, ti):
    base_url = "http://books.toscrape.com/catalogue/page-{}.html"
    books, page = [], 1

    while len(books) < num_books:
        url = base_url.format(page)
        response = requests.get(url)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {url}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        book_containers = soup.select(".product_pod")

        if not book_containers:
            print("⚠️ No more books found")
            break

        for container in book_containers:
            title = container.h3.a["title"]
            price = container.select_one(".price_color").text.strip()
            rating = container.p["class"][1]  # "One", "Two", etc.

            books.append({
                "title": title,
                "price": price,
                "rating": rating
            })

            if len(books) >= num_books:
                break

        page += 1

    df = pd.DataFrame(books)
    print(f"✅ Scraped {len(df)} books")
    ti.xcom_push(key="raw_book_data", value=df.to_dict("records"))

# -------------------------
# Transform: Clean data
# -------------------------
def transform_books_data(ti):
    records = ti.xcom_pull(task_ids="fetch_book_data", key="raw_book_data")
    if not records:
        print("⚠️ No data to transform")
        return

    df = pd.DataFrame(records)

    # Clean price → float
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace(r"[^\d.]", "", regex=True)
        .replace("", "0")
        .astype(float)
    )

    # Normalize rating
    rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    df["rating"] = df["rating"].map(rating_map)

    # Clean title
    df["title"] = df["title"].str.strip()

    print("✅ Cleaned sample:")
    print(df.head())

    ti.xcom_push(key="cleaned_book_data", value=df.to_dict("records"))

# -------------------------
# Load: Save to Postgres
# -------------------------
def save_to_postgres(ti):
    records = ti.xcom_pull(task_ids="transform_books_data", key="cleaned_book_data")
    if not records:
        print("⚠️ No data to load")
        return

    conn = psycopg2.connect(
        host="postgres",     # container name from docker-compose
        database="books",    # matches POSTGRES_DB
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Create table if not exists (history mode: no unique constraint)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            price NUMERIC,
            rating INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Insert every record (allow duplicates for history)
    for r in records:
        cur.execute(
            """
            INSERT INTO books (title, price, rating)
            VALUES (%s, %s, %s);
            """,
            (r["title"], r["price"], r["rating"])
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(records)} records into Postgres")

# -------------------------
# Airflow DAG
# -------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 20),  # explicit fixed start_date
    "retries": 0,
}

with DAG(
    dag_id="books_pipeline_dag",
    default_args=default_args,
    schedule=None,      # 👈 manual trigger only
    catchup=False,
    tags=["etl", "books"],
) as dag:

    fetch_book_data = PythonOperator(
        task_id="fetch_book_data",
        python_callable=get_books_data,
        op_kwargs={"num_books": 50},
    )

    transform_books = PythonOperator(
        task_id="transform_books_data",
        python_callable=transform_books_data,
    )

    save_books_data = PythonOperator(
        task_id="save_books_data",
        python_callable=save_to_postgres,
    )

    fetch_book_data >> transform_books >> save_books_data

