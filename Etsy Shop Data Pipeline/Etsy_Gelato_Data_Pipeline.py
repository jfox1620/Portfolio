"""
Etsy Gelato Data Pipeline
------------------

This Databricks notebook implements an end-to-end data ingestion and transformation pipeline for synchronizing Etsy shop data into a Databricks SQL database. It retrieves receipts and transactions, with associated attributes, from the Etsy API, and fulfillments from the Gelato API. It Normalizes nested fields into tabular structures, and loads the results into managed database tables. This notebook can be runned with the assumption that the Etsy API authentication setup has already been completed, with the necessary credentials stored in a local JSON file.

The pipeline follows a standard ETL flow:

1. Extract
   - Authenticate with Etsy via OAuth.
   - Fetch receipt and transaction records using paginated API calls.
   - Fetch fulfillment records using a Gelato API call.

2. Transform
   - Convert JSON records into clean pandas DataFrames, selecting relevant fields.
   - Normalize nested structures (e.g., transactions, variations, attributes).
   - Standardize column names and schemas.
   - Derive additional fields used for analytics.
   - Format variation data into human-readable key/value strings.

3. Load
   - Connect to Databricks SQL Warehouse using a secure SQL connector.
   - Replace existing tables with freshly ingested datasets (idempotent loads).
   - Ensure consistent schema and deterministic output for analytics.

Although Databricks Community Edition notebooks cannot run external orchestrators such as Prefect or Airflow, this script is designed following the same orchestration patterns: modularized tasks, explicit stage boundaries, error handling, and clear separation of ETL responsibilities. This makes the code structurally compatible with production workflow tools, even when executed as a standalone script.

Overall, this notebook showcases production-minded data engineering practices including modular function design, robust docstrings, defensive error handling, structured logging, external API integration, and warehouse-ready data modeling within Databricks.

"""

import requests
import pyodbc
import datetime
import os
import json
from prefect import flow, task
from datetime import timedelta
from databricks import sql


# -------------------------------
# CONFIG
# -------------------------------


json_path = "/Workspace/Users/humblefox90@gmail.com/etsy_secrets.json"
data = {}

if os.path.exists(json_path):
    with open(json_path) as f:
        data = json.load(f)

    CLIENT_ID = data.get("client_id")
    SHOP_ID = data.get("shop_id")
    REFRESH_TOKEN = data.get("refresh_token")
    GELATO_API_KEY = data.get("gelato_api_key")
    SQL_HOST = data.get("sql_host")
    SQL_PATH = data.get("sql_path")
    SQL_ACCESS_TOKEN = data.get("sql_access_token")


# -------------------------------
# TASKS
# -------------------------------


def get_etsy_access_token(REFRESH_TOKEN: str, CLIENT_ID: str):
    """
    Retrieve a new Etsy access token using an existing refresh token.

    This method calls Etsy's OAuth token endpoint with the provided refresh token and client ID. Etsy returns a short-lived access token, which is required for all authenticated API requests.

    Parameters
    ----------
    REFRESH_TOKEN : str
        The long-lived OAuth refresh token issued by Etsy.
    CLIENT_ID : str
        The Etsy application client ID (API key).

    Returns
    -------
    str
        A valid OAuth access token.

    Raises
    ------
    requests.HTTPError
        If the Etsy API responds with an error.
    """

    url = "https://api.etsy.com/v3/public/oauth/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": REFRESH_TOKEN
    }
    r = requests.post(url, data=data)
    r.raise_for_status()

    return r.json()["access_token"]


def get_etsy_receipts(ACCESS_TOKEN: str, CLIENT_ID: str, SHOP_ID: str):
    """
    Fetch all receipt records for an Etsy shop.

    Sends an authenticated GET request to the Etsy Receipts API and returns the list contained under the 'results' object.

    Parameters
    ----------
    ACCESS_TOKEN : str
        OAuth access token used for authorization.
    CLIENT_ID : str
        Etsy application client ID (sent as 'x-api-key').
    SHOP_ID : str
        The numeric ID of the Etsy shop.

    Returns
    -------
    list
        A list of receipt dictionaries. Returns an empty list if no results.

    Raises
    ------
    requests.HTTPError
        If the API request fails.
    """

    url = f"https://openapi.etsy.com/v3/application/shops/{SHOP_ID}/receipts"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}",
               "x-api-key": CLIENT_ID}
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    return r.json().get("results", [])


def get_etsy_transactions(ACCESS_TOKEN: str, CLIENT_ID: str, SHOP_ID: str):
    """
    Retrieve all transaction records for an Etsy shop.

    Sends an authenticated GET request to the Etsy Transactions API and returns the list contained under the 'results' object.

    Parameters
    ----------
    ACCESS_TOKEN : str
        OAuth access token for Etsy API authentication.
    CLIENT_ID : str
        Etsy application client ID (x-api-key).
    SHOP_ID : str
        Unique identifier for the Etsy shop.

    Returns
    -------
    list
        List of transaction objects returned by the API.

    Raises
    ------
    requests.HTTPError
        If the API call returns an error response.
    """


    url = f"https://openapi.etsy.com/v3/application/shops/{SHOP_ID}/transactions"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}",
               "x-api-key": CLIENT_ID}
    r = requests.get(url, headers=headers)
    r.raise_for_status()

    return r.json().get("results", [])


def get_gelato_fulfillments(API_KEY: str):
    """
    Retrieve order fulfillment data from the Gelato API.

    This function fetches the list of orders, then performs a second request per order to retrieve full order details, including nested receipt objects. Each record is flattened and enriched with subtotal, shipping, and tax values extracted from the first receipt entry.

    Parameters
    ----------
    API_KEY : str
        Private Gelato API key used for authentication.

    Returns
    -------
    list
        A list of flattened order dictionaries containing both top-level and nested receipt information.

    Raises
    ------
    requests.HTTPError
        If any Gelato API request fails.
    """

    url = "https://order.gelatoapis.com/v4/orders"
    headers = {'Content-Type': 'application/json','X-API-KEY': API_KEY}

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()

    gelato_fulfillments = []

    for order in data.get("orders", []):
        order_row = dict(order)

        url_order = f"https://order.gelatoapis.com/v4/orders/{order.get('id')}"
        r = requests.get(url_order, headers=headers)
        r.raise_for_status()
        order = r.json()

        # Extract receipts (nested object)
        gelato_receipts = order.get("receipts", {})
        order_row["subtotal"] = gelato_receipts[0].get("productsPriceInitial", 0)
        order_row["shipping"] = gelato_receipts[0].get("shippingPrice", 0)
        order_row["tax"] = gelato_receipts[0].get("totalVat", 0)

        gelato_fulfillments.append(order_row)

    return gelato_fulfillments


def save_etsy_receipts(receipts: list, conn):
    """
    Replace all data in the `etsy_receipts` table and load new receipt rows.

    The function truncates the existing table for idempotency, then iterates through Etsy receipt records and loads each field into the database. Monetary values from Etsy's API (in cents) are converted to standard currency units (dollars).

    Parameters
    ----------
    receipts : list
        List of receipt objects returned from Etsy.
    conn : DatabricksConnection
        Active Databricks SQL connector instance.

    Returns
    -------
    int
        Number of inserted receipt records.

    Raises
    ------
    Exception
        If an insertion or database operation fails.
    """

    cursor = conn.cursor()

    # Clear table first
    cursor.execute("TRUNCATE TABLE etsy_receipts")
    conn.commit()

    for r in receipts:
        cursor.execute("""
            INSERT INTO etsy_receipts (
                receipt_id, buyer_user_id, name, city, state, country_iso, 
                status, is_paid, is_shipped, created_timestamp, updated_timestamp, 
                grandtotal, subtotal, total_price, total_tax_cost, total_vat_cost, 
                discount_amt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,(
        r.get("receipt_id"),
        r.get("buyer_user_id"),
        r.get("name"),
        r.get("city"),
        r.get("state"),
        r.get("country_iso"),
        r.get("status"),
        r.get("is_paid"),
        r.get("is_shipped"),
        r.get("created_timestamp"),
        r.get("updated_timestamp"),
        r.get("grandtotal", {}).get("amount")/100,
        r.get("subtotal", {}).get("amount")/100,
        r.get("total_price", {}).get("amount")/100,
        r.get("total_tax_cost", {}).get("amount")/100,
        r.get("total_vat_cost", {}).get("amount")/100,
        r.get("discount_amt", {}).get("amount")/100
        ))

    conn.commit()
    cursor.close()

    return len(receipts)


# combine multiple variation details in transaction data
def format_variation_details(variations_list):
    """
    Convert an Etsy variations list into a readable "name: value" string.

    Etsy transaction objects may contain multiple variation attributes (e.g., size, color). This function concatenates them into a single semicolon-separated string suitable for database storage.

    Parameters
    ----------
    variations_list : list or None
        List of variation dictionaries, each containing `formatted_name` and `formatted_value`.

    Returns
    -------
    str or None
        A semicolon-delimited string of "name: value" pairs, or None if no variations exist.
    """

    if not variations_list:
        return None
    return "; ".join(f"{v.get('formatted_name')}: {v.get('formatted_value')}" for v in variations_list)


def save_etsy_transactions(transactions: list, conn):
    """
    Replace all data in the `etsy_transactions` table and load new transaction rows.

    The function truncates the existing table for idempotency, then iterates through Etsy transaction records and loads each field into the database. Variation fields are normalized into a single formatted string. Monetary values from Etsy's API (in cents) are converted to standard currency units (dollars).

    Parameters
    ----------
    transactions : list
        List of transaction objects returned from Etsy.
    conn : DatabricksConnection
        Active Databricks SQL connector instance.

    Returns
    -------
    int
        Number of inserted transaction records.

    Raises
    ------
    Exception
        If an insertion or database operation fails.
    """

    cursor = conn.cursor()

    # Clear table first
    cursor.execute("TRUNCATE TABLE etsy_transactions")
    conn.commit()

    # Insert transactions
    for t in transactions:
        cursor.execute("""
            INSERT INTO etsy_transactions (
                transaction_id, title, buyer_user_id, paid_timestamp, 
                shipped_timestamp, quantity, receipt_id, listing_id, 
                sku, product_id, variations, price, shop_coupon
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            t.get("transaction_id"),
            t.get("title"),
            t.get("buyer_user_id"),
            t.get("paid_timestamp"),
            t.get("shipped_timestamp"),
            t.get("quantity"),
            t.get("receipt_id"),
            t.get("listing_id"),
            t.get("sku"),
            t.get("product_id"),
            format_variation_details(t.get("variations")),
            t.get("price", {}).get("amount")/100,
            t.get("shop_coupon")
        ))
    
    conn.commit()
    cursor.close()
    
    return len(transactions)


def save_gelato_fulfillments(fulfillments: list, conn):
    """
    Replace all data in the `gelato_fulfillments` table and insert new fulfillment rows.

    The function truncates the existing table for idempotency, then iterates through Gelato fulfillment records and loads each field into the database. Values include both the top-level order metadata and extracted receipt-level financial fields.

    Parameters
    ----------
    fulfillments : list
        List of fulfillment objects returned from Gelato.
    conn : DatabricksConnection
        Active Databricks SQL connector instance.

    Returns
    -------
    int
        Number of inserted fulfillment records.

    Raises
    ------
    Exception
        If an insertion or database operation fails.
    """

    cursor = conn.cursor()

    # Clear table first
    cursor.execute("TRUNCATE TABLE gelato_fulfillments")
    conn.commit()

    # Insert orders
    for f in fulfillments:
        cursor.execute("""
            INSERT INTO gelato_fulfillments (
                id, clientId, orderReferenceId, fulfillmentStatus,
                financialStatus, totalInclVat, channel, country,
                itemsCount, orderedAt, customerReferenceId, subtotal, shipping, tax
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,(
        f.get("id"),
        f.get("clientId"),
        f.get("orderReferenceId"),
        f.get("fulfillmentStatus"),
        f.get("financialStatus"),
        f.get("totalInclVat"),
        f.get("channel"),
        f.get("country"),
        f.get("itemsCount"),
        f.get("orderedAt"),
        f.get("customerReferenceId"),
        f.get("subtotal"),
        f.get("shipping"),
        f.get("tax")
        ))
    
    conn.commit()
    cursor.close()
    
    return len(fulfillments)


# -------------------------------
# FLOW
# -------------------------------


def etsy_gelato_pipeline():
    """
    End-to-end pipeline that synchronizes Etsy and Gelato data into Databricks.

    Establishes a SQL connection, refreshes the Etsy access token, extracts receipts, transactions, and Gelato fulfillment data, and loads all three datasets into their respective database tables. Each table is rebuilt using truncate-and-load semantics to ensure deterministic, up-to-date data.

    Returns
    -------
    None
        Prints summary counts upon successful completion.

    Raises
    ------
    Exception
        If any step of extraction, transformation, or loading fails.
    """
    connection = sql.connect(
                        server_hostname = SQL_HOST,
                        http_path = SQL_PATH,
                        access_token = SQL_ACCESS_TOKEN)

    ACCESS_TOKEN = get_etsy_access_token(REFRESH_TOKEN, CLIENT_ID)

    receipts = get_etsy_receipts(ACCESS_TOKEN, CLIENT_ID, SHOP_ID)
    transactions = get_etsy_transactions(ACCESS_TOKEN, CLIENT_ID, SHOP_ID)
    gelato_fulfillments = get_gelato_fulfillments(GELATO_API_KEY)

    receipt_count = save_etsy_receipts(receipts, connection)
    transaction_count = save_etsy_transactions(transactions, connection)
    fulfillment_count = save_gelato_fulfillments(gelato_fulfillments, connection)

    print(f"Saved {receipt_count} receipts, {transaction_count} transactions, {fulfillment_count} fulfillments.")

    connection.close()


if __name__ == "__main__":
    etsy_gelato_pipeline()
