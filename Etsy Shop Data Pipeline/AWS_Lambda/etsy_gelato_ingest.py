"""
Etsy–Gelato ELT Raw Ingestion Pipeline (AWS Lambda)
--------------------------------------------------

This Lambda function implements the **extract and load (EL)** portion of the
Etsy–Gelato ELT pipeline for True Form Designs. It incrementally pulls data from the Etsy and
Gelato APIs, stores immutable raw JSON payloads in Amazon S3, and relies on
downstream Snowflake + Snowpipe + dbt processes for ingestion and transformation.

The Lambda function is intentionally lightweight and stateless, serving only
as the data extraction and raw landing layer.


Pipeline Responsibilities
-------------------------
1. Extract
   - Refresh Etsy OAuth access token using a stored refresh token.
   - Incrementally fetch:
     - Etsy receipts
     - Etsy transactions
     - Gelato order fulfillments
   - API requests are filtered using a persisted `last_successful_run` timestamp
     stored in AWS SSM Parameter Store.

2. Load (Raw / Bronze)
   - Persist raw API responses to Amazon S3 as JSON.
   - Data is written as immutable, append-only files.
   - Files are partitioned by source type (e.g., 'etsy_receipts', 'gelato_fulfillments')
     and ingestion date (year/month/day).
   - Each file contains:
     - extraction timestamp
     - record count
     - full raw payload

3. State Management
   - On successful completion, the pipeline updates the
     `etsy_gelato_last_successful_run` parameter in AWS SSM.
   - This ensures incremental loads with no gaps or duplication.

   
Out of Scope (Handled Downstream)
---------------------------------
- Snowflake ingestion
- Schema enforcement
- JSON parsing and flattening
- Data modeling
- Analytics and reporting


Design Principles
-----------------
- Raw data is treated as a system of record.
- No transformations occur in Lambda.
- Reprocessing and backfills are enabled via immutable S3 storage.
- Lambda remains fast, simple, and cost-efficient.

"""


def lambda_handler(event, context):

    try:
        etsy_gelato_pipeline()
        return {"statusCode": 200, "body": "Success"}
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return {"statusCode": 500, "body": f"Error: {e}"}


import requests
import datetime
import os
import boto3
import json
from datetime import timedelta, datetime, timezone


# -------------------------------
# FUNCTIONS
# -------------------------------

def load_secrets(parameter_names: list, region_name: str = "us-west-1", ssm_client=None) -> dict:
    """
    Load a list of secrets or parameters from AWS SSM Parameter Store.

    Parameters
    ----------
    parameter_names : list of str
        Names of the SSM parameters to retrieve.
    region_name : str, optional
        AWS region where the parameters are stored. Default is 'us-west-1'.
    ssm_client : boto3 SSM client, optional
        If provided, this SSM client is used instead of creating a new one.

    Returns
    -------
    dict
        Dictionary mapping parameter names to their decrypted values.

    Raises
    ------
    boto3.exceptions.Boto3Error
        If an error occurs when fetching parameters.
    KeyError
        If a parameter is missing in SSM.
    """
    
    # Use provided client or create a new one
    ssm = ssm_client or boto3.client('ssm', region_name=region_name)

    secrets = {}

    for name in parameter_names:
        try:
            response = ssm.get_parameter(Name=name, WithDecryption=True)
            secrets[name] = response['Parameter']['Value']
        except ssm.exceptions.ParameterNotFound:
            raise KeyError(f"SSM parameter '{name}' not found.")
        except Exception as e:
            # catch other boto3 errors
            raise RuntimeError(f"Error loading SSM parameter '{name}': {e}") from e

    return secrets


def get_etsy_access_token(REFRESH_TOKEN: str, CLIENT_ID: str):
    """
    Retrieve a new Etsy access token using an existing refresh token.

    This method calls Etsy's OAuth token endpoint with the provided refresh token and client ID. 
    Etsy returns a short-lived access token, which is required for all authenticated API requests.

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
    requests.exceptions.HTTPError
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


def extract_etsy_receipts(ACCESS_TOKEN: str, CLIENT_ID: str, CLIENT_SECRET: str, SHOP_ID: str, since: str = None):
    """
    Retrieve receipt records for a specific Etsy shop via the Etsy Open API v3.

    This function sends an authenticated GET request to the Etsy API to fetch
    receipt (order) data for the provided shop. Results may optionally be
    filtered to only include receipts updated or created after a given
    timestamp.

    Parameters
    ----------
    ACCESS_TOKEN : str
        OAuth2 bearer token used to authenticate the request to the Etsy API.
        Must be a valid, non-expired access token with permission to read shop receipts.
    CLIENT_ID : str
        Etsy application client ID (API key). Used as part of the
        `x-api-key` header for application identification.
    CLIENT_SECRET : str
        Etsy application client secret. Combined with CLIENT_ID in the
        `x-api-key` header for authenticated API access.
    SHOP_ID : str
        The unique identifier of the Etsy shop from which receipts
        will be retrieved.
    since : str, optional
        ISO 8601 formatted timestamp (e.g., "2025-01-01T00:00:00Z").
        When provided, only receipts with `last_modified` greater than
        or equal to this timestamp will be returned. If None, all
        available receipts are returned (subject to API defaults and
        pagination limits).

    Returns
    -------
    list
        A list of receipt objects (dicts) as returned by the Etsy API.
        If no receipts are found, an empty list is returned.

    Raises
    ------
    requests.exceptions.HTTPError
        If the HTTP request returns an unsuccessful status code.
    """

    url = f"https://openapi.etsy.com/v3/application/shops/{SHOP_ID}/receipts"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "x-api-key": f"{CLIENT_ID}:{CLIENT_SECRET}"
    }

    params = {}
    if since:
        params["min_last_modified"] = since  # ISO 8601 string

    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()

    return r.json().get("results", [])


def extract_etsy_transactions(ACCESS_TOKEN: str, CLIENT_ID: str, CLIENT_SECRET: str, SHOP_ID: str, since: str = None):
    """
    Retrieve transaction records for a specific Etsy shop via the Etsy Open API v3.

    This function sends an authenticated GET request to the Etsy API to fetch
    transaction (line item) data for the provided shop. Results may optionally be
    filtered to only include receipts updated or created after a given
    timestamp.

    Parameters
    ----------
    ACCESS_TOKEN : str
        OAuth2 bearer token used to authenticate the request to the Etsy API.
        Must be a valid, non-expired access token with permission to read shop receipts.
    CLIENT_ID : str
        Etsy application client ID (API key). Used as part of the
        `x-api-key` header for application identification.
    CLIENT_SECRET : str
        Etsy application client secret. Combined with CLIENT_ID in the
        `x-api-key` header for authenticated API access.
    SHOP_ID : str
        The unique identifier of the Etsy shop from which receipts
        will be retrieved.
    since : str, optional
        ISO 8601 formatted timestamp (e.g., "2025-01-01T00:00:00Z").
        When provided, only receipts with `last_modified` greater than
        or equal to this timestamp will be returned. If None, all
        available receipts are returned (subject to API defaults and
        pagination limits).

    Returns
    -------
    list
        A list of receipt objects (dicts) as returned by the Etsy API.
        If no receipts are found, an empty list is returned.

    Raises
    ------
    requests.exceptions.HTTPError
        If the HTTP request returns an unsuccessful status code.
    """

    url = f"https://openapi.etsy.com/v3/application/shops/{SHOP_ID}/transactions"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "x-api-key": f"{CLIENT_ID}:{CLIENT_SECRET}"
    }

    params = {}
    if since:
        params["min_last_modified"] = since  # ISO 8601 string

    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()

    return r.json().get("results", [])


def extract_gelato_fulfillments(API_KEY: str, since: str = None):
    """
    Retrieve order fulfillment data from the Gelato Orders API (v4).

    This function fetches a list of orders from Gelato and, for each order,
    performs an additional API request to retrieve detailed order information.
    Selected receipt-level pricing fields are extracted and flattened into
    the top-level order dictionary.

    Parameters
    ----------
    API_KEY : str
        Private Gelato API key for authentication.
        Must be a valid, non-expired access token with permission to read orders.
    since : str, optional
        ISO 8601 formatted timestamp (e.g., "2025-01-01T00:00:00Z").
        When provided, only receipts with `last_modified` greater than
        or equal to this timestamp will be returned. If None, all
        available receipts are returned (subject to API defaults and
        pagination limits).

    Returns
    -------
    list
        A list of flattened order dictionaries containing top-level and nested receipt information.

    Raises
    ------
    requests.exceptions.HTTPError
        If the HTTP request returns an unsuccessful status code.
    """

    url = "https://order.gelatoapis.com/v4/orders"
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': API_KEY
    }

    params = {}
    if since:
        params["updatedAfter"] = since  # ISO 8601 string

    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    data = r.json()

    gelato_fulfillments = []

    for order in data.get("orders", []):
        order_row = dict(order)

        url_order = f"https://order.gelatoapis.com/v4/orders/{order.get('id')}"
        r_order = requests.get(url_order, headers=headers, timeout=60)
        r_order.raise_for_status()
        order_detail = r_order.json()

        # Extract receipts (nested object)
        gelato_receipts = order_detail.get("receipts", {})
        if gelato_receipts:
            order_row["subtotal"] = gelato_receipts[0].get("productsPriceInitial", 0)
            order_row["shipping"] = gelato_receipts[0].get("shippingPrice", 0)
            order_row["tax"] = gelato_receipts[0].get("totalVat", 0)

        gelato_fulfillments.append(order_row)

    return gelato_fulfillments


def store_raw_to_s3(raw_data: list, bucket: str, entity: str) -> int:
    """
    Persist raw API data to Amazon S3 as immutable, time-partitioned JSON.

    This function represents the RAW / BRONZE layer of the pipeline and serves
    as the system of record for reprocessing, backfills, and auditing.

    S3 layout:
        s3://{bucket}/{entity}/year=YYYY/month=MM/day=DD/{entity}_timestamp.json

    Parameters
    ----------
    raw_data : list
        List of raw records (dictionaries) returned from the API.
    bucket : str
        Name of the S3 bucket used for raw data storage.
    entity : str
        Entity name (e.g., 'etsy_receipts', 'gelato_fulfillments').

    Returns
    -------
    int
        Number of records written.
    """

    now = datetime.now(timezone.utc)

    s3_key = (
        f"{entity}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{entity}_{now.strftime('%Y%m%dT%H%M%S')}Z.json"
    )

    payload = {
        "extracted_at": now.isoformat(),
        "record_count": len(raw_data),
        "data": raw_data
    }

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json"
    )

    return len(raw_data)


def etsy_gelato_pipeline():
    """

    This function implements a serverless ELT pipeline for Etsy and Gelato APIs. It is designed
    to run in AWS Lambda, extracting only new or updated records since the last successful run,
    storing raw JSON payloads in Amazon S3, and enabling downstream transformations.

    Workflow:
    1. Load secrets from SSM Parameter Store
    2. Refresh Etsy OAuth access token.
    3. Extract Etsy receipts, Etsy transactions, and Gelato fulfillments.
    - Only retrieves records updated since the last run timestamp stored in SSM Parameter Store.
    - API errors are caught and logged; failed extracts result in empty datasets.
    4. Store raw JSON in Amazon S3.
    - Each dataset is partitioned by entity type and year/month/day, and are timestamped for immutability.
    - S3 write failures are logged; datasets that fail are not counted toward successful run.
    5. Update the last successful run timestamp in SSM Parameter Store.
    - Only updated if all API calls and S3 writes succeed.

    Error Handling:
    - Individual API failures do not crash the Lambda; they log errors and continue.
    - S3 write errors are logged and prevent updating the last successful run timestamp.
    - Critical failures (like failing to refresh the Etsy access token) raise an exception
      and halt the pipeline.

    Returns:
    -------
    None
        Prints summary counts of records ingested per dataset.
    """
    
    ERRORS = False

    # S3 client and bucket
    s3 = boto3.client('s3', region_name='us-west-1')
    bucket_name = 'etsy-gelato-json-raw'

    # SSM client
    ssm = boto3.client('ssm', region_name='us-west-1')
    

    # ---------------------------
    # 1. Load secrets from SSM
    # ---------------------------

    parameter_names = [
        'etsy_gelato_last_successful_run',
        'etsy_client_id',
        'etsy_client_secret',
        'gelato_api_key',
        'etsy_refresh_token',
        'etsy_shop_id'
    ]

    params = load_secrets(parameter_names, ssm_client=ssm)

    LAST_RUN_TIME = params['etsy_gelato_last_successful_run']
    CLIENT_ID = params['etsy_client_id']
    CLIENT_SECRET = params['etsy_client_secret']
    GELATO_API_KEY = params['gelato_api_key']
    REFRESH_TOKEN = params['etsy_refresh_token']
    SHOP_ID = params['etsy_shop_id']

    # ---------------------------
    # 2. Refresh Etsy access token
    # ---------------------------

    try:
        ACCESS_TOKEN = get_etsy_access_token(REFRESH_TOKEN, CLIENT_ID)
    except requests.HTTPError as e:
        print(f"Error refreshing Etsy access token: {e}")
        ERRORS = True
        raise

    # ---------------------------
    # 3. Extract from API
    # ---------------------------

    # Get timestamp of API pull
    ingest_timestamp = datetime.now(timezone.utc).isoformat()

    try:
        receipts_raw = extract_etsy_receipts(ACCESS_TOKEN, CLIENT_ID, CLIENT_SECRET, SHOP_ID, LAST_RUN_TIME)
    except requests.HTTPError as e:
        print(f"Error fetching Etsy receipts: {e}")
        ERRORS = True
        receipts_raw = []

    try:
        transactions_raw = extract_etsy_transactions(ACCESS_TOKEN, CLIENT_ID, CLIENT_SECRET, SHOP_ID, LAST_RUN_TIME)
    except requests.HTTPError as e:
        print(f"Error fetching Etsy transactions: {e}")
        ERRORS = True
        transactions_raw = []

    try:
        fulfillments_raw = extract_gelato_fulfillments(GELATO_API_KEY, LAST_RUN_TIME)
    except requests.HTTPError as e:
        print(f"Error fetching Gelato fulfillments: {e}")
        ERRORS = True
        fulfillments_raw = []

    # ---------------------------
    # 4. Store in S3
    # ---------------------------

    if receipts_raw:
        try:
            receipt_count = store_raw_to_s3(
                receipts_raw,
                bucket=bucket_name,
                entity="etsy_receipts"
            )
        except Exception as e:
            print(f"Failed to write Etsy receipts to S3: {e}")
            ERRORS = True

    if transactions_raw:
        try:
            transaction_count = store_raw_to_s3(
                transactions_raw,
                bucket=bucket_name,
                entity="etsy_transactions"
            )
        except Exception as e:
            print(f"Failed to write Etsy transactions to S3: {e}")
            ERRORS = True

    if fulfillments_raw:
        try:
            fulfillment_count = store_raw_to_s3(
                fulfillments_raw,
                bucket=bucket_name,
                entity="gelato_fulfillments"
            )
        except Exception as e:
            print(f"Failed to write Gelato fulfillments to S3: {e}")
            ERRORS = True

    # ---------------------------
    # 5. Print results and update metadata.
    # ---------------------------
    
    if not ERRORS:
        print(f"Ingested {receipt_count} Etsy receipts, {transaction_count} Etsy transactions, {fulfillment_count} Gelato fulfillments.")

        # Update the run time parameter.
        try:
            ssm.put_parameter(
                Name="etsy_gelato_last_successful_run",
                Value=ingest_timestamp,
                Type="String",
                Overwrite=True
            )
        except Exception as e:

            print(f"Failed to update last successful run in SSM: {e}")
