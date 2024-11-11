Here’s the updated **README.md** with a demonstration of how to interact with the `/get` and `/pqt` endpoints using Python's `requests` library:

---

# FastAPI Database & Parquet Data Management

This project allows you to interact with a PostgreSQL database and Parquet files via FastAPI endpoints. It provides functionality for reading data from both the database and parquet files.

## Features
- **Database Queries**: Allows querying data from a PostgreSQL database.
- **Parquet File Management**: Read parquet files.
- **Insert/Update/Delete**: Insert data into the database or parquet files (explore the functionality as needed).

## Requirements
- **FastAPI**: Web framework used to build the API.
- **Polars**: For reading/writing parquet files.
- **Asyncpg**: Database connector for PostgreSQL.
  
Make sure you have all the dependencies installed:
```bash
pip install fastapi polars asyncpg pydantic requests
```

## Endpoints

### 1. **GET /get**
Fetch data from the PostgreSQL database using an SQL query.

- **URL**: `/get`
- **Method**: `GET`
- **Query Parameters**:
  - `queries` (required): SQL query to execute (e.g., `SELECT * FROM table_name`).
  - `params` (optional): A parameter for the SQL query, if required.

**Example Request**:
```bash
GET /get?queries=SELECT%20*%20FROM%20users
```

**Response Example**:
```json
{
  "status": "success",
  "timestamp": "2024-11-11T12:34:56",
  "data": [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
  ],
  "total_records": 2
}
```

### 2. **GET /pqt**
Fetch data from a Parquet file using SQL-like queries.

- **URL**: `/pqt`
- **Method**: `GET`
- **Query Parameters**:
  - `queries` (optional): SQL-like query for querying the parquet data (e.g., `SELECT * FROM df`).

**Example Request**:
```bash
GET /pqt?queries=SELECT%20*%20FROM%20df
```

**Response Example**:
```json
{
  "status": "success",
  "timestamp": "2024-11-11T12:34:56",
  "data": [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
  ],
  "total_records": 2
}
```

## Fetching Data Using `requests` Library

Here’s how you can interact with the `/get` and `/pqt` endpoints using the `requests` library in Python.

### Install `requests`
```bash
pip install requests
```

### Example: Fetching Data from the Database (`/get`)

```python
import requests

# Define the FastAPI server URL
url = "http://localhost:8000/get"

# Define the SQL query to fetch data
params = {"queries": "SELECT * FROM users"}

# Make the GET request to fetch data
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Status: {data['status']}")
    print(f"Timestamp: {data['timestamp']}")
    print(f"Data: {data['data']}")
    print(f"Total Records: {data['total_records']}")
else:
    print(f"Failed to fetch data: {response.status_code}")
```

### Example: Fetching Data from the Parquet File (`/pqt`)

```python
import requests

# Define the FastAPI server URL
url = "http://localhost:8000/pqt"

# Define the SQL query to fetch data
params = {"queries": "SELECT * FROM df"}

# Make the GET request to fetch parquet data
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Status: {data['status']}")
    print(f"Timestamp: {data['timestamp']}")
    print(f"Data: {data['data']}")
    print(f"Total Records: {data['total_records']}")
else:
    print(f"Failed to fetch parquet data: {response.status_code}")
```

### Response Example
Both of these requests will return a response similar to this:
```json
{
  "status": "success",
  "timestamp": "2024-11-11T12:34:56",
  "data": [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
  ],
  "total_records": 2
}
```

## Endpoints

### 1. **GET /get**
Fetch data from the PostgreSQL database using an SQL query.

- **URL**: `/get`
- **Method**: `GET`
- **Query Parameters**:
  - `queries` (required): SQL query to execute (e.g., `SELECT * FROM table_name`).
  - `params` (optional): A parameter for the SQL query, if required.

**Example Request**:
```bash
GET /get?queries=SELECT%20*%20FROM%20users
```

**Response Example**:
```json
{
  "status": "success",
  "timestamp": "2024-11-11T12:34:56",
  "data": [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
  ],
  "total_records": 2
}
```

### 2. **GET /pqt**
Fetch data from a Parquet file using SQL-like queries.

- **URL**: `/pqt`
- **Method**: `GET`
- **Query Parameters**:
  - `queries` (optional): SQL-like query for querying the parquet data (e.g., `SELECT * FROM df`).

**Example Request**:
```bash
GET /pqt?queries=SELECT%20*%20FROM%20df
```

**Response Example**:
```json
{
  "status": "success",
  "timestamp": "2024-11-11T12:34:56",
  "data": [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
  ],
  "total_records": 2
}
```

## Fetching Data Using `requests` Library

Here’s how you can interact with the `/get` and `/pqt` endpoints using the `requests` library in Python.

### Install `requests`
```bash
pip install requests
```

### Example: Fetching Data from the Database (`/get`)

```python
import requests

# Define the FastAPI server URL
url = "http://127.0.0.1:8000/get"

# Define the SQL query to fetch data
params = {"queries": "SELECT * FROM users"}

# Make the GET request to fetch data
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Status: {data['status']}")
    print(f"Timestamp: {data['timestamp']}")
    print(f"Data: {data['data']}")
    print(f"Total Records: {data['total_records']}")
else:
    print(f"Failed to fetch data: {response.status_code}")
```

### Example: Fetching Data from the Parquet File (`/pqt`)

```python
import requests

# Define the FastAPI server URL
url = "http://127.0.0.1:8000/pqt"

# Define the SQL query to fetch data
params = {"queries": "SELECT * FROM df"}

# Make the GET request to fetch parquet data
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print(f"Status: {data['status']}")
    print(f"Timestamp: {data['timestamp']}")
    print(f"Data: {data['data']}")
    print(f"Total Records: {data['total_records']}")
else:
    print(f"Failed to fetch parquet data: {response.status_code}")
```

### Response Example
Both of these requests will return a response similar to this:
```json
{
  "status": "success",
  "timestamp": "2024-11-11T12:34:56",
  "data": [
    {"id": 1, "name": "Item 1"},
    {"id": 2, "name": "Item 2"}
  ],
  "total_records": 2
}
```

## Setup and Run locally

1. Clone the repository:
```bash
git clone https://github.com/alifnurhadi/Database_API.git
cd Database_API
```

2. Install the dependencies:
```bash
pip install -r requirements.txt #( if not exist just put fastapi uvicorn pydantic asyncpg polars )
```
## Docker Support
This application can be containerized using Docker. You can build and run the app with Docker as follows:

used docker for db purposes only and it need to start first before run the uvicorn


### Run the Docker Container:
```bash
docker compose -f databases.yaml up -d
```

3. Run the FastAPI app:
```bash
uvicorn db:app --reload
```

The app will be available at `http://localhost:8000`.

---

This should now include all relevant information for making requests via `requests` and performing basic fetch operations on your FastAPI endpoints. Let me know if you'd like to add anything else!
cd Database_API
```
