
# Hudi Bootstrap Application

This project consists of a **FastAPI** backend and a **React** frontend. The FastAPI backend manages data processing and storage in a **PostgreSQL** database, while the React frontend provides a user-friendly interface for interaction with the backend services.

## Overview

The application allows users to bootstrap Hudi tables using data from various file formats (Parquet and ORC), track the history of bootstrap transactions, and check the status of ongoing transactions. It also includes features to validate whether a given path is an HDFS directory or Hive table. Additionally, the backend exposes a **WebSocket** endpoint to stream real-time progress updates for bootstrap transactions.

## Directory Structure

```
.
├── fastapi-backend
│   ├── app.py                # FastAPI application
│   └── pyspark_script.py     # PySpark script for data processing     
└── react-frontend
    ├── src                  # React application source files
    │   ├── App.js           # Main application component
    │   ├── BootstrapPage.js  # Component for bootstrapping Hudi tables
    │   ├── HistoryTable.js   # Component for displaying bootstrap history
    │   └── theme.js          # Custom Material-UI theme
    ├── package.json         # NPM dependencies
    └── public               # Static files
```

## Prerequisites

- **Python 3.7 or higher**
- **Node.js and npm**
- **PostgreSQL**
- **Apache Spark** (for running the PySpark script)
- **Hudi** (Hadoop Upserts Deletes and Incrementals)
- **HDFS** (Hadoop Distributed File System)
- **Hive** (for querying tables)

## Setup Instructions

### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/SunilKumar005/Hudi_utility.git
cd Hudi_utility
```

### 2. Run the Setup Script

Run the `setup.sh` script to set up the PostgreSQL database, install Python libraries, and install NPM packages for the React frontend:

```bash
chmod +x setup.sh  # Make the script executable
./setup.sh         # Run the setup script
```

### 3. Manually Create the Database (if needed)

If the setup script fails to create the database, you can do it manually:

```bash
sudo -u postgres psql
CREATE DATABASE hudi_bootstrap_db;
CREATE USER hudi_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE hudi_bootstrap_db TO hudi_user;
```

### 4. Start the FastAPI Server

Run the FastAPI application:

```bash
cd fastapi-backend
uvicorn app:app --reload
```

The FastAPI application should now be running at `http://127.0.0.1:8000`.

### 5. Start the React Frontend

Open another terminal window or tab, navigate to the React frontend directory, and start the React application:

```bash
cd react-frontend
npm start
```

The React application should now be running at `http://localhost:3000`.

## Usage

### FastAPI Endpoints

#### **POST** `/bootstrap_hudi/`
- **Description**: Bootstrap a Hudi table using specified configurations.
- **Request Body**:
    ```json
    {
      "data_file_path": "string",
      "hudi_table_name": "string",
      "key_field": "string",
      "precombine_field": "string",
      "partition_field": "string",
      "hudi_table_type": "string",  // COPY_ON_WRITE or MERGE_ON_READ
      "write_operation": "string",  // insert or upsert
      "output_path": "string",
      "spark_config": {"key": "value"}, // Optional Spark configurations
      "bootstrap_type": "string",  // FULL_RECORD or METADATA_ONLY
      "partition_regex": "string"  // Optional regex for partitions
    }
    ```

- **Response**:
    ```json
    {
      "transaction_id": "string",
      "message": "Bootstrapping started."
    }
    ```

- **Description**: This endpoint allows the user to initiate a bootstrap operation for a Hudi table. The operation runs a Spark job in the background, which is executed via the `spark-submit` command. The user receives a `transaction_id` for tracking the operation.

#### **GET** `/bootstrap_history/`
- **Description**: Retrieve the history of bootstrap transactions.
- **Query Parameters**:
    - `start_date`: Optional ISO format date string to filter transactions.
    - `end_date`: Optional ISO format date string to filter transactions.
    - `transaction_id`: Optional transaction ID to filter specific transactions.

- **Response**:
    ```json
    [
      {
        "transaction_id": "string",
        "status": "string",
        "start_time": "string",
        "end_time": "string",
        "error_log": "string"
      }
    ]
    ```

- **Description**: This endpoint returns a list of bootstrap transactions, filtered by the provided query parameters. It shows details such as transaction status, start and end time, and error logs if any.

#### **GET** `/bootstrap_status/{transaction_id}/`
- **Description**: Retrieve the status of a specific bootstrap transaction.
- **Path Parameter**:
    - `transaction_id`: The ID of the transaction to check the status for.

- **Response**:
    ```json
    {
      "status": "string",
      "error_log": "string",
      "error_message": "string",
      "record_counts": {
        "input_count": int,
        "hudi_count": int
      }
    }
    ```

- **Description**: This endpoint provides detailed status information for a specific transaction, including any errors that occurred, as well as input and Hudi table record counts extracted from the error log.

#### **POST** `/check_path_or_table/`
- **Description**: Check whether the provided path is an HDFS path or Hive table.
- **Request Body**:
    ```json
    {
      "hudi_table_name": "string"
    }
    ```

- **Response**:
    ```json
    {
      "isPartitioned": true,
      "tableName": "string",
      "hdfsLocation": "string"
    }
    ```

- **Description**: This endpoint validates whether the provided input is an HDFS path or a Hive table. If it's an HDFS path, it checks whether it's partitioned. If it's a Hive table, it returns the corresponding HDFS location and partition information.

#### **WebSocket** `/ws/`
- **Description**: This WebSocket endpoint allows users to subscribe to real-time progress updates for a bootstrap transaction.
- **Request Body**:
    - **JSON Object** with the `transaction_id` to track the progress of a specific bootstrap transaction.
    
    Example message to send upon connecting:
    ```json
    {
      "transaction_id": "your_transaction_id_here"
    }
    ```

- **Response**:
    The WebSocket will stream real-time updates as the bootstrap operation progresses. These updates may include information such as:
    - **Transaction status** (e.g., `PENDING`, `SUCCESS`, `FAILED`)
    - **Error messages** (if applicable)
    - **Record counts** for the bootstrap process (e.g., number of input records, number of records written to Hudi)
    
    Example of a response:
    ```json
    {
      "status": "PENDING",
      "error_message": "",
      "record_counts": {
        "input_count": None,
        "hudi_count": None
      }
    }
    ```

- **Description**: This endpoint allows clients to receive real-time updates on the status of a bootstrap transaction. As the bootstrap operation progresses, status updates are pushed to the connected clients via the WebSocket.

### React Frontend

The React frontend allows users to:

- **Bootstrap a New Hudi Table**: Users can submit a form with the necessary details to initiate a Hudi bootstrap operation.
- **View Bootstrap History**: Users can view a history table that shows previous bootstrap operations, including their status and error logs.
- **Real-time Progress Updates**: Users can subscribe to WebSocket updates to view real-time progress for ongoing bootstrap operations.

#### Components Overview

- **App.js**: Main application component that manages routing and layout.
- **BootstrapPage.js**: Component for handling the bootstrap process, including the form submission to the `/bootstrap_hudi/` endpoint.
- **HistoryTable.js**: Component for displaying the history of bootstrap transactions. It interacts with the `/bootstrap_history/` endpoint to fetch transaction details.
- **Theme.js**: Custom Material-UI theme configuration to style the React app.
- **WebSocket Component**: A component that connects to the `/ws/` WebSocket endpoint to receive real-time progress updates.

### FastAPI Application (`app.py`)

The FastAPI backend contains endpoints for managing the bootstrap process, including:

- A `/bootstrap_hudi/` endpoint to initiate bootstrapping via Spark.
- A `/bootstrap_history/` endpoint to retrieve bootstrap history from the PostgreSQL database.
- A `/bootstrap_status/{transaction_id}/` endpoint to check the status of a specific bootstrap transaction.
- A `/check_path_or_table/ endpoint to validate if a given path is an HDFS directory or a Hive table.
- A WebSocket `/ws/` endpoint to stream real-time updates of bootstrap transaction status.

The backend also uses **SQLAlchemy** for managing the database and **PySpark** for running Spark jobs in the background.

---

### Troubleshooting

1. **FastAPI Application not Starting**: Ensure that the required environment variables are set in `.env` file (e.g., `DATABASE_URL`, `HIVE_HOST`, `HDFS_PATH`). The FastAPI server should be accessible at `http://127.0.0.1:8000`.

2. **React App Issues**: If the React app fails to start, make sure all dependencies are installed by running `npm install` in the `react-frontend` directory.

3. **Database Connection Issues**: Verify the PostgreSQL database is running, and the credentials in `.env` match the database configuration.

4. **WebSocket Connection Issues**: Ensure that the WebSocket endpoint is correctly implemented and that the `transaction_id` provided is valid. You can check the WebSocket logs in the FastAPI server to troubleshoot connection issues.

---

### Conclusion

This Hudi Bootstrap Application allows users to efficiently bootstrap Hudi tables using the FastAPI backend and provides an intuitive user interface via React. It supports querying transaction history, checking bootstrap status, validating HDFS or Hive paths/tables, and streaming real-time progress updates via WebSocket. The architecture is modular and extensible, allowing for future improvements and additional features.


