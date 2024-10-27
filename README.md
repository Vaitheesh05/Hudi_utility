
# Hudi Bootstrap Application

This project consists of a **FastAPI** backend and a **React** frontend. The FastAPI backend manages data processing and storage in a **PostgreSQL** database, while the React frontend provides a user-friendly interface for interaction with the backend services.

## Overview

The application allows users to bootstrap Hudi tables using data from various file formats (Parquet and ORC) and track the history of bootstrap transactions.

## Directory Structure

```
.
├── fastapi-backend
│   ├── app.py              # FastAPI application
│   └── pyspark_script.py     # PySpark script for data processing
└── react-frontend
    ├── src                  # React application source files
    │   ├── App.js           # Main application component
    │   ├── BootstrapPage.js  # Component for bootstrapping Hudi tables
    │   ├── HistoryTable.js    # Component for displaying bootstrap history
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

- **POST** `/bootstrap_hudi/`
  - Description: Bootstrap a Hudi table using specified configurations.
  - Request Body:
    ```json
    {
      "data_file_path": "string",
      "hudi_table_name": "string",
      "key_field": "string",
      "precombine_field": "string",
      "partition_field": "string",
      "hudi_table_type": "string",  // COPY_ON_WRITE or MERGE_ON_READ
      "write_operation": "string",   // insert or upsert
      "output_path": "string",
      "spark_config": {"key": "value"}, // Optional
      "bootstrap_type": "string",     // FULL_RECORD or METADATA_ONLY
      "partition_regex": "string"     // Optional regex for partitions
    }
    ```

- **GET** `/bootstrap_history/`
  - Description: Retrieve the history of bootstrap transactions.
  - Query Parameters:
    - `start_date`: Optional ISO format date string to filter transactions.
    - `end_date`: Optional ISO format date string to filter transactions.
    - `transaction_id`: Optional transaction ID to filter specific transactions.

### React Frontend

- The React frontend allows users to:
  - Bootstrap a new Hudi table using a form.
  - View the history of previous bootstrap transactions.

## Components Overview

- **App.js**: Main application component that manages routing and layout.
- **BootstrapPage.js**: Component for handling the bootstrap process.
- **HistoryTable.js**: Component for displaying the history of bootstrap transactions.
- **theme.js**: Custom Material-UI theme configuration.

