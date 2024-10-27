
# FastAPI and React Application

This project consists of a FastAPI backend and a React frontend. The FastAPI backend handles data processing and storage in a PostgreSQL database, while the React frontend provides a user interface for interaction.

## Directory Structure

```
.
├── fastapi-backend
│   ├── main.py              # FastAPI application
│   ├── pyspark_script.py     # PySpark script for data processing
│   ├── requirements.txt      # Optional: list of Python dependencies
│   └── venv                 # Python virtual environment
└── react-frontend
    ├── src                  # React application source files
    ├── package.json         # NPM dependencies
    └── public               # Static files
```

## Prerequisites

- **Python 3.7 or higher**
- **Node.js and npm**
- **PostgreSQL**

## Setup Instructions

### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone <repository-url>
cd <repository-directory>
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
uvicorn main:app --reload
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

- **FastAPI Endpoints**:
  - `/bootstrap_hudi/`: POST request to bootstrap a Hudi table.
  - `/bootstrap_history/`: GET request to retrieve bootstrap history.

- **React Frontend**: Use the interface to interact with the FastAPI backend, submitting data and viewing transaction history.
```

