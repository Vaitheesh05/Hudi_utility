#!/bin/bash

# Update and install PostgreSQL
sudo apt update
sudo apt install -y postgresql postgresql-contrib

# Create PostgreSQL database and user
sudo -u postgres psql <<EOF
CREATE DATABASE hudi_bootstrap_db;
CREATE USER hudi_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE hudi_bootstrap_db TO hudi_user;
EOF


# Install necessary Python libraries
pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic pydoop pyhive thrift thrift-sasl python-dotenv

# Optionally install other libraries you might need for your project
# pip install <other-libraries>

# Navigate to the React frontend directory
cd react-frontend

# Install npm packages
npm install


# Start the FastAPI server (if you want to do this as part of the setup)
# uvicorn main:app --reload &  # Uncomment if you want to run the server in the background

echo "Setup completed successfully."

