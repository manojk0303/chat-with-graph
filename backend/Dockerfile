# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the backend code into the container
COPY . .

# Make sure the data folder is copied (crucial for DuckDB ingestion)
# Assuming sap-o2c-data is in the same directory as the Dockerfile
COPY sap-o2c-data ./sap-o2c-data

# Expose the port the app runs on
ENV PORT=8080
EXPOSE 8080

# Command to run the application (use shell form to expand $PORT)
CMD uvicorn main:app --host 0.0.0.0 --port $PORT
