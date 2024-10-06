
# Weather Data Pipeline

## Overview

This project implements a weather data pipeline that retrieves current weather data for multiple locations from the OpenWeather API, processes and combines the data, and uploads the results to Azure Blob Storage as a Parquet file. The pipeline is orchestrated using Apache Airflow, with the tasks handled in a modularized Python project structure. The pipeline runs every hour on a schedule, and secrets such as the OpenWeather API key and Azure Blob Storage URL are retrieved securely from Azure Key Vault.

## Table of Contents

1. [Features](#features)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Setting Up the Project](#setting-up-the-project)
5. [Airflow Setup](#airflow-setup)
6. [Running the Project](#running-the-project)
7. [Detailed Explanation of the DAG](#detailed-explanation-of-the-dag)
8. [Testing](#testing)
9. [Monitoring and Debugging](#monitoring-and-debugging)
10. [Future Enhancements](#future-enhancements)
11. [Conclusion](#conclusion)

---

## Features

- **Data Retrieval**: Retrieves current weather data from the OpenWeather API for multiple locations (latitude and longitude).
- **Data Processing**: Combines weather data into a pandas DataFrame, processes it, and stores it in Parquet format.
- **Blob Upload**: Uploads the combined weather data to Azure Blob Storage.
- **Airflow Orchestration**: Uses Airflow to manage task dependencies and schedule the pipeline to run periodically.
- **Azure Key Vault Integration**: Retrieves sensitive credentials from Azure Key Vault to securely access the OpenWeather API and Azure Blob Storage.

---

## Project Structure

```
.
├── ASTRONOMER.md                   # Documentation for setting up Airflow using Astronomer
├── Dockerfile                      # Docker setup for running Airflow in a containerized environment
├── README.md                       # Main project documentation explaining the weather data pipeline
├── airflow_settings.yaml           # Airflow configuration settings (e.g., connections, variables)
├── dags                            # Directory containing Airflow DAGs
│   ├── dag.py                      # Main DAG file that orchestrates the weather data pipeline
│   └── src                         # Source directory containing the core logic for tasks in the pipeline
│       ├── config.py               # Handles secret retrieval from Azure Key Vault
│       ├── convert_to_df.py        # Converts API response data to a pandas DataFrame
│       ├── fetch_weather_data.py   # Fetches weather data from OpenWeather API
│       ├── load_locations.py       # Loads location data from a JSON file
│       ├── logger_config.py        # Configures logging for the project
│       ├── process_weather_data.py # Processes the fetched weather data for all locations
│       └── upload_data_to_blob.py  # Uploads processed weather data to Azure Blob Storage
├── include                         # Directory containing additional resources
│   └── locations.json              # JSON file with location data (latitude, longitude)
├── packages.txt                    # System-level dependencies (e.g., Linux packages) required for the project
├── plugins                         # Directory for any Airflow plugins (currently empty)
├── requirements.txt                # Python dependencies for the project, managed by Airflow via Astronomer
└── tests                           # Directory containing unit and integration tests
    └── dags
        └── test_dag_example.py     # Example test for the DAG

```

---

## Prerequisites

### 1. Azure Account
- **Azure Blob Storage**: You will need an Azure Blob Storage account to store the output data.
- **Azure Key Vault**: Create a Key Vault to securely store the OpenWeather API key and the Azure Blob Storage URL.

### 2. OpenWeather API
- Sign up at [OpenWeather](https://openweathermap.org/) and obtain an API key.

### 3. Astro CLI
- Refer to [ASTRONOMER.md](./ASTRONOMER.md) for the Astronomer setup and instructions.

---

## Setting Up the Project

### 1. Environment Configuration

1. **Azure Key Vault**: 
   Store the following secrets in Azure Key Vault:
   - **OpenWeather API Key**: The key for accessing OpenWeather API. Store it as `OpenWeatherApiKey`.
   - **Azure Blob Storage URL**: The URL for accessing your Blob Storage account. Store it as `AzureStorageUrl`.

2. **`.env` File** (Optional but recommended for local testing):
   Create a `.env` file at the root of your project to store environment variables:
   ```bash
   AZURE_CLIENT_ID=<your-azure-client-id>
   AZURE_TENANT_ID=<your-azure-tenant-id>
   AZURE_CLIENT_SECRET=<your-azure-client-secret>
   AZURE_VAULT_URI=https://<your-vault-name>.vault.azure.net/
   ```

   Authentication to Azure Key vault and Blob Storage is done using a `Service Principal`. Make sure you create a service principal and grant it access to your secret and blob container.

### 2. Location Data

The `locations.json` file in the `include/` directory contains the latitude and longitude of the locations for which you want to fetch weather data. You can add as many locations as needed.

Sample structure of `locations.json`:
```json
[
  {"lat": 51.509865, "lon": -0.118092},  
  {"lat": 40.730610, "lon": -73.935242},
  {"lat": 35.652832, "lon": 139.839478} 
]
```

---

## Airflow Setup

You are using **Astronomer** for Airflow setup, and there is a dedicated README provided for that setup. Please follow that for the details on how to set up Airflow using Astronomer.

---

## Running the Project

### 1. Load the Airflow DAG

Once the Airflow environment is up and running (through Astronomer), the DAG file (`weather_data_pipeline.py`) should automatically be picked up by Airflow. You should see the DAG available in the Airflow UI.

### 2. Trigger the DAG

You can manually trigger the DAG from the Airflow UI to ensure that everything is working correctly.

1. Go to the Airflow UI.
2. Look for the `weather_data_pipeline` DAG.
3. Click on the DAG and press "Trigger DAG" to start running it.

---

## Detailed Explanation of the DAG

The pipeline is broken down into the following steps:

### 1. **Secrets Retrieval**
The DAG first retrieves secrets (API Key and Blob Storage URL) from Azure Key Vault using `config.py`. This ensures that sensitive credentials are securely managed.

### 2. **Location Data Loading**
Next, it loads location data from the `locations.json` file using `load_locations.py`. Each location is represented as a dictionary containing latitude and longitude values.

### 3. **Weather Data Processing**
Using `process_weather_data.py`, the DAG fetches weather data for each location from the OpenWeather API, processes the data, and combines it into a pandas DataFrame.

### 4. **Data Upload to Azure Blob**
The processed data is then saved as a Parquet file in memory and uploaded to Azure Blob Storage using `upload_data_to_blob.py`. The filename includes a timestamp for easy identification.

---

## Testing

### 1. Test Each Module
Each module (`config.py`, `load_locations.py`, `process_weather_data.py`, `upload_data_to_blob.py`) can be tested independently. Use Python's `unittest`, `pytest` or another testing framework to verify individual components.

### 2. Test End-to-End Flow
Trigger the Airflow DAG and check the Airflow UI logs for each task to ensure the entire pipeline runs smoothly. After the DAG completes, verify that the weather data Parquet file is uploaded to your Azure Blob Storage.

### 3. Check Logs
Each module logs its output into log files in the `logs/` directory. Review these logs for any errors or warnings during the execution of the pipeline.

---

## Monitoring and Debugging

### 1. Airflow UI
The Airflow UI allows you to monitor DAG runs and task status in real-time. You can retry failed tasks, check logs, and trigger manual runs as needed.

### 2. Logs
Detailed logs are generated for each task and stored in the `logs/` directory within the scheduler and also on the Airflow's UI. You can use these logs to diagnose issues or monitor the health of the pipeline.

---

## Future Enhancements

- **Error Handling Improvements**: Add more robust error handling for network issues, data validation, etc.
- **Data Transformation**: Implement more complex data transformations and analytics in the `process_weather_data.py`.
- **Scalability**: Use distributed systems like Dask or Spark for larger datasets or more frequent requests.
- **Notification System**: Add alerts or notifications to notify users of pipeline successes or failures.
- **Testing**: Expand the test coverage by adding unit, integration, and end-to-end tests for all modules, including DAG execution and data integrity checks.


---

## Conclusion

This weather data pipeline provides a comprehensive, automated solution to fetch, process, and store weather data from the OpenWeather API to Azure Blob Storage using Airflow. By following the steps in this guide, you can set up the pipeline and customize it for your needs.
