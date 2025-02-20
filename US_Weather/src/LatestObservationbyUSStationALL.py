import requests
import pandas as pd
import os
from requests.exceptions import RequestException
import logging

# Setup basic configuration for logging
script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory where the script is located
parent_dir = os.path.abspath(os.path.join(script_dir, ".."))  # Move one level up from `src`

# Define dynamic paths for input and output files
stations_file_path = os.path.join(parent_dir, "Stations", "Stations.xlsx")
observations_file_path = os.path.join(parent_dir, "Observations", "Observations.xlsx")

# Ensure the observations directory exists before saving
os.makedirs(os.path.dirname(observations_file_path), exist_ok=True)

# Setup logging file path dynamically
log_file_path = os.path.join(parent_dir, "Logs", "error_log.log")
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(filename=log_file_path, level=logging.ERROR, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Load the Excel file into a pandas DataFrame
df = pd.read_excel(stations_file_path)

# Define the base URL for API calls
base_url = "https://api.weather.gov/stations"

# Create a new DataFrame to store the observations
observations_data = []

# Function to get value from the property
def get_property_value(prop, key):
    return prop.get(key, {}).get('value') if key in prop else None

# Function to get unit code from the property
def get_property_unit(prop, key):
    return prop.get(key, {}).get('unitCode') if key in prop else None

# Iterate over each station ID in the DataFrame
for index, row in df.iterrows():
    station_id = row['stationIdentifier']
    observation_url = f"{base_url}/{station_id}/observations/latest"

    try:
        # Make the API call
        print(f"Attempting to retrieve data for station ID: {station_id}")
        response = requests.get(observation_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON data from the response
            observation = response.json().get('properties', {})
            
            # Prepare the data to be added to the observations DataFrame
            observation_data = {
                'id': station_id,
                "type": observation.get('"@type"'),
                'timestamp': observation.get('timestamp'),
                'textDescription': observation.get('textDescription'),

                'temperatureUnit': get_property_unit(observation, 'temperature'),
                'temperatureValue': get_property_value(observation, 'temperature'),

                'dewpointUnit': get_property_unit(observation, 'dewpoint'),
                'dewpointValue': get_property_value(observation, 'dewpoint'),

                'windDirectionUnit': get_property_unit(observation, 'windDirection'),
                'windDirectionValue': get_property_value(observation, 'windDirection'),

                'windSpeedUnit': get_property_unit(observation, 'windSpeed'),
                'windSpeedValue': get_property_value(observation, 'windSpeed'),

                'windGustUnit': get_property_unit(observation, 'windGust'),
                'windGustValue': get_property_value(observation, 'windGust'),

                'barometricPressureUnit': get_property_unit(observation, 'barometricPressure'),
                'barometricPressureValue': get_property_value(observation, 'barometricPressure'),

                'seaLevelPressureUnit': get_property_unit(observation, 'seaLevelPressure'),
                'seaLevelPressureValue': get_property_value(observation, 'seaLevelPressure'),

                'visibilityUnit': get_property_unit(observation, 'visibility'),
                'visibilityValue': get_property_value(observation, 'visibility'),

                'maxTemperatureLast24HoursUnit': get_property_unit(observation, 'maxTemperatureLast24Hours'),
                'maxTemperatureLast24HoursValue': get_property_value(observation, 'maxTemperatureLast24Hours'),

                'minTemperatureLast24HoursUnit': get_property_unit(observation, 'minTemperatureLast24Hours'),
                'minTemperatureLast24HoursValue': get_property_value(observation, 'minTemperatureLast24Hours'),

                'precipitationLast3HoursUnit': get_property_unit(observation, 'precipitationLast3Hours'),
                'precipitationLast3HoursValue': get_property_value(observation, 'precipitationLast3Hours'),

                'relativeHumidityUnit': get_property_unit(observation, 'relativeHumidity'),
                'relativeHumidityValue': get_property_value(observation, 'relativeHumidity'),

                'windChillUnit': get_property_unit(observation, 'windChill'),
                'windChillValue': get_property_value(observation, 'windChill'),

                'heatIndexUnit': get_property_unit(observation, 'heatIndex'),
                'heatIndexValue': get_property_value(observation, 'heatIndex'),
            }
            
            # Add the observation data to the list
            observations_data.append(observation_data)
            print(f"Successfully retrieved data for station ID: {station_id}")
        else:
            print(f"Failed to retrieve data for station ID: {station_id}, Status Code: {response.status_code}")
            logging.error(f"Failed to retrieve data for station ID: {station_id}, Status Code: {response.status_code}")

    except RequestException as e:
        print(f"RequestException occurred for station ID: {station_id}, Error: {e}")
        logging.error(f"RequestException occurred for station ID: {station_id}, Error: {e}")

# Convert the observations data into a DataFrame
observations_df = pd.DataFrame(observations_data)

# Save the observations DataFrame to a new Excel file
observations_df.to_excel(observations_file_path, index=False)

print(f"All observations have been retrieved and saved to {observations_file_path}")
