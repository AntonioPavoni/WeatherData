import requests
import pandas as pd
import os
from datetime import datetime
from pymongo import MongoClient
from pytz import timezone
from timezonefinder import TimezoneFinder

# Initialize TimezoneFinder
tf = TimezoneFinder()

# Define the dynamic path for the Excel file
script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory where the script is located
parent_dir = os.path.abspath(os.path.join(script_dir, ".."))  # Get parent directory
file_path = os.path.join(parent_dir, "Data", "WeatherStationDatabase.xlsx")  # Build the dynamic path

# Load the Excel file
sheet_name = "NewYork"
df = pd.read_excel(file_path, sheet_name=sheet_name)

# Extract required columns
data_entries = df[['gridId', 'gridX', 'gridY', 'INTPTLAT', 'INTPTLONG', 'NAME.1']]

def get_time_zone(lat, lon):
    """Get the timezone for given latitude and longitude."""
    tz_name = tf.timezone_at(lng=lon, lat=lat)
    return tz_name if tz_name else 'UTC'

def get_forecast_url(grid_id, grid_x, grid_y):
    """Build NOAA daily forecast API URL using grid values."""
    return f"https://api.weather.gov/gridpoints/{grid_id}/{grid_x},{grid_y}/forecast"

def get_daily_forecast(forecast_url, tz_name):
    """Fetch the daily weather forecast using NOAA API."""
    response = requests.get(forecast_url)
    if response.status_code == 200:
        data = response.json()
        properties = data.get('properties', {})
        structured_forecasts = []
        
        for period in properties.get('periods', []):
            start_time = datetime.fromisoformat(period['startTime'].replace('Z', '+00:00'))
            structured_forecasts.append({
                'startTime': start_time.astimezone(timezone(tz_name)).isoformat(),
                "isDaytime": period['isDaytime'],
                'temperature': period['temperature'],
                'temperatureUnit': period['temperatureUnit'],
                'probOfPrecipitationValue': period['probabilityOfPrecipitation']['value'],
                'probOfPrecipitationUnit': period['probabilityOfPrecipitation']['unitCode'],
                'windSpeed': period['windSpeed'],
                'windDirection': period['windDirection'],
                'forecast': period['shortForecast']
            })
        
        return {
            'updateTime': properties.get('updateTime', ''),
            'generatedAt': properties.get('generatedAt', ''),
            'forecasts': structured_forecasts
        }
    else:
        return {'error': f'Failed to fetch data from {forecast_url}', 'status_code': response.status_code}

def save_forecasts_to_mongo(forecast_data, city_name):
    """Save forecast data to MongoDB with city name."""
    client = MongoClient('mongodb://localhost:27017/')
    db = client['weather_database']
    collection = db['daily_forecasts']
    forecast_data['city'] = city_name
    forecast_data['insertedAt'] = datetime.now()
    collection.insert_one(forecast_data)
    print(f"Data for {city_name} inserted into MongoDB")

# Process each city in the dataset
for index, row in data_entries.iterrows():
    grid_id, grid_x, grid_y = row['gridId'], row['gridX'], row['gridY']
    lat, lon = float(row['INTPTLAT']), float(row['INTPTLONG'])
    city_name = row['NAME.1']
    
    # Get timezone
    tz_name = get_time_zone(lat, lon)
    
    # Build forecast URL
    forecast_url = get_forecast_url(grid_id, grid_x, grid_y)
    
    # Fetch forecast
    forecast_data = get_daily_forecast(forecast_url, tz_name)
    
    # Save to MongoDB
    if 'error' not in forecast_data:
        save_forecasts_to_mongo(forecast_data, city_name)
    else:
        print(f"Error fetching data for {city_name}: {forecast_data['error']}")
