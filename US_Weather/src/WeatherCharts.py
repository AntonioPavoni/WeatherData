import matplotlib.pyplot as plt
import pandas as pd
import os
from pymongo import MongoClient
from datetime import datetime, timedelta
import re

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['weather_database']

# Define cities to filter
cities = ["New York city, New York", "Buffalo city, New York", "Rochester city, New York"]

# Define image directory to save plots
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # Get parent folder
img_dir = os.path.join(parent_dir, "img")
os.makedirs(img_dir, exist_ok=True)

# Fetch data from Hourly Forecasts
hourly_collection = db['Hourlyforecasts']  # Corrected dataset name
hourly_data = list(hourly_collection.find({"city": {"$in": cities}}))

def parse_wind_speed(wind_speed_str):
    """Extract numeric wind speed from string."""
    if isinstance(wind_speed_str, str):
        match = re.search(r'\d+', wind_speed_str)
        return int(match.group()) if match else None
    return None

# Process Hourly Forecast Data
temp_data = []
precip_data = []
wind_data = []
forecasttext = []

for entry in hourly_data:
    city = entry.get('city', 'Unknown')
    if 'forecasts' not in entry:
        print(f"No forecasts found for {city}")
        continue
    for forecast in entry['forecasts']:
        try:
            timestamp = datetime.fromisoformat(forecast['startTime'])
            temp_data.append({'time': timestamp, 'city': city, 'temperature': forecast.get('temperature', None)})
            precip_data.append({'time': timestamp, 'city': city, 'precipitation': forecast.get('probOfPrecipitationValue', None)})
            wind_data.append({'time': timestamp, 'city': city, 'windSpeed': parse_wind_speed(forecast.get('windSpeed', ''))})
            forecasttext.append({'time': timestamp, 'city': city, 'Forecast': forecast.get('forecast', '')})
        except Exception as e:
            print(f"Error processing forecast data for {city}: {e}")

# Convert to DataFrame
temp_df = pd.DataFrame(temp_data)
precip_df = pd.DataFrame(precip_data)
wind_df = pd.DataFrame(wind_data)
forecast_df = pd.DataFrame(forecasttext)

# Fetch data from Quantitative Forecasts
quantitative_collection = db['quantitativeForecasts']
quantitative_data = list(quantitative_collection.find({"city": {"$in": cities}}))

# Process Quantitative Precipitation Data
quant_precip_data = []
for entry in quantitative_data:
    city = entry.get('city', 'Unknown')
    for precip in entry.get('quantitativePrecipitation', []):
        try:
            timestamp = datetime.fromisoformat(precip['validTime'].split('/')[0])
            quant_precip_data.append({'time': timestamp, 'city': city, 'precipitation': precip['value']})
        except Exception as e:
            print(f"Error processing quantitative precipitation data for {city}: {e}")

# Convert to DataFrame
quant_precip_df = pd.DataFrame(quant_precip_data)

def save_plot(fig, filename):
    """Save the plot to the img directory."""
    filepath = os.path.join(img_dir, filename)
    fig.tight_layout()  # Ensure all plot elements fit correctly
    fig.savefig(filepath, bbox_inches='tight')
    print(f"Saved plot: {filepath}")

# Plot Temperature Evolution
fig = plt.figure(figsize=(12, 6))
for city in cities:
    city_data = temp_df[temp_df['city'] == city]
    if not city_data.empty:
        plt.plot(city_data['time'], city_data['temperature'], label=city)
plt.xlabel("Time")
plt.ylabel("Temperature (°F)")
plt.title("Temperature Evolution Over Hours")
plt.legend()
plt.xticks(rotation=45)
plt.grid()
save_plot(fig, "temperature_evolution.png")
plt.show()

# Plot Wind Speed Evolution
fig = plt.figure(figsize=(12, 6))
for city in cities:
    city_data = wind_df[wind_df['city'] == city]
    if not city_data.empty:
        plt.plot(city_data['time'], city_data['windSpeed'], label=city)
plt.xlabel("Time")
plt.ylabel("Wind Speed (mph)")
plt.title("Wind Speed Evolution Over Hours")
plt.legend()
plt.xticks(rotation=45)
plt.grid()
save_plot(fig, "wind_speed_evolution.png")
plt.show()

# Plot Precipitation Probability
fig = plt.figure(figsize=(12, 6))
for city in cities:
    city_data = precip_df[precip_df['city'] == city]
    if not city_data.empty:
        plt.plot(city_data['time'], city_data['precipitation'], label=city)
plt.xlabel("Time")
plt.ylabel("Precipitation Probability (%)")
plt.title("Precipitation Probability Over Hours")
plt.legend()
plt.xticks(rotation=45)
plt.grid()
save_plot(fig, "precipitation_probability.png")
plt.show()

# Plot Cloud Cover
fig = plt.figure(figsize=(12, 6))
for city in cities:
    city_data = forecast_df[forecast_df['city'] == city]
    if not city_data.empty:
        plt.scatter(city_data['time'], city_data['Forecast'], label=city)
plt.xlabel("Time")
plt.ylabel("Forecast")
plt.title("Forecast Over Hours")
plt.legend()
plt.xticks(rotation=45)
plt.grid()
save_plot(fig, "forecast_over_time.png")
plt.show()

# Plot Quantitative Precipitation Over Time
fig = plt.figure(figsize=(12, 6))
for city in cities:
    city_data = quant_precip_df[quant_precip_df['city'] == city]
    if not city_data.empty:
        plt.plot(city_data['time'], city_data['precipitation'], label=city)
plt.xlabel("Time")
plt.ylabel("Precipitation (mm)")
plt.title("Quantitative Precipitation Over Time")
plt.legend()
plt.xticks(rotation=45)
plt.grid()
save_plot(fig, "quantitative_precipitation.png")
plt.show()
