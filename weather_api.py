import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Request daily weather data for Theatre district in New York City, USA
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 40.759,
	"longitude": 73.9845,
	"start_date": "1996-03-24",
	"end_date": "2025-02-02",
	"daily": ["temperature_2m_mean", "temperature_2m_min", "temperature_2m_max", "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean", "sunset", "sunrise", "precipitation_sum", "precipitation_hours", "daylight_duration", "sunshine_duration", "snowfall_sum", "rain_sum", "wind_gusts_10m_max", "wind_speed_10m_max", "weather_code"],
	"timezone": "auto"
}
responses = openmeteo.weather_api(url, params=params)

# Process the response
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Extract daily weather data
daily = response.Daily()
daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
daily_temperature_2m_max = daily.Variables(2).ValuesAsNumpy()
daily_apparent_temperature_max = daily.Variables(3).ValuesAsNumpy()
daily_apparent_temperature_min = daily.Variables(4).ValuesAsNumpy()
daily_apparent_temperature_mean = daily.Variables(5).ValuesAsNumpy()
daily_sunset = daily.Variables(6).ValuesAsNumpy()
daily_sunrise = daily.Variables(7).ValuesAsNumpy()
daily_precipitation_sum = daily.Variables(8).ValuesAsNumpy()
daily_precipitation_hours = daily.Variables(9).ValuesAsNumpy()
daily_daylight_duration = daily.Variables(10).ValuesAsNumpy()
daily_sunshine_duration = daily.Variables(11).ValuesAsNumpy()
daily_snowfall_sum = daily.Variables(12).ValuesAsNumpy()
daily_rain_sum = daily.Variables(13).ValuesAsNumpy()
daily_wind_gusts_10m_max = daily.Variables(14).ValuesAsNumpy()
daily_wind_speed_10m_max = daily.Variables(15).ValuesAsNumpy()
daily_weather_code = daily.Variables(16).ValuesAsNumpy()

# Create a dictionary with daily weather data
daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}

# Assign daily weather data to the dictionary
daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
daily_data["temperature_2m_min"] = daily_temperature_2m_min
daily_data["temperature_2m_max"] = daily_temperature_2m_max
daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
daily_data["apparent_temperature_mean"] = daily_apparent_temperature_mean
daily_data["sunset"] = daily_sunset
daily_data["sunrise"] = daily_sunrise
daily_data["precipitation_sum"] = daily_precipitation_sum
daily_data["precipitation_hours"] = daily_precipitation_hours
daily_data["daylight_duration"] = daily_daylight_duration
daily_data["sunshine_duration"] = daily_sunshine_duration
daily_data["snowfall_sum"] = daily_snowfall_sum
daily_data["rain_sum"] = daily_rain_sum
daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
daily_data["weather_code"] = daily_weather_code

# Create a DataFrame from the dictionary and save it to a CSV file
daily_dataframe = pd.DataFrame(data = daily_data)
daily_dataframe.to_csv("daily_weather_data.csv", index = False)