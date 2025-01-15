import schedule
import time
import requests
import json
import os

# Function to delete the existing JSON file if it exists
def reset_weather_data_file(filename="weather_data.json"):
    """Delete the existing JSON file if it exists."""
    if os.path.exists(filename):
        os.remove(filename)
        print(f"{filename} has been reset.")
    else:
        print(f"{filename} does not exist. Starting fresh.")

def fetch_simple_weather(lat, lon, api_key):
    """Fetch weather data from OpenWeatherMap API."""
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        return None

def preprocess_weather_data(data):
    """Preprocess raw weather data into the desired format."""
    if not data:
        return None
    return {
        "timestamp": data.get("dt"),
        "temperature": data["main"].get("temp"),
        "precipitation": data.get("rain", {}).get("1h", 0),  # Last hour precipitation
        "wind_speed": data["wind"].get("speed"),
        "cloudiness": data["clouds"].get("all")
    }

def save_weather_data(data, filename="weather_data.json"):
    """Save weather data to a JSON file in array format."""
    if data:
        # Check if the file exists
        if os.path.exists(filename):
            # Load existing data
            with open(filename, "r") as file:
                try:
                    existing_data = json.load(file)
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []

        # Append new data
        existing_data.append(data)

        # Save updated data
        with open(filename, "w") as file:
            json.dump(existing_data, file, indent=4)

def job():
    """Fetch, preprocess, and save weather data at scheduled intervals."""
    lat, lon = 40.7128, -74.006  # New York City
    api_key = "8104b06580b72d9308c015a06acc4fe6"  # Replace with your API key
    weather_data = fetch_simple_weather(lat, lon, api_key)
    processed_data = preprocess_weather_data(weather_data)
    if processed_data:
        save_weather_data(processed_data)
        print("Weather data saved:", processed_data)

# Reset the JSON file when the script starts
reset_weather_data_file()

# Schedule the job to run every 5 minutes
schedule.every(5).minutes.do(job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
