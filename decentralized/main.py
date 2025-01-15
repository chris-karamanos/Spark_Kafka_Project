import schedule
import time
import requests
import json
import os

def reset_data_file(filename):
    """Delete the existing JSON file if it exists."""
    if os.path.exists(filename):
        os.remove(filename)
        print(f"{filename} has been reset.")
    else:
        print(f"{filename} does not exist. Starting fresh.")

# Fetching

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

def fetch_bike_station_information():
    try:
        url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("data", {}).get("stations", [])
    except requests.exceptions.RequestException as err:
        print(f"Error occurred when retrieving station information: {err}")
        return None

def fetch_bike_station_status():
    try:
        url = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("data", {}).get("stations", []) , response.json().get("last_updated")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred when retrieving station status: {err}")
        return None

# Pre-processing to remove unnecessary data

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

def merge_station_data(station_info, station_status, last_updated):
    # Convert station status into a dictionary for quick lookup by station_id
    status_lookup = {station["station_id"]: station for station in station_status}

    # Merge information
    merged_data = {"timestamp" : last_updated, "stations" : []}
    for info in station_info:
        station_id = info.get("station_id")
        status = status_lookup.get(station_id, {})

        # Combine the data
        merged_data["stations"].append({
            "station_id": station_id,
            "name": info.get("name"),
            "capacity": info.get("capacity"),
            "location": (info.get("lon"), info.get("lat")),
            "num_bikes_available": status.get("num_bikes_available", 0),
            "num_docks_available": status.get("num_docks_available", 0)
        })

    return merged_data

def save_data(data, filename):
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
        save_data(processed_data,"weather_data.json")
        print("Weather data saved:", processed_data)

    # Bikes

    # Endpoint 2
    bike_station_status,last_updated = fetch_bike_station_status()

    # Combine
    merged_data = merge_station_data(bike_station_information, bike_station_status, last_updated)

    if bike_station_information and bike_station_status and merged_data:
        save_data(merged_data, "bike_station_data.json")
        print("Bike station data saved timestamp : ", merged_data["timestamp"] , " with " , len(merged_data["stations"]) , " stations")

if __name__ == '__main__':
    # Reset the JSON files when the script starts
    reset_data_file("weather_data.json")
    reset_data_file("bike_station_data.json")

    # Καλούμε fetch bike station information μια φορα για να παρουμε
    # static πληροφοριες οπως ονομα σταθμου , τοποθεσια κλπ και να μην
    # χρειαζεται να τα καλουμε καθε φορα

    # Endpoint 1
    bike_station_information = fetch_bike_station_information()

    # Ορισμός χρονοδιαγράμματος (κάθε 5 λεπτά)
    schedule.every(5).minutes.do(job)

    # Εκτέλεση
    while True:
        schedule.run_pending()
        time.sleep(1)

