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
def preprocess_bike_station_information(data):
    """Preprocess raw bike station information into the desired format."""
    if not data:
        return None
    return [{
        "station_id": station["station_id"],
        "name": station["name"],
        "capacity": station["capacity"],
        "lon": station["lon"],
        "lat": station["lat"]
    } for station in data]

def merge_station_data(station_status, last_updated):

    status_lookup = {station["station_id"]: station for station in station_status}

    # Merge information
    merged_data = {"timestamp" : last_updated, "stations" : []}
    for station in station_status:
        station_id = station.get("station_id")
        status = status_lookup.get(station_id, {})

        # Combine the data
        merged_data["stations"].append({
            "station_id": station_id,
            "num_bikes_available": status.get("num_bikes_available", 0),
            "num_docks_available": status.get("num_docks_available", 0)
        })

    return merged_data

def save_data(data, filename, overwrite=False):
    """Save data to a JSON file, ensuring proper format."""
    if data:
        if overwrite or not os.path.exists(filename):  #  Static data is overwritten
            with open(filename, "w") as file:
                json.dump(data if isinstance(data, list) else [data], file, indent=4)  #  Ensure list format
        else:
            with open(filename, "r") as file:
                try:
                    existing_data = json.load(file)
                    if isinstance(existing_data, list):  #  Ensure existing data is a list
                        existing_data.append(data)  #  Append new entry
                    else:
                        existing_data = [existing_data, data]  # Fix incorrect format
                except json.JSONDecodeError:
                    existing_data = [data]

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

    # Endpoint 2 Station Status
    bike_station_status,last_updated = fetch_bike_station_status()

    # Combine
    merged_data = merge_station_data(bike_station_status, last_updated)
    if merged_data:
        save_data(merged_data,"bike_station_status.json")

    # if bike_station_information and bike_station_status and merged_data:
    #     save_data(merged_data, "bike_station_data.json")
    #     print("Bike station data saved timestamp : ", merged_data["timestamp"] , " with " , len(merged_data["stations"]) , " stations")

if __name__ == '__main__':
    # Reset the JSON files when the script starts
    #reset_data_file("weather_data.json")
    #reset_data_file("bike_station_information.json")
    #reset_data_file("bike_station_status.json")


    # Endpoint 1 Αποθήκευση στοιχείων των σταθμών
    bike_station_information = fetch_bike_station_information()
    bike_station_information = preprocess_bike_station_information(bike_station_information)
    if bike_station_information:
        save_data(bike_station_information, "bike_station_information.json",overwrite=True)

    # Ορισμός χρονοδιαγράμματος (κάθε 5 λεπτά)
    (schedule.every(5).minutes.do(job))

    # Εκτέλεση
    while True:
        schedule.run_pending()
        time.sleep(1)

