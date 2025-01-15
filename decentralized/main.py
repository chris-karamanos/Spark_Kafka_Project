import schedule
import time
import requests
import json

# Fetching

def fetch_simple_weather(lat, lon, api_key):
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
    if not data:
        return None
    return {
        "timestamp": data.get("dt"),
        "temperature": data["main"].get("temp"),
        "precipitation": data.get("rain", {}).get("1h", 0),  # Βροχόπτωση τελευταίας ώρας
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

def save_weather_data(data, filename="weather_data.json"):
    if data:
        with open(filename, "a") as file:
            json.dump(data, file)
            file.write("\n")

def save_bike_station_data(data, filename="bike_station_data.json"):
    if data:
        with open(filename, "a") as file:
            json.dump(data, file)
            file.write("\n")

def job():
    # Weather
    lat, lon = 40.7128, -74.006  # Νέα Υόρκη
    api_key = "8104b06580b72d9308c015a06acc4fe6"
    weather_data = fetch_simple_weather(lat, lon, api_key)
    processed_data = preprocess_weather_data(weather_data)
    if processed_data:
        save_weather_data(processed_data)
        print("Weather data saved:", processed_data)

    # Bikes

    # Endpoint 2
    bike_station_status,last_updated = fetch_bike_station_status()

    # Combine
    merged_data = merge_station_data(bike_station_information, bike_station_status, last_updated)

    if bike_station_information and bike_station_status and merged_data:
        save_bike_station_data(merged_data)
        print("Bike station data saved timestamp : ", merged_data["timestamp"] , " with " , len(merged_data["stations"]) , " stations")

if __name__ == '__main__':
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