import schedule
import time
import requests
import json

def fetch_simple_weather(lat, lon, api_key):
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        return None

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

def save_weather_data(data, filename="weather_data.json"):
    if data:
        with open(filename, "a") as file:
            json.dump(data, file)
            file.write("\n")

def job():
    lat, lon = 40.7128, -74.006  # Νέα Υόρκη
    api_key = "8104b06580b72d9308c015a06acc4fe6"  
    weather_data = fetch_simple_weather(lat, lon, api_key)
    processed_data = preprocess_weather_data(weather_data)
    if processed_data:
        save_weather_data(processed_data)
        print("Weather data saved:", processed_data)

# Ορισμός χρονοδιαγράμματος (κάθε 5 λεπτά)
schedule.every(5).minutes.do(job)

# Εκτέλεση
while True:
    schedule.run_pending()
    time.sleep(1)
