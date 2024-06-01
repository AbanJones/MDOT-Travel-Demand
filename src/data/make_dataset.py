import requests
import time
import csv
import os

def get_local_businesses(api_key, location, radius, place_type, limit=1):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    businesses = []
    params = {
        'location': location,
        'radius': radius,
        'type': place_type,  # Changed from 'keyword' to 'type'
        'key': api_key
    }
    
    while len(businesses) < limit:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            businesses.extend(results[:limit - len(businesses)])
            next_page_token = data.get('next_page_token')
            if next_page_token and len(businesses) < limit:
                params = {'pagetoken': next_page_token, 'key': api_key}
                time.sleep(2)
            else:
                break
        else:
            print("Failed to fetch the businesses:", response.json().get("error_message", "No error message"))
            break
    
    return businesses[:limit]

def get_businesses_place_ids(businesses):
    place_ids = []
    for business in businesses:
        place_id = business.get('place_id')
        place_ids.append(place_id)
    return place_ids

def check_nearby_stops(api_key, latitude, longitude, place_type, radius=500):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        'location': f"{latitude},{longitude}",
        'radius': radius,
        'type': place_type,
        'key': api_key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return len(data.get('results', [])) > 0
    else:
        print(f"Failed to fetch nearby {place_type} for location {latitude},{longitude}")
        return False

def fetch_and_output_data(api_key, place_ids, csv_filename):
    headers = [
        "area_id", "pg_zone", "moco_zone", "landuse", "county", "address", "name_of_business_or_area",
        "types", "latitude", "longitude", # Added types column
        "business_size_micro(<100_employees)", 
        "business_size_small(100-1500_employees)", "business_size_medium(1500-2000_employees)", 
        "business_size_large(2000+_employees)", "parking_lot","bus_stop_within_500m", "train_stop_within_500m", "side_walk_on_parcel", "parking_lot_size_small(<100_spaces)",
        "parking_lot_size_medium(100-200_spaces)", "parking_lot_size_large(200+_spaces)", "street_lights", "bike_paths_or_lane"
    ]
    
    file_exists = os.path.isfile(csv_filename)
    
    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        
        if not file_exists:
            writer.writeheader()
        
        for place_id in place_ids:
            try:
                url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={api_key}"
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json().get('result', {})
                    coordinates = data.get('geometry', {}).get('location', {})
                    bus_stop_nearby = check_nearby_stops(api_key, coordinates.get('lat'), coordinates.get('lng'), 'transit_station', 500)
                    train_stop_nearby = check_nearby_stops(api_key, coordinates.get('lat'), coordinates.get('lng'), 'subway_station', 500)
                    
                    row = {
                        "county": "Montgomery",  # Example static value
                        "address": data.get('formatted_address'),
                        "name_of_business_or_area": data.get('name'),
                        "types": data.get('types', [None])[0] if data.get('types') else None,
                        "latitude": coordinates.get('lat') if coordinates else None,
                        "longitude": coordinates.get('lng') if coordinates else None,
                        "bus_stop_within_500m": bus_stop_nearby,
                        "train_stop_within_500m": train_stop_nearby,
                    }
                    
                    writer.writerow(row)
            except Exception as e:
                print(f"Failed to fetch data for {place_id}: {e}")

def read_place_types(file_path):
    with open(file_path, 'r') as file:
        place_types = [line.strip() for line in file.readlines()]
    return place_types

def process_queries_for_place_types(api_key, place_types, location, radius, limit, csv_filename="output.csv"):
    for place_type in place_types:
        print(f"Processing query for place type: {place_type}")
        
        businesses = get_local_businesses(api_key, location, radius, place_type, limit)
        if businesses:
            place_ids = get_businesses_place_ids(businesses)
            fetch_and_output_data(api_key, place_ids, csv_filename)
        else:
            print(f"No businesses found for place type: {place_type}")
        time.sleep(1)

# Example usage adjustments
api_key = ''
location = '39.004064,-77.149058'  # Adjust as needed
radius = 6000  # Adjust as needed
limit = 1
place_types_file = 'place_types.txt'  # Enter filename here
place_types = read_place_types(place_types_file)
csv_filename = "all_places_data.csv"

# Now, process all place types from the file and write to a single CSV
process_queries_for_place_types(api_key, place_types, location, radius, limit, csv_filename)
