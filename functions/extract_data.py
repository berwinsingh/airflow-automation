from supabase import create_client, Client
from dotenv import load_dotenv
from postgrest.exceptions import APIError
import json
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_airlines(airline_number:str=""):
    print("Airlines API called")
    try:
        if airline_number:
            flight_routes_data = supabase.table("flight_routes").select("*").eq("flight_number", airline_number).execute()
        else:
            flight_routes_data = supabase.table("flight_routes").select("*").execute()
        
        data_list = flight_routes_data.data

        for route in data_list:
            print(f"Flight Number: {route['flight_number']}")
            print(f"Departure: {route['departure_city']}")
            print(f"Arrival: {route['arrival_city']}")
            print("---")
        
        return data_list
    except APIError as e:
        print(f"An error occurred: {e}")
        return None

def save_to_supabase(data, **context):
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            ti = context['ti']
            data = ti.xcom_pull(task_ids='get_airlines_task')
    
    if not isinstance(data, list):
        data = [data]
    
    try:
        formatted_data = []
        for route in data:
            formatted_data.append({
                'flight_number': route.get('flight_number'),
                'departure_city': route.get('departure_city'),
                'arrival_city': route.get('arrival_city'),
            })
        
        if not formatted_data:
            print("No valid data to save")
            return None
        
        result = supabase.table('airline_data').insert(formatted_data).execute()
        print(f"Saved {len(formatted_data)} records to Supabase")
        return result
    except APIError as e:
        print(f"An error occurred while saving to Supabase: {e}")
        return None