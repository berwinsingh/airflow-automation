from supabase import create_client, Client
from dotenv import load_dotenv
from postgrest.exceptions import APIError
import os

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_airlines(airline_number:str=""):
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

result = get_airlines()
if result:
    print(f"Total routes: {len(result)}")
else:
    print("Failed to retrieve airline data.")