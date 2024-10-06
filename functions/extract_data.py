from supabase import create_client, Client
import os
from dotenv import load_dotenv
from postgrest.exceptions import APIError

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials. Please check your .env file.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_airlines(airline_number:str=""):
    try:
        if airline_number:
            flight_routes_data = supabase.table("flight_routes").select("*").eq("airline_number", airline_number).execute()
        else:
            flight_routes_data = supabase.table("flight_routes").select("*").execute()
        
        # The data is in the 'data' attribute of the response
        data_list = flight_routes_data.data

        for route in data_list:
            route_id, airline_id, flight_number, departure_city, arrival_city = route
            print(f"Flight Number: {flight_number}")
            print(f"Departure: {departure_city}")
            print(f"Arrival: {arrival_city}")
            print("---")
        
        return data_list
    except APIError as e:
        print(f"An error occurred: {e}")
        return None

# Call the function and print the result
result = get_airlines()
if result:
    print(f"Total routes: {len(result)}")
else:
    print("Failed to retrieve airline data.")