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

def extract_data():
    try:
        data = supabase.table("airlines").select("*").execute()
        return data
    except APIError as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    result = extract_data()
    if result:
        print(result)
    else:
        print("Failed to extract data.")