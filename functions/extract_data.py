from supabase import create_client, Client
from dotenv import load_dotenv
from postgrest.exceptions import APIError
import json
import os
from datetime import datetime

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_PROJECT_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase credentials.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#Main function to get the airlines and its related flight routes and schedules
def get_airlines(airline_number:str=""):
    try:
        if airline_number:
            flight_routes_data = supabase.table("flight_routes").select("flight_routes.flight_number, flight_routes.departure_city, flight_routes.arrival_city, flight_routes.route_id, flight_routes.airline_id, flight_schedules(schedule_id, departure_time, arrival_time)").eq("flight_number", airline_number).execute()
        else:
            flight_routes_data = supabase.table("flight_routes").select("flight_routes.flight_number, flight_routes.departure_city, flight_routes.arrival_city, flight_routes.route_id, flight_routes.airline_id, flight_schedules(schedule_id, departure_time, arrival_time)").execute()
        
        data_list = flight_routes_data.data
        
        return data_list
    except APIError as e:
        print(f"An error occurred: {e}")
        return None

#Testing to ensure that the data is being utilized by DAG and saving it to Supabase
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

#A custom logging function to log the DAG execution
def log_dag_execution(dag_id, status, task_id=None, message=None, additional_data=None):
    try:
        log_entry = {
            'dag_id': dag_id,
            'status': status,
            'task_id': task_id,
            'message': message,
            'additional_data': json.dumps(additional_data) if additional_data else None,
            'execution_time': datetime.utcnow().isoformat()
        }
        result = supabase.table('dag_logs').insert(log_entry).execute()
        print(f"Logged DAG execution: {dag_id} - {status}")
        return result
    except APIError as e:
        print(f"An error occurred while logging DAG execution: {e}")
        return None