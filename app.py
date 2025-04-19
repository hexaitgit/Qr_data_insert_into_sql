# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 17:25:49 2025

@author: star26
"""

import requests
import json
import pyodbc
from datetime import datetime

# Function to convert date string to datetime object
def convert_to_datetime(date_string):
    try:
        # Try to convert the date string to a datetime object in the format 'dd.MM.yyyy'
        return datetime.strptime(date_string, "%d.%m.%Y")
    except ValueError:
        print(f"Invalid date format: {date_string}")
        return None

# Function to remove metadata and store data
def process_and_store_data_in_sql(json_data, server, database, username, password):
    try:
        # Connect to SQL Server
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )
        cursor = conn.cursor()
        
        # Extract the 'results' from the JSON, which contains the relevant data
        results = json_data.get("d", {}).get("results", [])
        
        # Loop through each record in 'results' and insert it into the database
        for record in results:
            # Remove metadata and extract necessary fields
            srno = record.get("Srno", None)
            barcode = record.get("Barcode", None)
            lidcode = record.get("Lidcode", None)
            boxcode = record.get("Boxcode", None)
            invoice = record.get("Invoice", None)
            prefix = record.get("Prefix", None)
            points = float(record.get("Points", 0.0))  # Convert points to float
            cash = float(record.get("Cash", 0.0))  # Convert cash to float
            synced = int(record.get("Synced", 0))  # Convert synced to integer
            invoice_date = record.get("InvoiceDate", None)
            
            # Convert invoice_date to proper datetime format
            invoice_date = convert_to_datetime(invoice_date) if invoice_date else None
            
            # SQL Insert Query
            query = """
            INSERT INTO qrdata1 (Srno, Barcode, Lidcode, Boxcode, Invoice, Prefix, Points, Cash, Synced, InvoiceDate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(query, srno, barcode, lidcode, boxcode, invoice, prefix, points, cash, synced, invoice_date)
        
        conn.commit()  # Commit changes
        print("Data inserted successfully.")
    except Exception as e:
        print(f"An error occurred while storing data in SQL: {e}")
    finally:
        conn.close()

# URL to send the GET request
url = "https://cpi-nonprod-u7xwl3ag.it-cpi026-rt.cfapps.eu10-002.hana.ondemand.com/http/sendqrdata"

# Credentials
username = "S0026376022"
password = "Hexa@1000#"
# Headers with INVOICEDATE
headers = {
    "INVOICEDATE": "16.04.2025"
}

try:
    # Send GET request with headers and Basic Authentication
    response = requests.get(url,headers=headers,auth=(username, password))
    

    # Check if the request was successful
    if response.status_code == 200:
        try:
            # Try to parse the JSON response
            data_as_variable = response.json()  # Store the JSON response
            
            print("Data successfully fetched and stored as a variable:")
            print(json.dumps(data_as_variable, indent=4))  # Print formatted JSON

            # Process and store the data in SQL
            server ="192.168.172.100"
            database ="CILRTL"
            username ="sa"
            password= "sq"
            
            process_and_store_data_in_sql(data_as_variable, server, database, username, password)

        except json.JSONDecodeError:
            # If decoding JSON fails, store the raw response as a variable
            raw_data_as_variable = response.text
            
            print("Failed to decode JSON. Storing raw response as a variable:")
            print(raw_data_as_variable)
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        response_content = response.text  # Store the error response content in a variable
        print("Response content:")
        print(response_content)

except Exception as e:
    print(f"An error occurred: {e}")
