from datetime import datetime, timedelta, timezone
import requests
from azure.storage.blob import BlobServiceClient,ContainerClient
from azure.data.tables import UpdateMode
import os
import io
import json
import logging
import tempfile
import csv
import uuid
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableServiceClient, TableClient, TableEntity
import pandas as pd 
from io import StringIO

os.environ['http_proxy'] = "http://usfraaa-proxy.americas.nsn-net.net:8080"
os.environ['https_proxy'] = "http://usfraaa-proxy.americas.nsn-net.net:8080"
os.environ['HTTP_PROXY'] = "http://usfraaa-proxy.americas.nsn-net.net:8080"
os.environ['HTTPS_PROXY'] = "http://usfraaa-proxy.americas.nsn-net.net:8080"

#file_path = 
tempFilePath = tempfile.gettempdir()

clientid = "q95vErqG1LhizuWUFDuDqihFvHQL4liv"
clientsecret = "Zx1nZKqMqCQZePO0tWWWbUBlmZrDGQOC"
baseurl = "https://eu-apigw.central.arubanetworks.com/"
connection_string = 'DefaultEndpointsProtocol=https;AccountName=automationaakash9980;AccountKey=wYaTYjGbFjlUTS6U03FWBi7UTNHO+QCOQ53usvk+biO428gqQwCY1pXJrGXt9GCKW4LFPGE/nekp+AStaAbZtw==;BlobEndpoint=https://automationaakash9980.blob.core.windows.net/;QueueEndpoint=https://automationaakash9980.queue.core.windows.net/;TableEndpoint=https://automationaakash9980.table.core.windows.net/;FileEndpoint=https://automationaakash9980.file.core.windows.net/;'
table_service_client = TableServiceClient.from_connection_string(connection_string)
account_name = "automationaakash9980"
container_name = "arubaeventstoken"
container_name_failure = "aruba-failure"
blobFile = "token.json"
blobfailure = "APAC-INCHAX01_2024-08-27.csv"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blobFile)
blob_client_failure = blob_service_client.get_blob_client(container=container_name_failure, blob=blobfailure)
downloaded_blob = blob_client_failure.download_blob().content_as_text()
table_client = table_service_client.get_table_client("arubaeventstable")
fileName = tempFilePath + "/" + "aruba-eu-central-refresh-token.json"
client = ContainerClient.from_connection_string(connection_string, container_name)

# Use pandas to read the CSV data into a DataFrame
csv_data = StringIO(downloaded_blob)
print("downloaded_blob",downloaded_blob)
df = pd.read_csv(csv_data)
print(df.head())
print("this is csv file")
print(df)


# Group by 'bssid' and 'description' and calculate the count
grouped_df = df.groupby(['bssid', 'event_type']).agg({
    'group_name': 'first',  # Take the first CampusID
    'timestamp': 'first',  # Take the first timestamp
    'bssid': 'first',  # Keep the bssid (optional, could use reset_index)
    'event_type': 'first'  # Keep the description (optional, could use reset_index)
}).reset_index(drop=True)

# Add a 'Count' column
grouped_df['Count'] = df.groupby(['bssid', 'event_type']).size().values

# Generate the RowKey (Date + UUID)
grouped_df['RowKey'] = grouped_df['timestamp']#.apply(lambda x: x.split('T')[0]) + '_' + grouped_df.apply(lambda _: str(uuid.uuid4()), axis=1)

# Rename columns as per the requirement
grouped_df.rename(columns={
    'group_name': 'Primary',
    'timestamp': 'Timestamp',
    'event_type': 'event_type',
    'bssid': 'BSSID'
}, inplace=True)

# Reorder columns
grouped_df = grouped_df[['Primary', 'RowKey', 'Timestamp', 'event_type', 'Count', 'BSSID']]

print(grouped_df)


# disable warnings
requests.packages.urllib3.disable_warnings()


# Updating the information into the table

def update_table_entry(table_name, entry):
    try:
        table_client = table_service_client.get_table_client(table_name=table_name)
        table_client.upsert_entity(entity=entry, mode=UpdateMode.REPLACE)
        logging.info(f"Entry updated in table {table_name}: {entry}")
    except Exception as ex:
        logging.error(f"Error updating entry in table {table_name}: {ex}")



def upload_file(sourceFile, blobFile, conClient):
    logging.info(
        "upload in progress for sourceFile:{0} and blobFile:{1}".format(
            sourceFile, blobFile
        )
    )
    blobClient = conClient.get_blob_client(blobFile)
    try:
        with open(sourceFile, "rb") as data:
            blobClient.upload_blob(data, overwrite=True)
            logging.info(f"{sourceFile} uploaded successfully to blob storage")
        os.remove(sourceFile)
        return 0
    except Exception as ex:
        logging.info(ex.__str__)
        return 1


def get_last_used_refresh_token():
    logging.info("get_last_used_refresh_token")
    try:
        blobClient = client.get_blob_client(blobFile)
        with open(fileName, "wb") as my_blob:
            blob_data = blobClient.download_blob()
            blob_data.readinto(my_blob)
        # extract refresh_token from json
        with open(fileName, "r") as json_file:
            data = json.load(json_file)
            refresh_token = data["refresh_token"]
            #print("Refresh token:", refresh_token)  # Ensure the token is printed
        return refresh_token
    except Exception as ex:
        logging.info(f"JSON decoding error: {ex}")
        return 1

# Example usage to ensure print statement executes
refresh_token = get_last_used_refresh_token()


#campus_ids = set(["AEDUBX06", "BRRECX02", "PLWROX01", "PLKRAX01", "N_KUNX01", "SEKISX02", "INCHAX01"])

campus_ids =[]

#read   
def print_table_details(table_name):
    try:
        table_client = table_service_client.get_table_client(table_name=table_name)
        entities = table_client.list_entities()
        for entity in entities:
            #print(entity)
            campus_ids.append(entity['RowKey'])
        #print (campus_ids)
    except ResourceNotFoundError:
        logging.error(f"Table {table_name} not found.")
    except Exception as ex:
        logging.error(f"Error retrieving details from table {table_name}: {ex}")





def filter_campus_ids(json_data, campus_ids):

    ans = []
    for item in json_data:
        if "-" in item.get('group_name'):
            group_name = item.get('group_name').split("-")[1]
        else:
            group_name = item.get('group_name')
        
        if group_name in campus_ids:
            ans.append(item)

    return ans

def generate_row_key():
    date_str = datetime.now().strftime("%Y-%m-%d")
    unique_id = str(uuid.uuid4())
    return f"{date_str}_{unique_id}"

# Function to handle inserting or updating the entity
def process_row(row, table_client):
    # Check if an entry with the same SSID and Description already exists
    filter_query = f"PartitionKey eq '{row['CampusID']}' and SSID eq '{row['ssid']}' and event_type eq '{row['event_type']}'"
    entities = table_client.query_entities(filter=filter_query)

    # If an entry exists, update the Count
    updated = False
    for entity in entities:
        entity["Count"] += 1
        table_client.update_entity(entity=entity)
        updated = True
        break  # Assuming SSID + Description is unique, so break after updating

    # If no matching entry is found, insert a new row
    if not updated:
        new_entity = {
            "PartitionKey": row["CampusID"],
            "RowKey": generate_row_key(),
            "Timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": row["event_type"],
            "Count": 1,
            "SSID": row["ssid"]
        }
        
        table_client.create_entity(entity=new_entity)


def get_events(access_token,offset,start_time,end_time):
    try:
        print("entering try and except")
        filename = "aruba-eu-central-events.json"
       
        url = baseurl + "/monitoring/v2/events?limit=1000&offset="+str(offset)+"&from_timestamp="+str(start_time)+"&to_timestamp="+str(end_time)+"&level=negative"
        payload = {}
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + access_token,
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        json_data = response.json()
        print("COUNT:") 
        #print(json_data)
        print_table_details("arubacampusidlist")
        filtered_data = filter_campus_ids(json_data["events"], campus_ids)
        #print(len(filtered_data))
        global total
        total += len(filtered_data)
        filtered_json_string = json.dumps(filtered_data)

        print(len(filtered_data))
        #print(filtered_json_string)
        json_object = json.loads(filtered_json_string)
        #print_reason(json_object)
        #group_data = {}
        global group_data
        global written_events
        for event in json_object:
            group_name = event["group_name"]
            #print(event)
            
            # Generate a unique key for each event to avoid duplicates
            event_key = (group_name, event["event_uuid"])
            
            if event_key in written_events:
                print("already exists")
                break  # Skip if the event has already been processed
            
            written_events[event_key] = True
            
            if group_name not in group_data:
                group_data[group_name] = []
            
            # Ensure all events have the 'events_details' field
            if 'events_details' not in event:
                event['events_details'] = ''

            group_data[group_name].append(event)
            #print(group_data)
        
        # Handle tempfile creation and CSV writing
        #temp_files = {}
        global temp_files
        for group_name, events in group_data.items():
            date_str = datetime.now().strftime("%Y-%m-%d")
            filename = f"{group_name}_{date_str}.csv"
            
            # Check if file already exists in the temp directory
            temp_dir = tempfile.gettempdir()
            filepath = os.path.join(temp_dir, filename)

            existing_event_uuids = set()
        
            # If the file exists, read the event_uuid to avoid duplicates
            if os.path.isfile(filepath):
                with open(filepath, mode='r', newline='') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        existing_event_uuids.add(row["event_uuid"])
            
            # Write new events to the file, avoiding duplicates
            with open(filepath, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=events[0].keys())
                
                if not existing_event_uuids:
                    writer.writeheader()
                
                for event in events:
                    if event["event_uuid"] not in existing_event_uuids:
                        print(event)
                        print("event displayed")
                        writer.writerow(event)
                        print("success")
                
            temp_files[group_name] = filepath

        #return json_data

                
            #with open(filepath, mode='r', newline='') as file:
             #   reader = csv.DictReader(file)
              #  for row in reader:
               #     process_row(row, table_client)
        return json_data


# Send the filtered JSON string to Power Automate
        sendtopowerautomate(filename, filtered_json_string) 
        #json_string = json.dumps(json_data)
        #sendtopowerautomate(filename, json_string)
        return 0
    except Exception as ex:
        print("ERROR OCCURED WITH DATA")
        logging.info(ex.__str__)
        print(ex)

def upload_df_to_table(df, table_client):
    for index, row in df.iterrows():
        # Generate a unique RowKey using UUID and current UTC time
        row_key = f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_{str(uuid.uuid4())}"
        
        entity = {
            'PartitionKey': row['Primary'],
            'RowKey': row_key,
            'event_type': row['event_type'],
            'Count': row['Count'],
            'BSSID': row['BSSID']
        }

        # Insert entity into Azure Table
        table_client.create_entity(entity=entity)

# Upload DataFrame
upload_df_to_table(grouped_df, table_client)

print("Data uploaded successfully to Azure Table Storage.")
def print_reason(data_frame):

    """
    Processes a list of JSON objects, filters for 'negative' events, and updates data storage.
    
    Args:
    - json_data: List of JSON objects containing event data.

    Global Variables:
    - i: Counter for naming blobs uploaded to Azure Blob Storage.
    - j: Counter for counting events processed.
    """
    
    global i, j  # Access global variables i and j within the function
    
    # Iterate through each item in the JSON data list
    for item in data_frame:
        print("How Many Events?", j)  # Print the current event count
        print("What is the level of this item? ", item.get('level'))  # Print the 'level' of the current item
        j += 1  # Increment event count
        
        # Check if the event is of 'negative' level
        if True:
            print("The information about this json data is: ", "\n", item)
            
            id = item.get('group_name')  # Get the group name (or ID) from the item
            description = item.get('event_type')  # Get the event type
            unique_id = str(uuid.uuid4())
            times = epoch_to_iso8601(item['timestamp']) + "  ,"+unique_id # Convert the timestamp to ISO8601 format
            
            # Add the item to the negative_events dictionary
           
            bssid = item.get('ssid', 'NA')  # Get the BSSID or default to 'NA' if not present
            
            # Ensure bssid is 'NA' if it's an empty string
            if not bssid:
                bssid = 'NA'
            
            entry_arubaeventstable = {}
            x = find("arubaeventstable", id, times)  # Attempt to find an existing entry in the table
            

            if x is None:
                # If no existing entry, create a new entry
                print("New entry")
                entry_arubaeventstable = {
                    "PartitionKey": id,
                    "RowKey": times,
                    "Timestamp": 1,
                    "Counter": 1,
                    "description": description,
                    "Level": 'negative',
                    "SSID": bssid,
                }
            else:
                # If entry exists, update the counter and other information
                print("Repeated entry")
                print(x["Counter"])
                entry_arubaeventstable = {
                    "PartitionKey": id,
                    "RowKey": times,
                    "Timestamp": 1,
                    "Counter": x["Counter"] + 1,
                    "description": description,
                    "Level": 'negative',
                    "SSID": bssid,
                }
            
            # Update or insert the entry in the 'arubaeventstable' table
            update_table_entry("arubaeventstable", entry_arubaeventstable)
    
    # After processing all items, handle the negative events
    for id in negative_events:
        blob_name = id + " " + times  # Create a blob name using ID and timestamp
        blob_client = blob_service_client.get_blob_client(container=container_name_failure, blob=blob_name)
        json_data_failure = json.dumps(negative_events[id])  # Serialize negative events to JSON
        blob_client = blob_service_client.get_blob_client(container=container_name_failure, blob=blob_name + str(i) + ".csv")
        
        output = io.StringIO()  # Create an in-memory file-like object
        
        # Create a CSV writer and write the negative events data to it
        csv_writer = csv.DictWriter(output, fieldnames=negative_events[id][0].keys())
        csv_writer.writerows(negative_events[id])
        
        # Get the CSV content as a string and close the in-memory file
        csv_content = output.getvalue()
        output.close()
        
        # Output the CSV content to the console
        print(csv_content)
        i += 1  # Increment the blob counter
        
        # Upload the CSV content to Azure Blob Storage
        json_data_failure = ""
        for item in negative_events[id]:
            json_data_failure += json.dumps(item) + "\n"
        
        blob_client.upload_blob(csv_content, overwrite=True)  # Upload the CSV content to the blob
        print(f"JSON data has been written to blob {blob_name} in container {container_name_failure}")




def upload_to_azure_blob(temp_files):
    # Azure Blob Storage connection string
    #connection_string = "Your_Azure_Connection_String"
    #blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    #container_name = "aruba-failures"
    container_client = blob_service_client.get_container_client(container_name_failure)
    
    for group_name, filepath in temp_files.items():
        blob_name = os.path.basename(filepath)
        blob_client = container_client.get_blob_client(blob_name)
        
        with open(filepath, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        # Remove the temporary file
        os.remove(filepath)











def get_access_token():
    try:
        refresh_token = get_last_used_refresh_token()
        url = (
            str(baseurl)
            + "oauth2/token?client_id="
            + str(clientid)
            + "&client_secret="
            + str(clientsecret)
            + "&grant_type=refresh_token&refresh_token="
            + str(refresh_token)
        )
        payload = {}
        headers = {
            "Content-Type": "application/json",
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        # write response to json file
        with open(fileName, mode="w") as f:
            json.dump(response.json(), f)

        # upload response json
        upload_file(fileName, blobFile, client)
        access_token = response.json()["access_token"]
        logging.info(access_token)
        return access_token
    except Exception as ex:
        #print("logging.info2",type(ex),ex.__str__())
        logging.info(ex.__str__)
        return 1

def datetime_to_epoch(date_str, time_str):
    # Convert datetime string to Unix timestamp
    dt = datetime.strptime(date_str + " " + time_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())

 
# Get the current date and time
current_datetime = datetime.now()
 
# Subtract one day
previous_day_datetime = current_datetime - timedelta(days=1)
date_str = previous_day_datetime.strftime("%Y-%m-%d")
# Define time slots of 3 hours each
# time_slots = [
#     ("12:00:00", "13:59:59"),
#     ("14:00:00", "15:59:59"),
#     ("16:00:00", "17:59:59"),
#     ("18:00:00", "19:59:59"),
#     ("20:00:00", "21:59:59"),
#     ("22:00:00", "23:59:59")
# ]

time_slots = [
    ("12:00:00", "13:59:59")
]




#def epoch_to_time (date_str, time_str)
def epoch_to_iso8601(epoch_time):
    print(type(epoch_time))
    dt = datetime.fromtimestamp(epoch_time/1000.0 ,tz=timezone.utc)
    print(f"Converted datetime: {dt} from DT {epoch_time}")
    iso8601_string = dt.isoformat(timespec='microseconds') + 'Z'
    print("iso8601_string" , iso8601_string)
    return iso8601_string



total = 0
group_data = {}
temp_files = {}
written_events = {}
# Loop through each time slot
for start, end in time_slots:
    # Convert the start and end times to epoch
    start_time = datetime_to_epoch(date_str, start)
    end_time = datetime_to_epoch(date_str, end)
    
    # Initialize the offset
    offset = 0
    print("NEW TIME SLOT:", str(start) + " - " + str(end))
    
    while True:
        # Call the get_events function with the current time slot
        response = get_events(get_access_token(), offset, start_time, end_time)
        count = response['count']
        #total += count
        
        # Break the loop if the count is less than 1000
        if count < 1000:
            break
        
        # Increment the offset for the next batch
        offset += 1000
    #tart Time: 1721664000
#End Time: 1721707199
    
print("finished calling get_events")
print(temp_files)
upload_to_azure_blob(temp_files)
#The number of the events
#Token

print("Total number of events:", total)
print(len(group_data['APAC-INCHAX01']))
