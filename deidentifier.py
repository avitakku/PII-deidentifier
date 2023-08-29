import pandas as pd
import csv
import os
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient

# Azure Text Analytics API credentials
key = ""
endpoint = ""

# Create a Text Analytics client
# Authenticate the client using your key and endpoint 
def authenticate_client():
    ta_credential = AzureKeyCredential(key)
    text_analytics_client = TextAnalyticsClient(
            endpoint=endpoint, 
            credential=ta_credential)
    return text_analytics_client

client = authenticate_client()

def deidentify_excel(file_path):
    # Load the Excel file
    df = pd.read_excel(file_path)

    # Specify the columns to deidentify
    workers_to_deidentify = ['assigned_to', 'opened_by', 'u_assignee_history', 'u_point_of_contact', 'resolved_by']
    users_to_deidentify = ['caller_id', 'u_customer']
    emails_to_deidentify = ['sys_created_by', 'sys_updated_by']
    loc_to_deidentify = ['location']
    room_to_deidentify = ['u_room']
    phone_to_deidentify = ['u_phone']
    
    # Create a dictionaries to store the mappings between original items and deidentified identifiers
    name_mapping = {}
    user_mapping = {}
    email_mapping = {}
    loc_mapping = {}
    room_mapping = {}


    if workers_to_deidentify:
        # Iterate over each worker column to deidentify
        for column_name in workers_to_deidentify:
            # Iterate over each cell in the column
            for idx, cell_value in df[column_name].items():
                # Skip empty cells
                if pd.isnull(cell_value):
                    continue

                # Split multiple names separated by commas into a list
                names = [name.strip() for name in str(cell_value).split(',')]

                # Iterate over the names
                for name in names:
                    # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                    if name not in name_mapping:
                        identifier = f"Worker{str(len(name_mapping) + 1).zfill(3)}"
                        name_mapping[name] = identifier

                    # Replace the name with the corresponding identifier
                    df.at[idx, column_name] = df.at[idx, column_name].replace(name, name_mapping[name])

    if users_to_deidentify:
        # Iterate over each user column to deidentify
        for column_name in users_to_deidentify:
            # Iterate over each cell in the column
            for idx, cell_value in df[column_name].items():
                # Skip empty cells
                if pd.isnull(cell_value):
                    continue

                # Split multiple names separated by commas into a list
                users = [user.strip() for user in str(cell_value).split(',')]

                # Iterate over the names
                for user in users:
                    # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                    if user not in user_mapping:
                        identifier = f"User{str(len(user_mapping) + 1).zfill(3)}"
                        user_mapping[user] = identifier

                    # Replace the name with the corresponding identifier
                    df.at[idx, column_name] = df.at[idx, column_name].replace(user, user_mapping[user])

    if emails_to_deidentify:
        # Iterate over each email column to deidentify
        for column_name in emails_to_deidentify:
            # Iterate over each cell in the column
            for idx, cell_value in df[column_name].items():
                # Skip empty cells
                if pd.isnull(cell_value):
                    continue

                # Split multiple names separated by commas into a list
                emails = [email.strip() for email in str(cell_value).split(',')]

                # Iterate over the names
                for email in emails:
                    # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                    if email not in email_mapping:
                        identifier = f"email{str(len(email_mapping)+1).zfill(3)}@example.com"
                        email_mapping[email] = identifier

                    # Replace the name with the corresponding identifier
                    df.at[idx, column_name] = df.at[idx, column_name].replace(email, email_mapping[email])

    if loc_to_deidentify:
        # Iterate over each location column to deidentify
        for column_name in loc_to_deidentify:
            # Iterate over each cell in the column
            for idx, cell_value in df[column_name].items():
                # Skip empty cells
                if pd.isnull(cell_value):
                    continue

                # Split multiple names separated by commas into a list
                locs = [loc.strip() for loc in str(cell_value).split(',')]

                # Iterate over the names
                for loc in locs:
                    # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                    if loc not in loc_mapping:
                        identifier = f"Loc{str(len(loc_mapping) + 1).zfill(3)}"
                        loc_mapping[loc] = identifier

                    # Replace the name with the corresponding identifier
                    df.at[idx, column_name] = df.at[idx, column_name].replace(loc, loc_mapping[loc])

    if room_to_deidentify:
        # Iterate over each room column to deidentify
        for column_name in room_to_deidentify:
            for idx, room_number in df[column_name].items():
                # Skip empty cells
                if pd.isnull(cell_value):
                    continue

                # If the room number/letter is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                if room_number not in room_mapping:
                    identifier = f"Room{str(len(room_mapping) + 1).zfill(3)}"
                    room_mapping[room_number] = identifier

                # Replace the room number/letter with the corresponding identifier
                df.at[idx, column_name] = room_mapping[room_number]

    if phone_to_deidentify:
        # Iterate over each phone num column to deidentify
        for column_name in phone_to_deidentify:
            for idx, phone_number in df[column_name].items():
                # Skip empty cells
                if pd.isnull(phone_number):
                    continue

                # Replace the phone number with a generic value
                df.at[idx, column_name] = 'xxx xxx xxxx'
        
    # Save the modified Excel file
    df.to_excel(f"deidentified_{file_path}", index=False)

def deidentify_csv(file_path):
        read_file = pd.read_csv (file_path, encoding='cp1252')
        read_file.to_excel (f"{file_path}.xlsx", engine='xlsxwriter')
        
        # Load the Excel file
        df = pd.read_excel(f"{file_path}.xlsx")

        # Specify the columns to deidentify
        """workers_to_deidentify = ['assigned_to', 'opened_by', 'u_assignee_history', 'u_point_of_contact', 'resolved_by']
        users_to_deidentify = ['caller_id', 'u_customer']
        emails_to_deidentify = ['sys_created_by', 'sys_updated_by']
        loc_to_deidentify = ['location']
        room_to_deidentify = ['u_room']
        phone_to_deidentify = ['u_phone']"""

        #workers_to_deidentify = ['assigned_to']
        #users_to_deidentify = ['ref_incident.caller_id']
        emails_to_deidentify = []
        loc_to_deidentify = []
        room_to_deidentify = []
        phone_to_deidentify = []
        ai_deidentify = ['description']
        
        # Create a dictionaries to store the mappings between original items and deidentified identifiers
        name_mapping = {}
        user_mapping = {}
        email_mapping = {}
        loc_mapping = {}
        room_mapping = {}


        if workers_to_deidentify:
            # Iterate over each worker column to deidentify
            for column_name in workers_to_deidentify:
                # Iterate over each cell in the column
                for idx, cell_value in df[column_name].items():
                    # Skip empty cells
                    if pd.isnull(cell_value):
                        continue

                    # Split multiple names separated by commas into a list
                    names = [name.strip() for name in str(cell_value).split(',')]

                    # Iterate over the names
                    for name in names:
                        # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                        if name not in name_mapping:
                            identifier = f"Worker{str(len(name_mapping) + 1).zfill(3)}"
                            name_mapping[name] = identifier

                        # Replace the name with the corresponding identifier
                        df.at[idx, column_name] = df.at[idx, column_name].replace(name, name_mapping[name])

        if users_to_deidentify:
            # Iterate over each user column to deidentify
            for column_name in users_to_deidentify:
                # Iterate over each cell in the column
                for idx, cell_value in df[column_name].items():
                    # Skip empty cells
                    if pd.isnull(cell_value):
                        continue

                    # Split multiple names separated by commas into a list
                    users = [user.strip() for user in str(cell_value).split(',')]

                    # Iterate over the names
                    for user in users:
                        # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                        if user not in user_mapping:
                            identifier = f"User{str(len(user_mapping) + 1).zfill(3)}"
                            user_mapping[user] = identifier

                        # Replace the name with the corresponding identifier
                        df.at[idx, column_name] = df.at[idx, column_name].replace(user, user_mapping[user])

        if emails_to_deidentify:
            # Iterate over each email column to deidentify
            for column_name in emails_to_deidentify:
                # Iterate over each cell in the column
                for idx, cell_value in df[column_name].items():
                    # Skip empty cells
                    if pd.isnull(cell_value):
                        continue

                    # Split multiple names separated by commas into a list
                    emails = [email.strip() for email in str(cell_value).split(',')]

                    # Iterate over the names
                    for email in emails:
                        if email == 'admin':
                            continue
                        # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                        elif email not in email_mapping:
                            identifier = f"email{str(len(email_mapping)+1).zfill(3)}@example.com"
                            email_mapping[email] = identifier

                        # Replace the name with the corresponding identifier
                        df.at[idx, column_name] = df.at[idx, column_name].replace(email, email_mapping[email])

        if loc_to_deidentify:
            # Iterate over each location column to deidentify
            for column_name in loc_to_deidentify:
                # Iterate over each cell in the column
                for idx, cell_value in df[column_name].items():
                    # Skip empty cells
                    if pd.isnull(cell_value):
                        continue

                    # Split multiple names separated by commas into a list
                    locs = [loc.strip() for loc in str(cell_value).split(',')]

                    # Iterate over the names
                    for loc in locs:
                        # If the name is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                        if loc not in loc_mapping:
                            identifier = f"Loc{str(len(loc_mapping) + 1).zfill(3)}"
                            loc_mapping[loc] = identifier

                        # Replace the name with the corresponding identifier
                        df.at[idx, column_name] = df.at[idx, column_name].replace(loc, loc_mapping[loc])

        if room_to_deidentify:
            # Iterate over each room column to deidentify
            for column_name in room_to_deidentify:
                for idx, room_number in df[column_name].items():
                    # Skip empty cells
                    if pd.isnull(cell_value):
                        continue

                    # If the room number/letter is not in the mapping dictionary, generate a new identifier and add it to the dictionary
                    if room_number not in room_mapping:
                        identifier = f"Room{str(len(room_mapping) + 1).zfill(3)}"
                        room_mapping[room_number] = identifier

                    # Replace the room number/letter with the corresponding identifier
                    df.at[idx, column_name] = room_mapping[room_number]

        if phone_to_deidentify:
            # Iterate over each phone num column to deidentify
            for column_name in phone_to_deidentify:
                for idx, phone_number in df[column_name].items():
                    # Skip empty cells
                    if pd.isnull(phone_number):
                        continue

                    # Replace the phone number with a generic value
                    df.at[idx, column_name] = 'xxx xxx xxxx'

        if ai_deidentify:
            for column_name in ai_deidentify:
                for value in df[column_name]:
                    text = str(value)
                    pii_entities = extract_pii_from_text(text)

                    if pii_entities:
                        print(f"PII entities found in column '{column_name}':")
                        for entity in pii_entities:
                            print(f"- Type: {entity.category}, Text: {entity.text}, Confidence score: {entity.confidence_score}")                    
            
        # Save the modified Excel file
        #df.to_excel(f"deidentified_{file_path}", index=False)
        df.to_csv (f"deidentified_{file_path}", 
                  index = None,
                  header=True)

# Usage example
#file_path = "NINDS_Incidents_05012022_10312022.xlsx"
#file_path = "NINDS_1.csv"
#deidentify_excel(file_path)
#deidentify_csv(file_path)
