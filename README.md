# PII-deidentifier

## Description
This is a utility script that deidentifies sensitive information in Excel and CSV files. The utility uses Azure Text Analytics API to perform PII detection in textual data and pandas for data manipulation.

## Features
- Deidentify names, emails, locations, rooms, and phone numbers in Excel files.
- Can use for both csv and excel files, and then deidentify sensitive data.
- Uses Azure Text Analytics API to identify PII entities in specified textual columns.

## Prerequisites
1. Python 3.x
2. pandas
3. Azure Text Analytics API credentials
4. Python packages: azure.core, azure.ai.textanalytics

You can install the required Python packages using pip:
```bash
pip install pandas azure-core azure-ai-textanalytics
```
## How to Use

### Setup Azure Credentials
Replace the key and endpoint variables in the script with your Azure Text Analytics API credentials:
```python
key = "your_azure_key_here"
endpoint = "your_azure_endpoint_here"
```

### Function Calls
To deidentify an Excel file:
```python
deidentify_excel("your_excel_file.xlsx")
```

To deidentify a CSV file:
```python
deidentify_csv("your_csv_file.csv")
```

### Specify Columns to Deidentify
You can specify which columns to deidentify by editing these lists:
```python
workers_to_deidentify = ['assigned_to', ...]
users_to_deidentify = ['caller_id', ...]
emails_to_deidentify = ['sys_created_by', ...]
```
And so on for locations, rooms, and phone numbers.

### Run the Script
Run the Python script. A new Excel or CSV file with deidentified data will be created in the same directory.

## Note:
Code is yet to be modularized and tested. Potential updates:
```python
def deidentify_column(df, column_name, mapping_dict, identifier_prefix):
    for idx, cell_value in df[column_name].items():
        if pd.isnull(cell_value):
            continue
        values = [value.strip() for value in str(cell_value).split(',')]
        for value in values:
            if value not in mapping_dict:
                identifier = f"{identifier_prefix}{str(len(mapping_dict) + 1).zfill(3)}"
                mapping_dict[value] = identifier
            df.at[idx, column_name] = df.at[idx, column_name].replace(value, mapping_dict[value])

def deidentify_excel(file_path):
    df = pd.read_excel(file_path)
    
    # Define mapping dictionaries
    name_mapping = {}
    user_mapping = {}
    email_mapping = {}
    loc_mapping = {}
    room_mapping = {}
    
    # Define columns to deidentify
    config = {
        "workers": ['assigned_to', 'opened_by', 'u_assignee_history'],
        "users": ['caller_id', 'u_customer'],
        "emails": ['sys_created_by', 'sys_updated_by'],
        "locations": ['location'],
        "rooms": ['u_room'],
        "phones": ['u_phone'],
    }

    # Deidentify worker, user, and other columns
    for identifier_prefix, columns in config.items():
        mapping_dict = locals().get(f"{identifier_prefix}_mapping")
        for column in columns:
            deidentify_column(df, column, mapping_dict, identifier_prefix.capitalize())

    df.to_excel(f"deidentified_{file_path}", index=False)
```










