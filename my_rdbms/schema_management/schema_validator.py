import json
import os
from datetime import datetime

# Define data type validation functions
def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def is_string(value):
    return isinstance(value, str)

def is_boolean(value):
    return isinstance(value, bool) or value in ["True", "False", "true", "false"]

def is_date(value):
    try:
        datetime.strptime(value, '%Y-%m-%d')
        return True
    except ValueError:
        return False

# Map data types to validation functions
validation_mapping = {
    'integer': is_integer,
    'float': is_float,
    'string': is_string,
    'boolean': is_boolean,
    'date': is_date
}

def validate_data(schema, data):
    """
    Validates a row of data against the schema.

    Parameters:
    schema (dict): Schema of the table.
    data (dict): Data to be inserted.

    Returns:
    bool: True if data is valid, False otherwise.
    """
    # Check if all columns in schema are present in data
    if not all(col in data for col in schema['columns']):
        return False

    # Validate data for each column
    for column, col_type in schema['columns'].items():
        if not validation_mapping[col_type](data[column]):
            return False

    return True

def load_schema(table_name):
    """
    Loads the schema for a given table.

    Parameters:
    table_name (str): Name of the table.

    Returns:
    dict: The schema of the table.
    """
    schema_path = os.path.join('schemas', f'{table_name}.json')
    with open(schema_path, 'r') as schema_file:
        return json.load(schema_file)
