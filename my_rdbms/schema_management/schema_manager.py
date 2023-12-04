import json
import os

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), '../../schemas')

if not os.path.exists(SCHEMA_DIR):
    os.makedirs(SCHEMA_DIR)

def create_table_schema(table_name, columns):
    """
    Create a new schema for a table and save it.

    Parameters:
    table_name (str): The name of the table.
    columns (dict): A dictionary where keys are column names and values are data types.
    """
    schema = {'columns': columns}
    save_schema(table_name, schema)

def save_schema(table_name, schema):
    """
    Save a schema to a JSON file.

    Parameters:
    table_name (str): The name of the table.
    schema (dict): The schema to be saved.
    """
    schema_file_path = os.path.join(SCHEMA_DIR, f'{table_name}.json')
    with open(schema_file_path, 'w') as schema_file:
        json.dump(schema, schema_file)

def load_schema(table_name):
    """
    Load a schema from a JSON file.

    Parameters:
    table_name (str): The name of the table to load the schema for.

    Returns:
    dict: The loaded schema.
    """
    schema_file_path = os.path.join(SCHEMA_DIR, f'{table_name}.json')
    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"Schema for table {table_name} not found.")
    
    with open(schema_file_path, 'r') as schema_file:
        return json.load(schema_file)

def update_table_schema(table_name, new_columns):
    """
    Update an existing table's schema with new columns.

    Parameters:
    table_name (str): The name of the table.
    new_columns (dict): New columns to add to the schema.
    """
    schema = load_schema(table_name)
    schema['columns'].update(new_columns)
    save_schema(table_name, schema)

def delete_table_schema(table_name):
    """
    Delete a table's schema file.

    Parameters:
    table_name (str): The name of the table.
    """
    schema_file_path = os.path.join(SCHEMA_DIR, f'{table_name}.json')
    if os.path.exists(schema_file_path):
        os.remove(schema_file_path)
