import csv,re,os,traceback
from data_storage.chunking import write_data, read_data
from schema_management.schema_manager import create_table_schema, load_schema, delete_table_schema, update_table_schema
from schema_management.schema_validator import validate_data


def handle_create_table(parsed_query):
    table_name = parsed_query['table_name']
    columns = parsed_query['columns']
    create_table_schema(table_name, columns)


def handle_insert_into(parsed_query):
    # print("handle_insert_into called")  # Debugging: Confirm the function is called

    table_name = parsed_query['table_name']
    values_list = parsed_query['values']  # Assuming this is already a list

    # print(f"Values list: {values_list}, Type: {type(values_list)}")  # Debugging

    try:
        schema = load_schema(table_name)

        if len(values_list) != len(schema['columns']):
            raise ValueError("Number of values does not match number of columns")

        # Convert values to appropriate types based on schema
        data = {}
        for i, (column, col_type) in enumerate(schema['columns'].items()):
            value = values_list[i]
            if col_type == "string":
                # Assuming string values are passed with quotes
                data[column] = value.strip("'\"")
            elif col_type in ["integer", "float"]:
                data[column] = float(value) if col_type == "float" else int(value)
            else:
                data[column] = value  # For other data types, use as is

        if not validate_data(schema, data):
            raise ValueError("Inserted data does not conform to table schema.")

        write_data(data, schema['columns'].keys(), table_name)
    except Exception as e:
        print("Error in handle_insert_into:", e)
        print(traceback.format_exc())  # Print the full traceback


# import traceback

# def handle_insert_into(parsed_query):
#     print("handle_insert_into called")  # Debugging: Confirm the function is called

#     table_name = parsed_query['table_name']
#     values_string = parsed_query['values']

#     print(f"Values string: {values_string}, Type: {type(values_string)}")  # Debugging

#     pattern = r'\s*(?:\'([^\']*)\'|"([^"]*)"|([^,\s]+))\s*'

#     try:
#         values_list = [match[0] or match[1] or match[2] for match in re.findall(pattern, values_string)]
#     except Exception as e:
#         print("Error parsing values string:", e)
#         print(f"Values string causing error: {values_string}")
    
#     print("Processed values list:", values_list)

#     try:
#         schema = load_schema(table_name)

#         if len(values_list) != len(schema['columns']):
#             raise ValueError("Number of values does not match number of columns")

#         data = {column: value for column, value in zip(schema['columns'].keys(), values_list)}

#         if not validate_data(schema, data):
#             raise ValueError("Inserted data does not conform to table schema.")

#         write_data(data, schema['columns'].keys(), table_name)
#     except Exception as e:
#         print("Error in handle_insert_into:", e)
#         print(traceback.format_exc())  # Print the full traceback


# def handle_insert_into(parsed_query):
#     """
#     Handles inserting new data into a table.
#     """
#     table_name = parsed_query['table_name']
#     values_string = parsed_query['values']
#     pattern = r'\s*(?:\'([^\']*)\'|"([^"]*)"|([^,\s]+))\s*'
#     values_list = [match[0] or match[1] or match[2] for match in re.findall(pattern, values_string)]

#     # Ensure values are a string before processing
#     # values_str = ','.join(map(str, parsed_query['values']))
#     # Splitting values by comma while considering potential commas inside quoted strings
#     # values_list = re.findall(r'(?:[^,"]|"(?:\\.|[^"])*")+?', values_str)
#     # Cleaning up values (removing extra spaces and quotes)
#     # values_list = [value.strip().strip("'\"") for value in values_list]

#     print("Processed values list:", values_list)

#     schema = load_schema(table_name)

#     # Check if the number of values matches the number of columns
#     if len(values_list) != len(schema['columns']):
#         raise ValueError("Number of values does not match number of columns")

#     # Mapping values to columns
#     data = {column: value for column, value in zip(schema['columns'].keys(), values_list)}

#     # Validate the data against the schema
#     if not validate_data(schema, data):
#         raise ValueError("Inserted data does not conform to table schema.")

#     write_data(data, schema['columns'].keys(), table_name)


# def handle_insert_into(parsed_query):
#     table_name = parsed_query['table_name']
#     values = parsed_query['values']

#     # Splitting values by comma while considering potential commas inside quoted strings
#     values_list = re.findall(r'(?:[^,"]|"(?:\\.|[^"])*")+?', values)

#     # Cleaning up values (removing extra spaces and quotes)
#     values_list = [value.strip().strip("'\"") for value in values_list]

    
#     schema = load_schema(table_name)
#     # Convert values to a dictionary based on schema
#     data = {column: value for column, value in zip(schema['columns'].keys(), values)}

#     if not validate_data(schema, data):
#         raise ValueError("Inserted data does not conform to table schema.")

#     write_data(data, schema['columns'].keys(), table_name)

def handle_delete(parsed_query):
    table_name = parsed_query['table_name']
    condition = parsed_query['condition']
    condition_func = compile_condition(condition)

    schema = load_schema(table_name)  # Load schema for validation

    chunk_number = 0
    data_directory = os.path.join(os.path.dirname(__file__), '..', '..', 'data') # Adjust the path to the data directory

    # current_directory = os.getcwd()
    # print(f"Current working directory: {current_directory}")  # Debugging
    while True:
        chunk_file = f"{table_name}_chunk_{chunk_number}.csv"
        chunk_file_path = os.path.join(data_directory, chunk_file)
        # print(f"Looking for chunk file at: {chunk_file_path}")  # Debugging
        if not os.path.exists(chunk_file_path):
            # print(f"No more chunk files found after chunk number {chunk_number}. Exiting loop.")  # Debugging
            break

        # print(f"Checking chunk file: {chunk_file}")  # Debugging
        updated_chunk_data = []
        with open(chunk_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            updated = False
            for row in reader:
                if not validate_data(schema, row):
                    raise ValueError("Data in chunk does not conform to table schema.")
                if not condition_func(row):
                    updated_chunk_data.append(row)
                else:
                    updated = True
                    # print(f"Deleting row: {row}")  # Debugging

        if updated:
            # Rewrite only the affected chunk
            with open(chunk_file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                writer.writeheader()
                writer.writerows(updated_chunk_data)
            # print(f"Chunk {chunk_number} updated.")  # Debugging

        chunk_number += 1

def handle_update(parsed_query):
    table_name = parsed_query['table_name']
    column_to_update = parsed_query['column']
    new_value = parsed_query['new_value']
    condition = parsed_query['condition']
    condition_func = compile_condition(condition)

    chunk_number = 0
    data_directory = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
    # print(f"Updating column {column_to_update} to {new_value} for table {table_name} where {condition}")  # Debugging

    while True:
        chunk_file = f"{table_name}_chunk_{chunk_number}.csv"
        chunk_file_path = os.path.join(data_directory, chunk_file)
        # print(f"Looking for chunk file at: {chunk_file_path}")  # Debugging
        if not os.path.exists(chunk_file_path):
            # print(f"No more chunk files found after chunk number {chunk_number}. Exiting loop.")  # Debugging
            break

        updated_chunk_data = []
        with open(chunk_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            updated = False
            for row in reader:
                # print(f"CSV row: {row}")  # Debugging
                if condition_func(row):
                    # print(f"Row before update: {row}")  # Debugging
                    row[column_to_update] = new_value
                    # print(f"Row after update: {row}")  # Debugging
                    updated = True
                updated_chunk_data.append(row)

        if updated:
            # print(f"Chunk {chunk_number} has updates.")  # Debugging
            # Rewrite only the affected chunk
            with open(chunk_file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                writer.writeheader()
                # writer.writerows(updated_chunk_data)
                for updated_row in updated_chunk_data:
                    # print(f"Writing row: {updated_row}")  # Debugging
                    writer.writerow(updated_row)
                

        chunk_number += 1

# def compile_condition(condition):
#     """
#     Compiles a condition string into a function.
#     Supports basic comparison operators: >, <, >=, <=, ==, !=.
#     """
#     operators = {'>': lambda x, y: x > y,
#                  '<': lambda x, y: x < y,
#                  '>=': lambda x, y: x >= y,
#                  '<=': lambda x, y: x <= y,
#                  '==': lambda x, y: x == y,
#                  '!=': lambda x, y: x != y}

#     for op in operators:
#         if op in condition:
#             column, value = condition.split(op)
#             column, value = column.strip(), value.strip()

#             def condition_func(row):
#                 try:
#                     # Attempt to handle as float for numerical comparison
#                     return operators[op](float(row[column]), float(value))
#                 except ValueError:
#                     # Fallback to string comparison
#                     return operators[op](row[column], value)

#             return condition_func

#     raise ValueError(f"Invalid condition format: {condition}")

# def compile_condition(condition):
#     """
#     Compiles a condition string into a function that can be used to filter rows.
#     The function returned will perform case-insensitive comparison on column names.
#     """
#     operators = {
#         '>': lambda x, y: x > y,
#         '<': lambda x, y: x < y,
#         '>=': lambda x, y: x >= y,
#         '<=': lambda x, y: x <= y,
#         '==': lambda x, y: x == y,
#         '!=': lambda x, y: x != y
#     }

#     parts = re.split(r'([><=!]=?|!=)', condition)
#     if len(parts) == 3:
#         column, operator, value = parts
#         column = column.strip().lower()  # Normalize column name to lowercase

#         def condition_func(row):
#             # Convert row keys to lowercase to perform a case-insensitive match
#             row = {k.lower(): v for k, v in row.items()}
#             try:
#                 # Attempt to convert the value to a float for comparison
#                 row_value = float(row[column])
#                 value_float = float(value)
#                 return operators[operator](row_value, value_float)
#             except ValueError:
#                 # If conversion fails, fall back to string comparison
#                 return operators[operator](row[column], value)
#             except KeyError:
#                 # If the column is not found in the row, return False
#                 return False

#         # print(f"Compiled condition function: {condition_func}")  # Debugging
#         return condition_func
#     else:
#         raise ValueError(f"Invalid condition format: {condition}")

def compile_condition(condition):
    
    operators = {
        '>': lambda x, y: x > y,
        '<': lambda x, y: x < y,
        '>=': lambda x, y: x >= y,
        '<=': lambda x, y: x <= y,
        '==': lambda x, y: x == y,
        '!=': lambda x, y: x != y
    }

    parts = re.split(r'([><=!]=?|!=)', condition)
    if len(parts) == 3:
        column, operator, value = parts
        column = column.strip().lower()  # Normalize column name to lowercase

        def condition_func(row):
            # Convert row keys to lowercase to perform a case-insensitive match
            row_lower = {k.lower(): v for k, v in row.items()}
            try:
                # Attempt to convert the value to a float for comparison
                row_value = float(row_lower[column])
                value_float = float(value)
                result = operators[operator](row_value, value_float)
            except ValueError:
                # If conversion fails, fall back to string comparison
                row_value = row_lower[column]
                result = operators[operator](row_value.lower(), value.lower().strip())
            except KeyError:
                # If the column is not found in the row, return False
                result = False
            
            # Debugging print should be inside the condition_func
            # print(f"Comparing row value '{row_value}' with condition value '{value}' -> {result}")
            return result

        return condition_func
    else:
        raise ValueError(f"Invalid condition format: {condition}")

def handle_load(parsed_query):
    
    csv_file_path = parsed_query['csv_file_path']
    table_name = parsed_query['table_name']

    # Read data from the CSV file and write it into the database
    with open(csv_file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            write_data(row, reader.fieldnames, table_name)

