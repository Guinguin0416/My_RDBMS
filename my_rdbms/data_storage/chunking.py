import csv
import os
from .file_manager import create_new_chunk, get_chunk_file_name  # Adjust the import path as needed

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
# CURRENT_CHUNK = 0
chunk_counters = {}

import csv
import os
from .file_manager import create_new_chunk, get_chunk_file_name

CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
chunk_counters = {}  # Dictionary to track current chunk for each table

def write_data(data, columns, table_name):
    global chunk_counters

    # Initialize chunk counter for the table if it doesn't exist
    if table_name not in chunk_counters:
        chunk_counters[table_name] = 0

    file_name = get_chunk_file_name(table_name, chunk_counters[table_name])

    # Check if the current chunk has space
    if os.path.exists(file_name):
        if os.path.getsize(file_name) + len(str(data)) > CHUNK_SIZE:
            chunk_counters[table_name] += 1
            file_name = create_new_chunk(table_name, chunk_counters[table_name])

    # Write data to the chunk
    with open(file_name, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        if file.tell() == 0:  # Write header if file is empty
            writer.writeheader()
        writer.writerow(data)


# def write_data(data, columns, table_name):
#     global CURRENT_CHUNK
#     # global chunk_counters

#     # # Initialize chunk counter for the table if it doesn't exist
#     # if table_name not in chunk_counters:
#     #     chunk_counters[table_name] = 0

#     file_name = get_chunk_file_name(table_name, CURRENT_CHUNK)
#     # file_name = f"{table_name}_chunk_{chunk_counters[table_name]}.csv"

#     # Check if the current chunk has space
#     if os.path.exists(file_name):
#         if os.path.getsize(file_name) + len(str(data)) > CHUNK_SIZE:
#             CURRENT_CHUNK += 1
#             # chunk_counters[table_name] += 1
#             file_name = create_new_chunk(table_name, CURRENT_CHUNK)
#             # file_name = f"{table_name}_chunk_{chunk_counters[table_name]}.csv"

#     # Write data to the chunk
#     with open(file_name, mode='a', newline='') as file:
#         writer = csv.DictWriter(file, fieldnames=columns)
#         if file.tell() == 0:  # Write header if file is empty
#             writer.writeheader()
#         writer.writerow(data)

def read_data(table_name, condition_func=None, input_file=None):
    if input_file and os.path.exists(input_file):
        with open(input_file, mode='r') as file:
            reader = csv.DictReader(file)
            reader.fieldnames = [name.lower() for name in reader.fieldnames]
            for row in reader:
                row = {k.lower(): v for k, v in row.items()}
                if condition_func is None or condition_func(row):
                    # print(f"Yielding row from file: {row}")  # Debugging
                    yield row
    else:
        # Reading from default chunked files
        chunk_number = 0
        while True:
            file_name = get_chunk_file_name(table_name, chunk_number)
            if not os.path.exists(file_name):
                break

            with open(file_name, mode='r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if condition_func is None or condition_func(row):
                        # print(f"Yielding row from chunk {chunk_number}: {row}")  # Debugging
                        yield row

            chunk_number += 1
