import os

# Assumes that the chunks are stored in a 'data' subdirectory
DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')

def create_new_chunk(table_name, chunk_number):
    """
    Create a new chunk file for a table if it doesn't exist and return its file name.
    """
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    file_name = get_chunk_file_name(table_name, chunk_number)
    if not os.path.exists(file_name):
        open(file_name, 'a').close()  # Create an empty file
    return file_name

def get_chunk_file_name(table_name, chunk_number):
    """
    Returns the standardized file name for a chunk given a table name and chunk number.
    """
    return os.path.join(DATA_DIR, f"{table_name}_chunk_{chunk_number}.csv")

# Additional file operations like deleting chunks, listing all chunks, etc. could be added here.
