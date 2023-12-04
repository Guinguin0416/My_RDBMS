from tabulate import tabulate
from query_processing.query_parser import parse_user_input
from query_processing.data_manipulation import (
    handle_create_table,
    handle_insert_into,
    handle_delete,
    handle_update,
    handle_load
)
from query_processing.data_query import (
    project_data,
    filter_data,
    join_data,
    group_data,
    aggregate_data,
    order_data
)

def display_output(output_file):
    """Reads the output file and displays its contents in a tabular format."""
    try:
        with open(output_file, mode='r') as file:
            # Assuming the first line contains headers
            headers = file.readline().strip().split(',')
            data = [line.strip().split(',') for line in file]
        print(tabulate(data, headers=headers))
    except Exception as e:
        print(f"Error displaying output: {e}")

def main():
    print("Welcome to MyDB. Type 'exit' to exit.")
    previous_output_file = None  # To hold the output file name of the previous operation

    while True:
        user_input = input("MyDB > ")
        if user_input.lower() == 'exit':
            print("Exiting MyDB.")
            # Clean up temporary files or perform other shutdown tasks
            break

        try:
            operations = parse_user_input(user_input)
            for idx, (op_type, details) in enumerate(operations):
                # If previous_output_file is not None, it means we should use it as input for the current operation
                # debugging
                # print(f"Operation: {op_type}, Details: {details}, Previous output file: {previous_output_file}")

                # if previous_output_file:
                #     details['input_file'] = previous_output_file
                # else:
                #     details['input_file'] = None

                 # Determine if the operation should use the output file from the previous operation
                use_previous_output = previous_output_file and op_type not in ['create', 'insert', 'delete', 'update', 'load']
                input_file = previous_output_file if use_previous_output else None
                details['input_file'] = input_file  # Set the input_file in details


                if op_type == 'create':
                    handle_create_table(details)
                    print("Table created.")
                    output_file = None

                elif op_type == 'insert':
                    handle_insert_into(details)
                    print("Data inserted.")
                    output_file = None

                elif op_type == 'delete':
                    handle_delete(details)
                    print("Data deleted.")
                    output_file = None

                elif op_type == 'update':
                    handle_update(details)
                    print("Data updated.")
                    output_file = None

                elif op_type == 'load':
                    handle_load(details)
                    print("Data loaded.")
                    output_file = None

                elif op_type == 'projection':
                    # output_file = project_data(details)
                    output_file = project_data(details, input_file=previous_output_file)
                    display_output(output_file)
                    print("Projection completed.")
                    

                elif op_type == 'filtering':
                    # output_file = filter_data(details)
                    output_file = filter_data(details, input_file=previous_output_file)
                    display_output(output_file)
                    print("Filtering completed.")

                elif op_type == 'joining':
                    # output_file = join_data(details)
                    output_file = join_data(details, input_file1=previous_output_file, input_file2=None)  # Adjust as needed
                    display_output(output_file)
                    print("Join completed.")

                elif op_type == 'grouping':
                    # output_file = group_data(details)
                    output_file = group_data(details, input_file=previous_output_file)
                    display_output(output_file)
                    print("Grouping completed.")
                    
                elif op_type == 'aggregation':
                    # output_file = aggregate_data(details)
                    output_file = aggregate_data(details, input_file=previous_output_file)
                    display_output(output_file)
                    print("Aggregation completed.")

                elif op_type == 'ordering':
                    # output_file = order_data(details)
                    output_file = order_data(details, input_file=previous_output_file)
                    display_output(output_file)
                    print("Ordering completed.")
                
                # if output_file:
                #     previous_output_file = output_file

                # Update previous_output_file for the next operation in the chain
                if 'output_file' in locals():
                    previous_output_file = output_file
                else:
                    previous_output_file = None

                # Reset previous_output_file if it's the last operation or after certain operations
                if (idx == len(operations) - 1) or op_type in ['create', 'insert', 'delete', 'update', 'load']:
                    previous_output_file = None

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
