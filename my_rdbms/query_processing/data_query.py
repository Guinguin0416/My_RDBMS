# data_query.py
from data_storage.chunking import read_data
import csv, re

def project_data(operation_details, input_file=None, output_file="projected_data.csv"):
    table_name = operation_details['table_name']
    columns = operation_details['columns']

    with open(output_file, mode='w', newline='') as file:
        writer = None
        for row in read_data(table_name, input_file):
            # Convert row keys to lowercase for case-insensitive matching
            row_lower = {k.lower(): v for k, v in row.items()}
            # Convert desired columns to lowercase for matching
            columns_lower = [c.lower() for c in columns]

            projected_row = {col: row_lower[col] for col in columns_lower if col in row_lower}
            # print(f"Processing Row: {row}")  # Debugging
            # print(f"Projected Row: {projected_row}")  # Debugging

            if not writer:
                writer = csv.DictWriter(file, fieldnames=projected_row.keys())
                writer.writeheader()
            writer.writerow(projected_row)

    return output_file

def filter_data(operation_details, input_file=None, output_file="filtered_data.csv"):
    table_name = operation_details['table_name']
    condition = operation_details['condition']
    condition_func = compile_condition(condition)

    with open(output_file, mode='w', newline='') as file:
        writer = None
        for row in read_data(table_name, condition_func, input_file):
            # print(f"Row data: {row}")  # Debugging output
            if condition_func(row):  # Check if row meets the condition
                if not writer:
                    writer = csv.DictWriter(file, fieldnames=row.keys())
                    writer.writeheader()
                writer.writerow(row)

                
    return output_file

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
#                 row_value = row_lower[column]
#                 return operators[operator](row[column], value)
#             except KeyError:
#                 # If the column is not found in the row, return False
#                 return False
#         print(f"Comparing row value '{row_value}' with condition value '{value}' -> {result}")
    

#         return condition_func
#     else:
#         raise ValueError(f"Invalid condition format: {condition}")

def compile_condition(condition):
    """
    Compiles a condition string into a function that can be used to filter rows.
    The function returned will perform case-insensitive comparison on column names.
    """
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



def join_data(operation_details, input_file1=None, input_file2=None, output_file="join_output.csv"):
    table1_name = operation_details['table1']
    table2_name = operation_details['table2']
    join_column = operation_details['condition'].lower()  # Assuming condition is the join column

    with open(output_file, mode='w', newline='') as outfile:
        writer = None
        for row1 in read_data(table1_name, input_file=input_file1):
            row1_lower = {k.lower(): v for k, v in row1.items()}
            for row2 in read_data(table2_name, input_file=input_file2):
                row2_lower = {k.lower(): v for k, v in row2.items()}
                if row1_lower.get(join_column, None) == row2_lower.get(join_column, None):
                    combined_row = {**row1, **row2}
                    if not writer:
                        writer = csv.DictWriter(outfile, fieldnames=combined_row.keys())
                        writer.writeheader()
                    writer.writerow(combined_row)
    
    return output_file


# def group_data(operation_details, input_file=None, output_file="group_output.csv"):
#     table_name = operation_details['table_name']
#     group_column = operation_details['column_name']

#     # Initialize a dictionary to hold grouped data
#     grouped_data = {}

#     # Process each row and group them by the specified column
#     for row in read_data(table_name, input_file=input_file):
#         row_lower = {k.lower(): v for k, v in row.items()}  # Convert row keys to lowercase
#         key = row_lower[group_column]
#         if key not in grouped_data:
#             grouped_data[key] = []
#         grouped_data[key].append(row)

#     # Write grouped data to a file
#     with open(output_file, mode='w', newline='') as outfile:
#         writer = None
#         for key, group_rows in grouped_data.items():
#             if not writer:
#                 writer = csv.DictWriter(outfile, fieldnames=group_rows[0].keys())
#                 writer.writeheader()
#             # Write all rows in the group
#             for row in group_rows:
#                 writer.writerow(row)

#     return output_file

def group_data(operation_details, input_file=None, output_file="group_output.csv"):
    table_name = operation_details['table_name']
    group_column = operation_details['group_column'].lower()
    agg_column = operation_details.get('agg_column', None)
    if agg_column:
        agg_column = agg_column.lower()
    agg_type = operation_details.get('agg_type', 'sum').lower()

    # debugging
    # print(f"Grouping Table: {table_name}, Group Column: {group_column}, Agg Column: {agg_column}")
    
    grouped_data = {}

    for row in read_data(table_name, input_file=input_file):
        row_lower = {k.lower(): v for k, v in row.items()}  # Convert row keys to lowercase
        group_key = row_lower.get(group_column, None)
        if group_key is None:
            continue
        # debugging
        # print(f"Processing Row: {row}")

        if group_key not in grouped_data:
            grouped_data[group_key] = {'count': 0, 'sum': 0, 'min': float('inf'), 'max': float('-inf')}

        try:
            value = float(row_lower[agg_column]) if agg_column else 1
        except (ValueError, KeyError):
            continue

        grouped_data[group_key]['count'] += 1
        grouped_data[group_key]['sum'] += value
        grouped_data[group_key]['min'] = min(grouped_data[group_key]['min'], value)
        grouped_data[group_key]['max'] = max(grouped_data[group_key]['max'], value)

    with open(output_file, mode='w', newline='') as outfile:
        fieldnames = [group_column]
        if agg_type == 'average':
            fieldnames.append('Average')
        else:
            fieldnames.append(agg_type.capitalize())

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for key, values in grouped_data.items():
            if agg_type == 'average':
                avg_value = values['sum'] / values['count'] if values['count'] > 0 else 0
                writer.writerow({group_column: key, 'Average': avg_value})
            else:
                agg_result = values[agg_type] if agg_type != 'count' else values['count']
                writer.writerow({group_column: key, agg_type.capitalize(): agg_result})

    return output_file





def aggregate_data(operation_details, input_file=None, output_file="aggregated_data.csv"):
    table_name = operation_details['table_name']
    agg_column = operation_details['column_name'].lower()
    agg_type = operation_details['aggregation_type'].lower()

    aggregated_results = {'sum': 0, 'count': 0, 'min': None, 'max': None, 'total_for_avg': 0}
    row_count_for_avg = 0  # Separate counter for average calculation

    for row in read_data(table_name, input_file=input_file):
        row_lower = {k.lower(): v for k, v in row.items()}  # Convert row keys to lowercase
        try:
            value = float(row_lower[agg_column])
        except (ValueError, KeyError):
            continue

        # Update aggregation results
        if agg_type in ['sum', 'average']:
            aggregated_results['sum'] += value
        if agg_type == 'average':
            aggregated_results['total_for_avg'] += value
            row_count_for_avg += 1
        if agg_type == 'count':
            aggregated_results['count'] += 1
        if agg_type == 'min':
            if aggregated_results['min'] is None or value < aggregated_results['min']:
                aggregated_results['min'] = value
        if agg_type == 'max':
            if aggregated_results['max'] is None or value > aggregated_results['max']:
                aggregated_results['max'] = value

    # Calculate average if needed
    if agg_type == 'average' and row_count_for_avg > 0:
        aggregated_results[agg_column] = aggregated_results['total_for_avg'] / row_count_for_avg
    else:
        aggregated_results[agg_column] = aggregated_results[agg_type]

    # Write the aggregated result to a file
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Aggregation_Type', 'Value'])
        writer.writerow([agg_type, aggregated_results[agg_column]])

    return output_file


def merge_sort(arr, column, ascending=True):
    if len(arr) > 1:
        mid = len(arr) // 2
        left_half = arr[:mid]
        right_half = arr[mid:]

        merge_sort(left_half, column, ascending)
        merge_sort(right_half, column, ascending)

        i = j = k = 0

        while i < len(left_half) and j < len(right_half):
            if (left_half[i][column] < right_half[j][column] and ascending) or \
               (left_half[i][column] > right_half[j][column] and not ascending):
                arr[k] = left_half[i]
                i += 1
            else:
                arr[k] = right_half[j]
                j += 1
            k += 1

        while i < len(left_half):
            arr[k] = left_half[i]
            i += 1
            k += 1

        while j < len(right_half):
            arr[k] = right_half[j]
            j += 1
            k += 1

# def order_data(operation_details, input_file=None, output_file="ordered_data.csv"):
#     table_name = operation_details['table_name']
#     order_column = operation_details['column_name'].lower()
#     order_direction = operation_details['order'].lower()  # 'asc' or 'desc'

#     # Convert all keys in rows to lowercase
#     data_list = [{k.lower(): v for k, v in row.items()} for row in read_data(table_name, input_file=input_file)]

#     # Applying merge sort
#     merge_sort(data_list, order_column, ascending=order_direction == 'asc')

#     # Write the ordered results to a file
#     with open(output_file, mode='w', newline='') as file:
#         if data_list:
#             writer = csv.DictWriter(file, fieldnames=data_list[0].keys())
#             writer.writeheader()
#             for row in data_list:
#                 writer.writerow(row)

#     return output_file

def order_data(operation_details, input_file=None, output_file="ordered_data.csv"):
    table_name = operation_details['table_name']
    order_column = operation_details['column_name'].lower()  # Convert to lowercase
    order_direction = operation_details['order'].lower()  # 'asc' or 'desc'

    # Convert all keys in rows to lowercase and ensure numeric values are treated as such
    data_list = []
    for row in read_data(table_name, input_file=input_file):
        row_lower = {k.lower(): v for k, v in row.items()}
        # Attempt to convert the order column to a numeric value
        try:
            row_lower[order_column] = float(row_lower[order_column])
        except ValueError:
            pass  # If conversion fails, leave it as is
        data_list.append(row_lower)

    # Applying merge sort
    merge_sort(data_list, order_column, ascending=order_direction == 'asc')

    # Write the ordered results to a file
    with open(output_file, mode='w', newline='') as file:
        if data_list:
            writer = csv.DictWriter(file, fieldnames=data_list[0].keys())
            writer.writeheader()
            for row in data_list:
                writer.writerow(row)

    return output_file


