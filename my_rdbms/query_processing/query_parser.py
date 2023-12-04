import re

def parse_user_input(user_input):
    """
    Parses the entire user input and returns a list of parsed queries.

    Parameters:
    user_input (str): The full query string entered by the user.

    Returns:
    list: A list of parsed queries.
    """
    operations = user_input.split(';')
    return [parse_query(op.strip()) for op in operations if op.strip()]

def parse_query(query):
    """
    Parses a query string and returns the operation type and its details.

    Parameters:
    query (str): The query string entered by the user.

    Returns:
    tuple: A tuple containing the operation type and a dictionary of details.
    """
    query = query.lower().strip()

    if query.startswith("create table"):
        return "create", parse_create_table(query)
    elif query.startswith("insert into"):
        return "insert", parse_insert_into(query)
    elif query.startswith("delete"):
        return "delete", parse_delete(query)
    elif query.startswith("update"):
        return "update", parse_update(query)
    elif query.startswith("show me"):
        return "projection", parse_projection(query)
    elif query.startswith("find all"):
        return "filtering", parse_filtering(query)
    elif query.startswith("join"):
        return "joining", parse_joining(query)
    elif query.startswith("group"):
        return "grouping", parse_grouping(query)
    elif query.startswith("calculate"):
        return "aggregation", parse_aggregation(query)
    elif query.startswith("list"):
        return "ordering", parse_ordering(query)
    elif query.startswith("load data"):
        return "load", parse_load(query)
    else:
        raise ValueError("Unsupported query type.")

def parse_create_table(query):
    """
    Parses a CREATE TABLE query with support for quoted column names.

    Example:
    create table Youtubers ("Rank" Integer, "User Name" String, "Average Age" Integer)
    """
    pattern = r'create table (\w+) \((.*)\)'
    match = re.match(pattern, query, re.IGNORECASE)
    if not match:
        raise ValueError("Invalid CREATE TABLE query.")

    table_name = match.group(1)
    columns_part = match.group(2)

    # Regular expression to match both quoted and unquoted column names
    column_pattern = r'(?:"([^"]+)"|(\w+))\s+(\w+)'
    columns = {}
    for column_match in re.finditer(column_pattern, columns_part):
        # Column name is either in the first or second group, depending on whether it's quoted
        column_name = column_match.group(1) or column_match.group(2)
        column_type = column_match.group(3)
        columns[column_name] = column_type

    return {"table_name": table_name, "columns": columns}


def parse_insert_into(query):
    """
    Parses an INSERT INTO query.
    """
    # Basic pattern matching to extract table name and values
    # Note: This is a simplified parser and might not handle complex cases
    pattern = r"insert into (\w+) values \((.*)\)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid INSERT INTO query.")

    table_name = match.group(1)
    values = match.group(2).split(',')

    return {"table_name": table_name, "values": values}

def parse_delete(query):
    """
    Parses a DELETE query.
    """
    # Basic pattern matching to extract table name and condition
    pattern = r"delete from (\w+) where (.*)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid DELETE query.")

    table_name = match.group(1)
    condition = match.group(2)

    return {"table_name": table_name, "condition": condition}

def parse_update(query):
    """
    Parses an UPDATE query.
    """
    # Basic pattern matching to extract table name, column, new value, and condition
    pattern = r"update (\w+) set (\w+) = (.*) where (.*)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid UPDATE query.")

    table_name = match.group(1)
    column = match.group(2)
    new_value = match.group(3)
    condition = match.group(4)

    return {"table_name": table_name, "column": column, "new_value": new_value, "condition": condition}

def parse_projection(query):
    pattern = r"show me (.*) from (\w+)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid projection query.")
    return {"columns": match.group(1).split(', '), "table_name": match.group(2)}

def parse_filtering(query):
    pattern = r"find all (\w+) where (.*)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid filtering query.")
    return {"table_name": match.group(1), "condition": match.group(2)}

def parse_joining(query):
    pattern = r"join (\w+) with (\w+) on (.*)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid joining query.")
    return {"table1": match.group(1), "table2": match.group(2), "condition": match.group(3)}

# def parse_grouping(query):
#     pattern = r"group (\w+) by (\w+)"
#     match = re.match(pattern, query)
#     if not match:
#         raise ValueError("Invalid grouping query.")
#     return {"table_name": match.group(1), "column_name": match.group(2)}

def parse_grouping(query):
    pattern = r"group (\w+) by (\w+)(?: and (sum|average|min|max|count) (\w+))?"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid grouping query.")

    table_name = match.group(1)
    group_column = match.group(2)
    agg_type = match.group(3) if match.group(3) else 'sum'  # Default aggregation type
    agg_column = match.group(4) if match.group(4) else None

    return {"table_name": table_name, "group_column": group_column, "agg_type": agg_type, "agg_column": agg_column}

def parse_aggregation(query):
    pattern = r"calculate (.*) of (\w+) from (\w+)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid aggregation query.")
    return {"aggregation_type": match.group(1), "column_name": match.group(2), "table_name": match.group(3)}

def parse_ordering(query):
    pattern = r"list (\w+) ordered by (\w+) in (asc|desc)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid ordering query.")
    return {"table_name": match.group(1), "column_name": match.group(2), "order": match.group(3)}

def parse_load(query):
    """
    Parses a LOAD DATA query.

    Parameters:
    query (str): The query string to be parsed.

    Returns:
    dict: A dictionary containing the path to the CSV file and the target table name.
    """
    pattern = r"load data from ['\"](.+?)['\"] into (\w+)"
    match = re.match(pattern, query)
    if not match:
        raise ValueError("Invalid LOAD DATA query.")

    csv_file_path = match.group(1)
    table_name = match.group(2)

    return {"csv_file_path": csv_file_path, "table_name": table_name}