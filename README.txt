MyRDBMS is a Python-based relational database management system (RDBMS) designed to provide a simple yet effective platform for data storage and query processing. This system implements basic functionalities of a traditional RDBMS, including data manipulation (CRUD operations) and querying capabilities.

**** Commands to run the program ****

How to invoke CLI:
- Navigate to my_rdbms folder
- run python3 main.py

I. data_manipulation
create
command: create table [table name] ("col1" [type],"col2" [type],"col3" type)
example: create table customers ("rank" integer, "username" string, "balance" integer)
* Supported type: integer, boolean, float, string, date

insert
command: insert into [table name] values [list of values]
example: insert into customers values (1,John W,300)

delete
command: delete from [table name] where [condition]
example: delete from customers where username==Sandy Z

update
command: update [table name] set [column change] where [condition]
example: update customers set username = john z where rank==1

load
command: load data from [path to CSV file] into [table name]
example: load data from './athlete_events.csv' into events 

II. data_query
Projection
command: show me [column name] from [table name]
exampe: show me event from events

Filtering
command: find all [table name] where [condition]
example: find all events where team==United States
* Only support one condition per query

Join
command: join [table name1] with [table name2] on [column name]
example: find all events where year<=1920;join events with regions on NOC;
* Only support inner join

Group
command: group [table name] by [column name] and [aggregation type] [column name]
example: group events by team and max year
* Aggregation type: max, min, average, count, sum

Aggregation
command: calculate [aggregation type] of [column name] from [table name]
example: calculate average of year from events
* Aggregation type: max, min, average, count, sum

Order
command: list [table name] ordered by [column name] in [asc/desc]
example: find all events where year<=1920;list events ordered by year in desc;

**** File structure ****

my_project_root/
│
├── my_rdbms/
│   ├── __init__.py
│   ├── main.py (entry point of the program, invoke CLI)
│   ├── data_storage/
│   │   ├── __init__.py
│   │   ├── chunking.py (handle read/write data in chunks)
│   │   └── file_manager.py (assist in creating and managing chunk files)
│   │
│   ├── schema_management/
│   │   ├── __init__.py
│   │   ├── schema_manager.py (handle the creation, updating and deletion of table schemas)
│   │   └── schema_validator.py (validate data type to ensure data adheres to the defined type)
│   │
│   ├── query_processing/
│   │   ├── __init__.py
│   │   ├── query_parser.py (parse user's input command and map to corresponding operation)
│   │   ├── data_manipulation.py (CREATE, INSERT, DELETE, UPDATE and LOAD)
│   │   └── data_query.py (filter, projection, join, group, aggregate, sort)
│   │
│   └── utils/
│       ├── __init__.py
│       └── helpers.py (placeholder for future utility functions)
│
├── data/
│   └── (chunks will be stored here)
│
└── schemas/
    └── (schema files will be stored here)

**** License ****
This project is licensed under the MIT License - see the LICENSE.md file for details.
