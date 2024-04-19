psql -h 34.91.230.60 -p 5432 -U airflow -d airflow


# Basic psql Commands

# After connecting to your database, here are some basic commands you can use:

#     List all databases: \l or \list
#     Connect to a specific database: \c <database_name>
#     List all tables in the current database: \dt (for standard tables) or \d (for all objects)
#     Describe a table: \d <table_name>
#     Execute SQL queries: Directly type any SQL query and end it with a semicolon (;). For example, SELECT * FROM your_table;