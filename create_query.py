from create_tables import *

def sql_query():
    sub_folder = "V_1"
    print(sub_folder)
    export_file = "european_trade123.csv"
    file_path = f"/home/bautegg/Documents/Wunsz/GitHub/DB_integration/{sub_folder}/{export_file}"
    table_name = 'population'

    df = pd.read_csv(export_file)
    print(df)
    col_list = df.head()

    column_types = ''
    for i in col_list:
        column_types = column_types + i + ' bigint, '

    query1 = f"CREATE TABLE {table_name} ({column_types});"
    query2 = f"\COPY {table_name} FROM {file_path} WITH CSV HEADER DELIMITER ',';"

    print(query1, "\n")
    print(query2)

if __name__ == '__main__':
    print("** sql_querry **\n", sql_query())