from create_tables import *

def sql_query():
    export_file = "euro_trade_df12.csv"
    file_path = f"/home/bautegg/Documents/Wunsz/GitHub/DB_integration/{export_file}"
    table_name = 'trade_euro4'

    df = pd.read_csv(export_file)
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