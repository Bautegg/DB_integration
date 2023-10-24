import pandas as pd
import json

"""
data sources:
https://ec.europa.eu/eurostat/databrowser/view/DS-059268/legacyMultiFreq/table?lang=en
    1,2 - import, export; EXTRA, INTRA, WORLD - external (without UE), internal (only UE), SUM External + Internal
https://ec.europa.eu/eurostat/databrowser/view/demo_gind/default/table?lang=en
"""
delimiter = "\t"
first_col = "trade"
primary_key_label = "p_k"
files_dict = {"trade": "ds-059268$defaultview_tabular.tsv"
             , "population": "demo_gind_tabular.tsv"}


def load_file(df_key):
    file_path = files_dict[df_key]
    df = pd.read_csv(file_path, sep=delimiter, na_values=": ", keep_default_na=True)
    pd.options.display.float_format = "{:.0f}".format
    df.iloc[:, 0] = df.iloc[:, 0].replace(regex={",": "_"})

    return df

# TODO wwszystkie 4 operacje wykonać na każdym elemencie


def prepare_header(df_key):
    df_p = load_file(df_key)
    # remove whitespaces from header_names list
    header_names = [x.strip() for x in list(df_p.columns)]
    # lower all elements of header_names
    header_names = [x.lower() for x in header_names]
    # replace '-' with '_' in header_names list
    header_names = [x.replace('-', '.') for x in header_names]
    # add 'd' at the begining of column name where first character is a number
    # header_names = ['d' + x if x[0].isnumeric() else x for x in header_names]
    header_names[0] = first_col

    return header_names


def read_json():
    with open("country_codes.json", "r") as openfile:
        mapping_json = json.load(openfile)

    return mapping_json


def trade_cleansing():
    # split trade column into another columns
    table_name = "trade"
    df = load_file(table_name)
    df.columns = prepare_header(table_name)
    df["country_id"] = df["trade"].str[2:4]
    df["ext_int"] = df["trade"].str[-28:-23]
    df["imp_exp"] = df["trade"].str[-16].replace({"1": "IMPORT", "2": "EXPORT"})
    # divide df to word and month tables
    df_year = df.iloc[:166, :]
    df_year = df_year.loc[:, ["country_id", "ext_int", "imp_exp", "2020", "2021", "2022"]]
    df_year = pd.melt(df_year, id_vars=["country_id", "ext_int", "imp_exp"], value_vars=["2020", "2021", "2022"],
                      var_name="year", value_name="trade_value")
    df_month = df.iloc[166:, :]
    df_month = df_month.drop(["2020", "2021", "2022"], axis=1)
    df_month_r = df_month.iloc[:, 1: 31]
    df_month_l = df_month.loc[:, ["country_id", "ext_int", "imp_exp"]]
    df_month = df_month_l.join(df_month_r)
    month_header = list(df_month.iloc[:, 3:])
    df_month = pd.melt(df_month, id_vars=["country_id", "ext_int", "imp_exp"], value_vars=month_header,
                      var_name="month", value_name="trade_value")
    df_month["month"] = pd.to_datetime(df_month["month"], format="%Y.%m")

    table_name = "population"
    df_population = load_file(table_name)
    df_population .columns = prepare_header(table_name)
    df2 = df_population.iloc[:, 0].str.split("_", expand=True)
    country_codes = read_json()[0]
    sr_country = df2.iloc[:, 2].replace(country_codes).rename("country_name")
    geo_data = read_json()[1]
    sr_geo_data = df2.iloc[:, 1].replace(geo_data).rename("data_desc")
    df_pop = pd.concat([df2.iloc[:, 2], sr_country, df2.iloc[:, 1], sr_geo_data, df_population.iloc[:, 1:64]], axis=1)
    df_pop = df_pop.rename(columns={2: "country_id", 1: "data_short"})
    pop_header = list(df_pop.iloc[:, 4:])
    df_pop = pd.melt(df_pop, id_vars=["country_id", "country_name", "data_short", "data_desc"], value_vars=pop_header,
                       var_name="year", value_name="pop_value")

    return [df_year, df_month, df_pop]


def export_file_sql():
    export_file = "euro_trade_df"
    df_list = trade_cleansing()
    counter = 0
    for i in df_list:
        counter += 1
        export_file = export_file + str(counter)
        # i.to_csv(f"{export_file}.csv", sep=",", index_label=primary_key_label, float_format="%.0f")



def basic_analyse():
    df = load_file()
    print("** MAX IS ** \n", df.max())
    print("** MIN IS ** \n", df.min())
    print("** DTYPES IS ** \n", df.dtypes)

    df.fillna(0, inplace=True)
    sum_2020 = sum(df.iloc[:, 1])
    return sum_2020

    # TODO tabela sumująca wartości ze wszystkich krajów, tabela porównująca import i eksport per kraj
    # TODO sensownie podzielić na więcej kolumn tabele populacja
    # TODO zabezpieczeniqa autorskie do psycopg2-binarry
    # TODO zestawić dane handlowe np. z populacją
    # TODO nagłówek z pliku i tabeli musi się zgadzać, zapisać nowy plik i go załadować do psql

    # ---TODO funkcja load_file ma pobrać słownik nazwa pliku: ścieżka pliku i zwrócić liste df w zależności od parametru funkcji load_file---
    # ---TODO skupić się na tabeli per miesiąc, zamienić kolumnę "month" na datę---
    # ---TODO zrobić z danych pivota (columny z datą jako wiersze) -> pd.melt---


if __name__ == '__main__':
    # print(load_file())
    # print(prepare_header())
    # print("** basic_analyse **\n", basic_analyse())
    # print("** read_json **\n", read_json())
    # print("** export_file_sql **\n", export_file_sql())
    print("** trade_cleansing **\n", trade_cleansing())
