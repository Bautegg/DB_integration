import psycopg2
import json

def read_json():
    with open("config.json", "r") as openfile:
        config_json = json.load(openfile)

    return config_json


password = read_json()["password"]
conn = psycopg2.connect(f"dbname=UE_eco_data user=postgres password={password} host=localhost")
cur = conn.cursor()
cur.execute("""
select trade_year.p_k, trade_year.country_id, country_name, imp_exp, trade_year.year
	, pop_value, trade_value
from trade_year
	join population on population.country_id = trade_year.country_id
		and population.year = trade_year.year
where trade_year.country_id = 'PL' and ext_int = 'WORLD'
;""")
records = cur.fetchall()

print(*records, sep = "\n")
