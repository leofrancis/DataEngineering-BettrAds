import pandas as pd
import requests
import json
import os

from configparser import ConfigParser
import psycopg2
from sqlalchemy import create_engine

PATH_DIR = os.path.dirname(__file__)
REPORT_DIR = f"{PATH_DIR[:len(PATH_DIR)-3]}/report"
SQLDB_FILE = f"{PATH_DIR[:len(PATH_DIR)-3]}/schema.sql"

def load_configs(PATH=f"{PATH_DIR}/CONFIGS"):
    with open(PATH, "r") as f:
        return json.loads(f.read())

def load_data_api(configs, loads=True):
    if loads:
        session = requests.Session()
    
        try: 
            response = session.get(configs["URL_API"])
            status_code = response.status_code
            valid = response.ok
            data = response.json()
        except:
            status_code = None
            valid = False
            data = []

        filter_column = ["changeuuid", "name", "countrycode", "country", "tags"]

        df = pd.read_json(json.dumps(data))[filter_column]

        df.to_parquet(f"{PATH_DIR}/data.parquet")
    else:
        df = pd.read_parquet(f"{PATH_DIR}/data.parquet")

    return df

def populate():
    configs = load_configs()

    df_raw = load_data_api(configs)

    return df_raw

execute_once = True

def report_save_db(df):
    try:
        # read connection parameters
        configs = load_configs()
        configs = configs["db"]

        # connect to the PostgreSQL server
        print(' Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(f"dbname='{configs['database']}' user='{configs['user']}' host='{configs['host']}' password='{configs['password']}'")        
                        
        # create a cursor
        cur = conn.cursor()
        
        if execute_once: 
            try:
                with open(SQLDB_FILE, "r") as f:
                    query_execute = f.read()
                
                cur.execute(query_execute)
                execute_once = False
            except:
                execute_once = True
            
        list_stations = set()
        list_stations_tags = set()     

        for idx in range(0, len(df)):
            list_stations.add((df["changeuuid"][idx], df['name'][idx], df['countrycode'][idx]))
            if len(df["tags"][idx]) > 1:
                for tag in df["tags"][idx].split(","):
                    list_stations_tags.add((df["changeuuid"][idx], tag))

        query_execute = ""
        for df_idx in list_stations:
            (id, name, code) = df_idx

            if "'" in id: 
                id = id.replace("'", '"')
            if "'" in name: 
                name = name.replace("'", '"')
            if "'" in code: 
                code = code.replace("'", '"')
            
            id = "'"+id+"'"
            name = "'"+name+"'"
            code = "'"+code+"'"

            query_execute = f"{query_execute} INSERT INTO stations (id, name, countrycode) values ({id}, {name}, {code}); commit; "
        cur.execute(query_execute)

        for df_idx in list_stations_tags:
            (id, tag) = df_idx
            if "'" in id: 
                id = id.replace("'", '"')
            if "'" in tag: 
                tag = tag.replace("'", '"')
            
            id = "'"+id+"'"
            tag = "'"+tag+"'"

            query_execute = f'{query_execute} INSERT INTO station_tags (id, tag) values ({id}, {tag}); commit; '
        cur.execute(query_execute) 

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print(' Database connection closed.')

def report_export_from_db(code):
    try:
        # read connection parameters
        configs = load_configs()
        configs = configs["db"]

        # connect to the PostgreSQL server
        print(' Connecting to the PostgreSQL database...')
        # conn = psycopg2.connect(**params)
        conn = psycopg2.connect(f"dbname='{configs['database']}' user='{configs['user']}' host='{configs['host']}' password='{configs['password']}'")        
            
        # create a cursor
        cur = conn.cursor()
        
        if code != 'BR':
            query_execute = " SELECT count(tag) as qty, tag FROM station_tags, stations WHERE station_tags.id = stations.id GROUP BY tag ORDER BY qty DESC LIMIT 10; "
        else:
            query_execute = f" SELECT count(tag) as qty, tag FROM station_tags, stations WHERE station_tags.id = stations.id and stations.countrycode = '{code}' GROUP BY tag ORDER BY qty DESC LIMIT 10; "

        cur.execute(query_execute)

        src_res = cur.fetchall()
        print('PostgreSQL: extracting data from tables...')
        df_export = pd.DataFrame()
        count = 0 
        for tbl in src_res:
            if count == 0: 
                df_export["Qty"] = tbl[0]
            else:
                df_export["tag"] = tbl[0]
        
        if code != 'BR':
            df_export.to_csv(f"{REPORT_DIR}/mundo.csv", index=False)
        else:
            df_export.to_csv(f"{REPORT_DIR}/brasil.csv", index=False)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print(' Database connection closed.')
    return True

def report(sql="sql"):
    file_path = f"{PATH_DIR}/data.parquet"

    if not os.path.exists(file_path):
        return False

    df = pd.read_parquet(file_path)

    if sql != "sql":
        list_name = ["mundial", "brasil"]
        for name in list_name:
            tags_set = {}
            if name == "brasil":
                df["countrycode"] = df["countrycode"].apply(lambda row: row.upper())
                df = df.drop(df[df["countrycode"] != "BR"].index, axis=0)

                print(df[["name", "countrycode", "tags"]])

            for tag in df["tags"]:
                tag_list = tag.split(",")
                
                for i in tag_list:
                    try:
                        tags_set[i] = tags_set[i] + 1
                    except:
                        tags_set[i] = 1
            
            df_qty = pd.DataFrame([(tag, tags_set[tag]) for tag in tags_set], columns=["Gênero Musical", "Quantidade de Rádios Tocando"])
            df_qty["Quantidade de Rádios Tocando"] = df_qty["Quantidade de Rádios Tocando"].astype('int64')
        
            df_export = df_qty.sort_values(by=["Quantidade de Rádios Tocando"], ascending=False).head(11)[1:]
            df_export.to_csv(f"{REPORT_DIR}/{name}.csv", index=False)
    else:
        # report_save_db(df)

        countrycode = None
        countrycode_br = "BR"

        report_export_from_db(countrycode)
        report_export_from_db(countrycode_br)
        

    
    return True