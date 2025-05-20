import os
import pandas as pd
from pymongo import MongoClient

# -- configuracao bd
DB_USER = "postgres"
DB_PASSWORD = "#######"  # - coloquem a palavra passe da vossa bd!
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bigdata"

MONGO_USER = "postgres"
MONGO_PASSWORD = "root"
MONGO_CLUSTER = "cluster0.6p5ublm.mongodb.net"
MONGO_DBNAME = "ObesityDatabase"

mongo_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER}/?retryWrites=true&w=majority&appName=Cluster0"


client = MongoClient(mongo_uri)
mongo_db = client[MONGO_DBNAME]
# -- caminhos
COUNTY_FOLDER = "data/county_partitioned_states"
FINAL_CSV = "data/processed/final.csv"
FASTFOOD_CSV = "data/augmented/fastfood_augmented.csv"

client = MongoClient(mongo_uri)
mongo_db = client[MONGO_DBNAME]
# -- Import county obesity CSV files
print("\n📁 Importação dos counties...\n")

for file in os.listdir(COUNTY_FOLDER):
    if file.endswith(".csv"):
        path = os.path.join(COUNTY_FOLDER, file)
        try:
            df = pd.read_csv(path)
            df.columns = [col.strip().lower() for col in df.columns]
            records = df.to_dict("records")
            mongo_db["county_obesity_data"].insert_many(records)
            print(f"[✔] County importado: {file}")
        except Exception as e:
            print(f"[⚠️] Erro ao importar {file}: {e}")

# -- Import final.csv
print("\n📁 Importação do final.csv...\n")

try:
    df_final = pd.read_csv(FINAL_CSV)
    df_final.columns = [col.strip().lower() for col in df_final.columns]
    mongo_db["obesity_fastfood"].delete_many({})  # Clear collection before insert
    mongo_db["obesity_fastfood"].insert_many(df_final.to_dict("records"))
    print("[✔] final.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar final.csv: {e}")

# -- Import fastfood_augmented.csv
print("\n📁 Importação do fastfood_augmented.csv...\n")

try:
    df_ff = pd.read_csv(FASTFOOD_CSV)
    df_ff.columns = [col.strip().lower() for col in df_ff.columns]
    mongo_db["fastfood_augmented"].delete_many({})
    mongo_db["fastfood_augmented"].insert_many(df_ff.to_dict("records"))
    print("[✔] fastfood_augmented.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar fastfood_augmented.csv: {e}")