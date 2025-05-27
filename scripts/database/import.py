import os
import pandas as pd
from pymongo import MongoClient

# -- Configuração MongoDB LOCAL (Docker ou Compass)

mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)
mongo_db = client["ObesityDatabase"]

# -- Caminhos dos ficheiros
COUNTY_FOLDER = "data/county_partitioned_states"
FINAL_CSV = "data/processed/final.csv"
FASTFOOD_CSV = "data/processed/fastfood_cleaned.csv"

# -- Importação dos ficheiros CSV por county
print("\n📁 Importação dos counties...\n")
if not os.path.exists(COUNTY_FOLDER):
    print(f"[❌] Pasta não encontrada: {COUNTY_FOLDER}")
else:
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

# -- Importação do final.csv
print("\n📁 Importação do final.csv...\n")
try:
    df_final = pd.read_csv(FINAL_CSV)
    df_final.columns = [col.strip().lower() for col in df_final.columns]
    mongo_db["obesity_fastfood"].delete_many({})
    mongo_db["obesity_fastfood"].insert_many(df_final.to_dict("records"))
    print("[✔] final.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar final.csv: {e}")

# -- Importação do fastfood_cleaned.csv
print("\n📁 Importação do fastfood_cleaned.csv...\n")
try:
    df_ff = pd.read_csv(FASTFOOD_CSV)
    df_ff.columns = [col.strip().lower() for col in df_ff.columns]
    mongo_db["fastfood_cleaned"].delete_many({})
    mongo_db["fastfood_cleaned"].insert_many(df_ff.to_dict("records"))
    print("[✔] fastfood_cleaned.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar fastfood_cleaned.csv: {e}")
