import os
import pandas as pd
import sqlite3

# -- Configuração do SQLite
SQLITE_DB = "obesity_data.sqlite"

# -- Caminhos
COUNTY_FOLDER = "data/county_partitioned_states"
FINAL_CSV = "data/processed/final.csv"
FASTFOOD_CSV = "data/augmented/fastfood_augmented.csv"

# -- Conexão SQLite
conn = sqlite3.connect(SQLITE_DB)
cursor = conn.cursor()

# -- Criação de tabelas (caso não existam)
cursor.execute("""
CREATE TABLE IF NOT EXISTS county_obesity_data (
    county TEXT,
    state TEXT,
    obesity_index REAL,
    fast_food_density REAL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS obesity_fastfood (
    state TEXT,
    obesity_index REAL,
    fast_food_density REAL
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS fastfood_augmented (
    name TEXT,
    state TEXT,
    latitude REAL,
    longitude REAL
);
""")

conn.commit()

# -- Importação dos counties
print("\n📁 Importação dos counties...\n")

for file in os.listdir(COUNTY_FOLDER):
    if file.endswith(".csv"):
        path = os.path.join(COUNTY_FOLDER, file)
        try:
            df = pd.read_csv(path)
            df.columns = [col.strip().lower() for col in df.columns]
            df.to_sql("county_obesity_data", conn, if_exists="append", index=False)
            print(f"[✔] County importado: {file}")
        except Exception as e:
            print(f"[⚠️] Erro ao importar {file}: {e}")

# -- Importar final.csv
print("\n📁 Importação do final.csv...\n")

try:
    df_final = pd.read_csv(FINAL_CSV)
    df_final.columns = [col.strip().lower() for col in df_final.columns]
    df_final.to_sql("obesity_fastfood", conn, if_exists="replace", index=False)
    print("[✔] final.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar final.csv: {e}")

# -- Importar fastfood_augmented.csv
print("\n📁 Importação do fastfood_augmented.csv...\n")

try:
    df_ff = pd.read_csv(FASTFOOD_CSV)
    df_ff.columns = [col.strip().lower() for col in df_ff.columns]
    df_ff.to_sql("fastfood_augmented", conn, if_exists="replace", index=False)
    print("[✔] fastfood_augmented.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar fastfood_augmented.csv: {e}")

# -- Fechar conexão
conn.close()