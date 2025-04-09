import os
import pandas as pd
from sqlalchemy import create_engine, text  

# -- configuracao bd
DB_USER = "postgres"
DB_PASSWORD = "#######"  # - coloquem a palavra passe da vossa bd!
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "bigdata"

# -- caminhos
COUNTY_FOLDER = "data/county_partitioned_states"
FINAL_CSV = "data/processed/final.csv"
FASTFOOD_CSV = "data/augmented/fastfood_augmented.csv"

# -- conexao
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --criacao tabelas bd
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS county_obesity_data (
            county VARCHAR,
            state VARCHAR,
            obesity_index FLOAT,
            fast_food_density FLOAT
        );
    """))
    print("[✔] Tabela 'county_obesity_data' pronta.")

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS obesity_fastfood (
            state VARCHAR,
            obesity_index FLOAT,
            fast_food_density FLOAT
        );
    """))
    print("[✔] Tabela 'obesity_fastfood' pronta.")

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fastfood_augmented (
            name VARCHAR,
            state VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        );
    """))
    print("[✔] Tabela 'fastfood_augmented' pronta.")

# --importacao de dados dos counties
print("\n Importação dos counties...\n")

for file in os.listdir(COUNTY_FOLDER):
    if file.endswith(".csv"):
        path = os.path.join(COUNTY_FOLDER, file)
        try:
            df = pd.read_csv(path)
            df.columns = [col.strip().lower() for col in df.columns]
            df.to_sql("county_obesity_data", engine, if_exists="append", index=False)
            print(f"[✔] County importado: {file}")
        except Exception as e:
            print(f"[⚠️] Erro ao importar {file}: {e}")

# --importar final.csv
print("\n📁 Importação do final.csv...\n")

try:
    df_final = pd.read_csv(FINAL_CSV)
    df_final.columns = [col.strip().lower() for col in df_final.columns]
    df_final.to_sql("obesity_fastfood", engine, if_exists="replace", index=False)
    print("[✔] final.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar final.csv: {e}")

# --importar fastfood_augmented.csv
print("\n📁 Importação do fastfood_augmented.csv...\n")

try:
    df_ff = pd.read_csv(FASTFOOD_CSV)
    df_ff.columns = [col.strip().lower() for col in df_ff.columns]
    df_ff.to_sql("fastfood_augmented", engine, if_exists="replace", index=False)
    print("[✔] fastfood_augmented.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar fastfood_augmented.csv: {e}")
