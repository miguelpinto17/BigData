import os
import pandas as pd
from sqlalchemy import create_engine, text

# -- Configuração do SQL Server
DB_SERVER = "localhost\\SQLEXPRESS"   # or just "localhost", depending on your setup
DB_NAME = "ObesityDatabase"
DB_USERNAME = "your_username"
DB_PASSWORD = "your_password"
DRIVER = "ODBC Driver 17 for SQL Server"

# -- Caminhos
COUNTY_FOLDER = "data/county_partitioned_states"
FINAL_CSV = "data/processed/final.csv"
FASTFOOD_CSV = "data/augmented/fastfood_augmented.csv"

# -- Conexão SQL Server
connection_string = (
    f"mssql+pyodbc://{DB_USERNAME}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}"
    f"?driver={DRIVER.replace(' ', '+')}"
)
engine = create_engine(connection_string)

# -- Criação de tabelas
with engine.connect() as conn:
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='county_obesity_data' AND xtype='U')
        CREATE TABLE county_obesity_data (
            county VARCHAR(255),
            state VARCHAR(255),
            obesity_index FLOAT,
            fast_food_density FLOAT
        );
    """))

    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='obesity_fastfood' AND xtype='U')
        CREATE TABLE obesity_fastfood (
            state VARCHAR(255),
            obesity_index FLOAT,
            fast_food_density FLOAT
        );
    """))

    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fastfood_augmented' AND xtype='U')
        CREATE TABLE fastfood_augmented (
            name VARCHAR(255),
            state VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
        );
    """))

# -- Importação dos counties
print("\n📁 Importação dos counties...\n")

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

# -- Importar final.csv
print("\n📁 Importação do final.csv...\n")

try:
    df_final = pd.read_csv(FINAL_CSV)
    df_final.columns = [col.strip().lower() for col in df_final.columns]
    df_final.to_sql("obesity_fastfood", engine, if_exists="replace", index=False)
    print("[✔] final.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar final.csv: {e}")

# -- Importar fastfood_augmented.csv
print("\n📁 Importação do fastfood_augmented.csv...\n")

try:
    df_ff = pd.read_csv(FASTFOOD_CSV)
    df_ff.columns = [col.strip().lower() for col in df_ff.columns]
    df_ff.to_sql("fastfood_augmented", engine, if_exists="replace", index=False)
    print("[✔] fastfood_augmented.csv importado com sucesso.")
except Exception as e:
    print(f"[⚠️] Erro ao importar fastfood_augmented.csv: {e}")