import pandas as pd
import random
from faker import Faker
from pathlib import Path

# Inicializar Faker
fake = Faker('en_US')

# Forçar execução relativa ao local do script
current_dir = Path(__file__).resolve().parent
os.chdir(current_dir)

# ========== Paths ==========
INPUT_FILE = current_dir.parent.parent / "data" / "processed" / "final.csv"
OUTPUT_DIR = current_dir.parent.parent / "data" / "county_partitioned_states"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ========== Configurações ==========
counties_per_state = 50
obesity_variation = 2.0        # % de variação do índice de obesidade
density_variation = 0.05       # variação da densidade fast food

# ========== Carregar dados ==========
df = pd.read_csv(INPUT_FILE)

# ========== Gerar ficheiros ==========
for _, row in df.iterrows():
    state = row["State"]
    avg_obesity = row["Obesity_index"]
    avg_density = row["fast_food_density"]

    # Gerar nomes únicos de counties
    counties = set()
    while len(counties) < counties_per_state:
        counties.add(fake.city() + " County")

    # Criar dados para os counties
    county_data = []
    for county in counties:
        obesity = round(random.uniform(avg_obesity - obesity_variation, avg_obesity + obesity_variation), 2)
        density = round(random.uniform(avg_density - density_variation, avg_density + density_variation), 4)
        county_data.append({
            "County": county,
            "State": state,
            "Obesity_index": max(0, min(obesity, 100)),  # limitar entre 0-100%
            "fast_food_density": max(0, density)
        })

    county_df = pd.DataFrame(county_data)

    # Nome do ficheiro
    output_file = OUTPUT_DIR / f"{state.replace(' ', '_').lower()}.csv"
    county_df.to_csv(output_file, index=False)

print(f"✅ County-level CSVs saved to: {OUTPUT_DIR}")
