import pandas as pd
import os
import random
from faker import Faker

fake = Faker('en_US')

# load
df = pd.read_csv("BigData/processed_datasets/final.csv")


output_folder = "BigData/county_partitioned_states"
os.makedirs(output_folder, exist_ok=True)

# Settings
counties_per_state = 50
obesity_variation = 2.0         
density_variation = 0.05        

# gerar counties
for _, row in df.iterrows():
    state = row["State"]
    avg_obesity = row["Obesity_index"]
    avg_density = row["fast_food_density"]

    counties = set()
    while len(counties) < counties_per_state:
        county_name = fake.city() + " County"
        counties.add(county_name)

    county_data = []
    for county in counties:
        obesity = round(random.uniform(avg_obesity - obesity_variation, avg_obesity + obesity_variation), 2)
        density = round(random.uniform(avg_density - density_variation, avg_density + density_variation), 4)
        county_data.append({
            "County": county,
            "State": state,
            "Obesity_index": max(0, min(obesity, 100)),
            "fast_food_density": max(0, density)
        })

    county_df = pd.DataFrame(county_data)
    file_path = os.path.join(output_folder, f"{state.replace(' ', '_').lower()}.csv")
    county_df.to_csv(file_path, index=False)

print("✅ County-level CSVs saved to:", output_folder)
