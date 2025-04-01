from faker import Faker
import random
import pandas as pd
import os


# load
original_df = pd.read_csv("BigData/processed_datasets/fastfood.csv")


# 
fake = Faker('en_US')

# num_restaurantes
num_new_restaurants = 10000  

# fake 
fake_brands = [
    "Burger Town", "Fry King", "Taco Planet", "Pizza Express",
    "Grill Master", "Hot Chicken USA", "Wrap & Roll", "Donut Haven",
    "Veggie Bites", "Sub Stop", "Waffle World"
]

# ir buscar lista de estados
states = original_df['state'].unique().tolist()

# criar entradas
new_data = []
for _ in range(num_new_restaurants):
    state = random.choice(states)
    name = random.choice(fake_brands)
    
    # coordenadas
    latitude = round(random.uniform(24.396308, 49.384358), 6)
    longitude = round(random.uniform(-125.0, -66.93457), 6)
    
    new_data.append([name, state, latitude, longitude])

# criar df
synthetic_df = pd.DataFrame(new_data, columns=['name', 'state', 'latitude', 'longitude'])

# concatenar com df original
combined_df = pd.concat([original_df, synthetic_df], ignore_index=True)

# novo csv
combined_df.to_csv("fastfood_augmented.csv", index=False)  # Optional

# preview
print(combined_df.head())
print(f"Original size: {len(original_df)}")
print(f"New total size: {len(combined_df)}")

# output folder
output_folder = os.path.join(os.path.dirname(__file__), "../augmentation_datasets")
output_file = os.path.join(output_folder, "fastfood_augmented.csv")

os.makedirs(output_folder, exist_ok=True)

# guardar csv
combined_df.to_csv(output_file, index=False)

print(f"Augmented dataset saved to: {output_file}")