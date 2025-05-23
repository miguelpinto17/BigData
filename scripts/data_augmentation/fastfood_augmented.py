from faker import Faker
import random
import pandas as pd
from pathlib import Path

# ========== Inicializações ==========
fake = Faker('en_US')
random.seed(42)
Faker.seed(42)

fake_brands = [
    "Burger Town", "Fry King", "Taco Planet", "Pizza Express",
    "Grill Master", "Hot Chicken USA", "Wrap & Roll", "Donut Haven",
    "Veggie Bites", "Sub Stop", "Waffle World"
]

num_new_restaurants = 10000

# ========== Paths ==========
BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = BASE_DIR / "data" / "processed" / "fastfood_cleaned.csv"
OUTPUT_DIR = BASE_DIR / "data" / "augmented"
OUTPUT_FILE = OUTPUT_DIR / "fastfood_augmented.csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ========== Carregar original ==========
original_df = pd.read_csv(INPUT_FILE)
states = original_df['state'].unique().tolist()

# ========== Gerar dados sintéticos ==========
new_data = []
for _ in range(num_new_restaurants):
    state = random.choice(states)
    name = random.choice(fake_brands)
    latitude = round(random.uniform(24.396308, 49.384358), 6)
    longitude = round(random.uniform(-125.0, -66.93457), 6)
    new_data.append([name, state, latitude, longitude])

synthetic_df = pd.DataFrame(new_data, columns=['name', 'state', 'latitude', 'longitude'])

# ========== Combinar e guardar ==========
combined_df = pd.concat([original_df, synthetic_df], ignore_index=True)
combined_df.to_csv(OUTPUT_FILE, index=False)

# ========== Feedback ==========
print(combined_df.head())
print(f"✅ Original size: {len(original_df)}")
print(f"✅ New total size: {len(combined_df)}")
print(f"✅ Augmented dataset saved to: {OUTPUT_FILE}")
