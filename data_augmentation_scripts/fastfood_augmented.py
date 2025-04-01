from faker import Faker
import random
import pandas as pd
import os


# Load your original dataset
original_df = pd.read_csv("BigData/processed_datasets/fastfood.csv")


# Initialize Faker
fake = Faker('en_US')

# Define number of synthetic entries to create
num_new_restaurants = 10000  # You can change this to any number

# Define some fake fast food chain names to diversify the dataset
fake_brands = [
    "Burger Town", "Fry King", "Taco Planet", "Pizza Express",
    "Grill Master", "Hot Chicken USA", "Wrap & Roll", "Donut Haven",
    "Veggie Bites", "Sub Stop", "Waffle World"
]

# Extract the list of U.S. states from the original dataset
states = original_df['state'].unique().tolist()

# Create synthetic entries
new_data = []
for _ in range(num_new_restaurants):
    state = random.choice(states)
    name = random.choice(fake_brands)
    
    # Simulate coordinates within rough U.S. bounds
    latitude = round(random.uniform(24.396308, 49.384358), 6)
    longitude = round(random.uniform(-125.0, -66.93457), 6)
    
    new_data.append([name, state, latitude, longitude])

# Create DataFrame with the new entries
synthetic_df = pd.DataFrame(new_data, columns=['name', 'state', 'latitude', 'longitude'])

# Concatenate with the original dataset
combined_df = pd.concat([original_df, synthetic_df], ignore_index=True)

# Save to a new CSV if needed
combined_df.to_csv("fastfood_augmented.csv", index=False)  # Optional

# Optional: show preview
print(combined_df.head())
print(f"Original size: {len(original_df)}")
print(f"New total size: {len(combined_df)}")

# Define the output folder and file name
output_folder = os.path.join(os.path.dirname(__file__), "../augmentation_datasets")
output_file = os.path.join(output_folder, "fastfood_augmented.csv")

# Create the folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Save the augmented CSV
combined_df.to_csv(output_file, index=False)

print(f"Augmented dataset saved to: {output_file}")