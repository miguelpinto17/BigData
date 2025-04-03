import pandas as pd
import numpy as np
from pathlib import Path

# ========== Constantes ==========
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

US_states_mapping = {  
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO',  
    'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',  
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',  
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',  
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',  
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',  
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',  
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA',  
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'  
}

# ========== Main ==========
if __name__ == "__main__":

    # === Carregar datasets ===
    ff_data = pd.read_csv(RAW_DIR / 'fastfood.csv')
    ob_data = pd.read_csv(RAW_DIR / 'obesity.csv')

    # === Limpeza: Fast Food ===
    ff_data.drop(columns=['country'], inplace=True)

    ff_data['postalCode'] = (
        ff_data['postalCode']
        .fillna(np.nan)  # Melhor que string "Unknown"
        .astype('string')
        .str.replace('.0', '', regex=False)
    )

    ff_data['websites'] = ff_data['websites'].fillna(np.nan)

    # Normalizar abreviações -> nomes completos dos estados
    ff_data['province'] = ff_data['province'].replace(US_states_mapping)
    ff_data.rename(columns={'province': 'state'}, inplace=True)
    ff_data['state'] = ff_data['state'].replace({v: k for k, v in US_states_mapping.items()})  # Abrev → Nome

    # Remover colunas irrelevantes
    ff_data.drop(columns=['postalCode', 'address', 'city', 'keys', 'websites'], inplace=True)
    ff_data = ff_data[['name', 'state', 'latitude', 'longitude']]

    # Guardar dataset limpo
    ff_data.to_csv(PROCESSED_DIR / 'fastfood.csv', index=False)

    # === Limpeza: Obesidade ===
    ob_data.rename(columns={
        "NAME": "State",
        "Obesity": "Obesity_index",
        "Shape__Area": "Area",
        "Shape__Length": "Perimeter"
    }, inplace=True)
    ob_data.to_csv(PROCESSED_DIR / 'obesity.csv', index=False)

    # === Merge e cálculo ===
    grouped_ff = ff_data.groupby('state').size().reset_index(name='fast_food_count')
    df_merged = ob_data.merge(grouped_ff, left_on='State', right_on='state', how='left')
    df_merged.drop(columns=['state'], inplace=True)

    df_merged['fast_food_density'] = df_merged['fast_food_count'] / df_merged['Area']

    # Normalização (min-max)
    df_merged['fast_food_density_normalized'] = (
        (df_merged['fast_food_density'] - df_merged['fast_food_density'].min()) /
        (df_merged['fast_food_density'].max() - df_merged['fast_food_density'].min())
    )

    df_merged.to_csv(PROCESSED_DIR / "fastfood_obesity_full.csv", index=False)

    # Criar final.csv (usado nos benchmarks)
    df_final = df_merged[['State', 'Obesity_index', 'fast_food_density_normalized']].rename(
        columns={'fast_food_density_normalized': 'fast_food_density'}
    )
    df_final.to_csv(PROCESSED_DIR / "final.csv", index=False)

    print("✅ Dados processados com sucesso e salvos na pasta 'data/processed'")
