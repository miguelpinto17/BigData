import pandas as pd
import numpy as np

def merge_datasets(dataset_list: list):
    pass

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

#TODO: make sure all these transformations are necessary.
if __name__ == "__main__":
    
    # read datasets
    ff_data = pd.read_csv('datasets/fastfood.csv')
    ob_data = pd.read_csv('datasets/obesity.csv')

    # process fastfood dataset
    # drop country
    ff_data.drop(columns=['country'], inplace=True)
    # Convert postalCode to string to avoid precision issues
    ff_data['postalCode'] = ff_data['postalCode'].astype(str).str.replace('.0', '', regex=False)
    # missing values #TODO: should not be substituted by a variable of different type. Should be null
    ff_data['postalCode'].fillna('Unknown', inplace=True)
    # missing values
    ff_data['websites'].fillna('Not Available', inplace=True)
    # normalize US states
    ff_data['province'] = ff_data['province'].replace(US_states_mapping) #TODO not understanding this. are we not repeating?
    ff_data.rename(columns={'province': 'state'}, inplace=True)
    state_abbr_to_name = {v: k for k, v in US_states_mapping.items()}  # Swap keys and values TODO: why?
    ff_data['state'] = ff_data['state'].replace(state_abbr_to_name)
    ff_data.drop(columns=['postalCode', 'address', 'city', 'keys', 'websites'], inplace=True)
    ff_data = ff_data[['name', 'state', 'latitude', 'longitude']]
    ff_data.to_csv('processed_datasets/fastfood.csv', index=False)

    # process_ob_data
    ob_data = ob_data.rename(columns={"NAME": "State","Obesity": "Obesity_index","Shape__Area": "Area", "Shape__Length": "Perimeter"})
    ob_data.to_csv('processed_datasets/obesity.csv', index=False)

    # merge into final datasets:
    df_fastfood_grouped = ff_data.groupby('state').size().reset_index(name='fast_food_count')
    df_merged = ob_data.merge(df_fastfood_grouped, left_on='State', right_on='state', how='left')
    df_merged.drop(columns=['state'], inplace=True)
    df_merged['fast_food_density'] = df_merged['fast_food_count'] / df_merged['Area']
    
    # normalização
    df_merged['fast_food_density_normalized'] = (df_merged['fast_food_density'] - df_merged['fast_food_density'].min()) / \
                                         (df_merged['fast_food_density'].max() - df_merged['fast_food_density'].min())
    df_merged.to_csv("processed_datasets/fastfood_obesity_full.csv", index=False)

    df_final = df_merged[['State', 'Obesity_index', 'fast_food_density_normalized']].rename(
        columns={'fast_food_density_normalized': 'fast_food_density'}
    )
    df_final.to_csv("processed_datasets/final.csv", index=False)