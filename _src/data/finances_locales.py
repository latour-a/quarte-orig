"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import os, var, json
import pandas as pd
from utils import get

filename = get('http://www.nosfinanceslocales.fr/static/data/city_all.csv')

# Retrieve data from csv:
all_data = pd.read_csv(filename[0], sep=',', dtype={'code officiel '\
    'géographique (insee)': str})

# Drop useless columns:
to_drop = ['nom', 'type de zone administrative', 'url']
all_data.drop(to_drop, axis=1, inplace=True)

# Reorder the dataframe:
pivoted = all_data.pivot(index='code officiel géographique (insee)',
    columns='année')

# Define valid Python identifiers for data:
with open('finances_locales_mapper.json', 'r', encoding='utf8') as col_file:
    col_mapper = json.load(col_file)

# Export the data as several dataframes:
with pd.HDFStore(os.path.join(var.path['out'], 'finances_locales.h5'), 'w') as store:
    for col in pivoted.columns.levels[0]:
        # Use a valid Python identifier as key:
        key = col_mapper[col]
        store.put(key, pivoted[col], format='fixed')
        # Store the full name of the df as metadata:
        store.get_storer(key).attrs.metadata = col
