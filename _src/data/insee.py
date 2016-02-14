"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import os, var, json
import pandas as pd
from utils import get, InseeXLSWrapper

# Retrieve the instructions file:
with open('download_insee.json', 'r', encoding='utf8') as instrf:
    instructions = json.load(instrf)

# Get the mapper between communes and cantons:
correspondances = pd.read_csv(os.path.join(var.path['out'],
    'correspondances_communes2015.csv'), sep=';', index_col=[0, 1, 2])

#TODO
#HACK
#FIXME

# Convert data from xls to dataframes and store them into an HDF file:
with pd.HDFStore(os.path.join(var.path['out'], 'insee.h5'), 'w') as store:
    full_header = None
    for key, source in instructions.items():
        # Indicates which file is being processed:
        print('\n' + key + '\n' + '-' * len(key))
        source['file'] = get(source['link'])[0]

        # Get a df for each file:
        print(" * Processing XLS file %s" % source['file'])
        wrapper = InseeXLSWrapper(source)

        # Drop data for communes that do not exist anymore:
        wrapper.drop_ancient_communes(correspondances)

        # Due to the 'statistical secret' (see 'Secret statistique' on
        # http://www.insee.fr), some data may be missing for some communes:
        print(" * Replacing missing values")
        wrapper.fill_nans(correspondances)

        # Put into store:
        store.put(source['h5_key'], wrapper.table, format='fixed')

        # Update the list of variables:
        if full_header is None:
            full_header = wrapper.header
        else:
            full_header = pd.concat([full_header, wrapper.header])

    # Add this to the file only at the end, in order to avoid issues with
    # variable length strings:
    store.put('keys', full_header, format='fixed')
