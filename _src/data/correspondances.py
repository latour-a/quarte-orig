"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import os, var
import numpy as np
import pandas as pd
from utils import get

# Download data:
filename = get('http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2015/txt/france2015.zip')

# Get data as a dataframe:
data = pd.read_csv(filename[0], sep='\t',
    encoding='cp1252', dtype={'DEP': str, 'COM': str, 'POLE': str})

# Add/format some information:
data['NOM'] = data['NCCENR'] + data['ARTMIN']\
    .map(lambda x: ' ' + x if type(x) == str else '')

# Recreate INSEE code (5 digits) and handle 'métropole' and 'outre-mer':
data['CODGEO'] = '0'
conv2 = lambda x: '0' * (2 - len(x)) + x
conv3 = lambda x: '0' * (3 - len(x)) + x
mask_met = data['DEP'].map(lambda x: len(x) < 3)
data.loc[mask_met, 'CODGEO'] = data.loc[mask_met, 'DEP'].map(conv2)\
    + data.loc[mask_met, 'COM'].map(conv3)
mask_om = -mask_met
data.loc[mask_om, 'CODGEO'] = data.loc[mask_om, 'DEP'].map(conv3)\
    + data.loc[mask_om, 'COM'].map(conv2)

# Drop useless columns:
data.drop(['ARTMAJ', 'NCC', 'ARTMIN', 'NCCENR', 'ARTICLCT', 'NCCCT', 'CHEFLIEU',
    'RANG', 'CDC', 'TNCC'], axis=1, inplace=True)

# Drop Mayotte, which is (surprisingly) missing from several INSEE data files:
mayotte = (data['DEP'] == '976')
data.drop(data[mayotte].index, inplace=True)

# Split data between the current communes (existing in 2015) and the old ones
# (not existing anymore due to fusion/split/etc):
mask = (data['ACTUAL'] == 1) | (data['ACTUAL'] == 5) | (data['ACTUAL'] == 6)
current = data[mask].copy()
previous = data[-mask].copy()

# For Paris, Marseille, Guyane and Martinique which do not have 'cantons', we
# use the 'arrondissements départementaux' as proxy:
current.loc[:, 'CT'] = current['CT'].map(float)
mask = np.isnan(current['CT'])
current.loc[mask, 'CT'] = current.loc[mask, 'AR']

# Ensure that some data are stored as strings:
current.loc[:, 'CT'] = current['CT']\
    .map(lambda x: str(int(x)) if type(x) != str else x)

# Recreate the full canton code:
current['CODCAN15'] = current['DEP'].map(conv2) + current['CT'].map(conv2)

# Drop some more columns, now useless:
current.drop(['ACTUAL', 'COM', 'AR', 'CT', 'MODIF', 'POLE'], axis=1,
    inplace=True)
previous.drop(['COM', 'AR', 'CT', 'MODIF'], axis=1, inplace=True)

# Create an appropriate index (use a multiindex for existing communes because
# some communes are splitted between several cantons):
current.set_index(['DEP', 'CODCAN15', 'CODGEO'], inplace=True)
current.sort_index(inplace=True)
previous.set_index('CODGEO', inplace=True)

# Save to csv (with an appropriate encoding!):
current.to_csv(os.path.join(var.path['out'],
    'correspondances_communes2015.csv'), sep=';', encoding='utf-8')
previous.to_csv(os.path.join(var.path['out'],
    'correspondances_anciennes_communes.csv'), sep=';', encoding='utf-8')
