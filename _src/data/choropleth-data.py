"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import os, var
import pandas as pd
from utils import get

# Load cantons from ‘code officiel geographique’ database
cog_fname = get('http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2015/txt/comsimp2015.zip')[0]
cog = pd.read_csv(cog_fname, sep="\t", encoding="iso-8859-1", dtype=str)
cog = pd.DataFrame(data={'canton':cog.DEP+"-"+cog.CT,'id':cog.DEP+cog.COM})
cog = cog.set_index('id')
# Perform some weird voodoo ("in order to make it work", according to SD):
cog = cog[cog.canton.str[-2:].fillna("99").astype('int') < 50]
