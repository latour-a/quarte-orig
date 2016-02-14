"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import os, var
import pandas as pd
from utils import get, fmt_elections, grouper,\
    rectangular_csv, read_csv, read_csv_with_header

dtype_mapper = {
    'Code du département': str,
    'Code département': str,
    'Code de la commune': str,
    'Code commune': str,
    'N° de bureau de vote': str,
    'Code du b.vote': str
}

cols_mapper = {
    'Code du département':           'DEP',
    'Code département':              'DEP',
    'Code de la commune':            'COM',
    'Code commune':                  'COM',
    'N° de bureau de vote':          'BV',
    'Code du b.vote':                'BV',
    'Code nuance du candidat':       'CODNUA',
    'Code nuance de la liste':       'CODNUA',
    'Code Nuance':                   'CODNUA',
    'Nom':                           'NOMCAND',
    'Nom du candidat':               'NOMCAND',
    'Nom du candidat tête de liste': 'NOMCAND',
    'Nombre de voix du candidat':    'VOIX',
    'Nombre de voix':                'VOIX',
    'Voix':                          'VOIX'
}

cols_to_drop = ['N° tour', 'Nom de la commune', 'N° de dépôt du candidat',
        'Prénom du candidat', 'N° tour', 'Nom de la commune',
        'N° de dépôt de la liste', 'Prénom du candidat tête de liste',
        u"Date de l'export", 'Libellé du département',
        'Libellé de la commune', '% Abs/Ins', '% Vot/Ins', '% BlNuls/Ins',
        '% BlNuls/Vot', '% Exp/Ins', '% Exp/Vot', 'Sexe', 'Prénom',
        'Liste', 'Sieges', u'% Voix/Ins', u'% Voix/Exp', 'Abstentions',
        'Blancs et nuls']

#FIXME: si les données disponibles étaient plus uniformes, on pourrait
# automatiser davantage tout ceci.

# Export the data as several dataframes:
with pd.HDFStore(os.path.join(var.path['out'], 'elections.h5'), 'w') as store:
    # European elections, 2014:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees/elections/2014_europeennes/europ%C3%A9ennes-2014-r%C3%A9sultats-bureaux_vote-tour1.csv")
    store.put('europeennes2014', fmt_elections(read_csv(fname[0], cols_mapper,
        cols_to_drop, dtype_mapper)), format='fixed')

    # Municipal elections, second turn, 2014:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees/elections/2014_municipales/municipales-2014-r%C3%A9sultats-bureaux_vote-tour2.csv")
    store.put('municipales2014_tour2', fmt_elections(read_csv(fname[0],
        cols_mapper, cols_to_drop, dtype_mapper)), format='fixed')

    # Municipal elections, first turn, 2014:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees/elections/2014_municipales/municipales-2014-r%C3%A9sultats-bureaux_vote-tour1.csv")
    store.put('municipales2014_tour1', fmt_elections(read_csv(fname[0],
        cols_mapper, cols_to_drop, dtype_mapper)), format='fixed')

    # Legislative elections, 2012:
    fname = get("http://www.nosdonnees.fr/storage/f/2013-03-05T184148/LG12_BV_T1T2.zip")
    store.put('legislatives2012_tour2', fmt_elections(read_csv(fname[1],
        cols_mapper, cols_to_drop, dtype_mapper, encoding='cp1252')),
        format='fixed')
    store.put('legislatives2012_tour1', fmt_elections(read_csv(fname[0],
        cols_mapper, cols_to_drop, dtype_mapper, encoding='cp1252')),
        format='fixed')

    # Presidential election, 2012:
    fname = get("http://www.nosdonnees.fr/dataset/10de0dd8-f67d-473d-a625-44e2bb33f685/resource/84ecf1c6-f487-4bc4-a689-31ad1ded5dd2/download/pr12bvt1t2.zip")
    store.put('presidentielle2012_tour2', fmt_elections(read_csv(fname[1],
        cols_mapper, cols_to_drop, dtype_mapper, encoding='cp1252')),
        format='fixed')
    store.put('presidentielle2012_tour1', fmt_elections(read_csv(fname[0],
        cols_mapper, cols_to_drop, dtype_mapper, encoding='cp1252')),
        format='fixed')

    # Municipal elections, second turn, 2008:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees/elections/2008_municipales/municipales-2008-r%c3%a9sultats-bureaux_vote-tour2.csv")
    fname = rectangular_csv(fname[0])
    store.put('municipales2008_tour2', fmt_elections(read_csv(fname,
        cols_mapper, cols_to_drop, dtype_mapper)), format='fixed')

    # Municipal elections, first turn, 2008:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees/elections/2008_municipales/municipales-2008-r%c3%a9sultats-bureaux_vote-tour1.csv")
    fname = rectangular_csv(fname[0])
    store.put('municipales2008_tour1', fmt_elections(read_csv(fname,
        cols_mapper, cols_to_drop, dtype_mapper)), format='fixed')

    # Presidential election, 2007:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees_publiques/Donnees_Presidentielles_2007_Officielles_cvs.zip")
    store.put('presidentielle2007_tour2', fmt_elections(read_csv_with_header(
        fname[1])), format='fixed')
    store.put('presidentielle2007_tour1', fmt_elections(read_csv_with_header(
        fname[0])), format='fixed')

    # Legislative elections, 2002:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees_publiques/resultats_elections_legislatives_2002.tgz")
    store.put('legislatives2002_tour2', fmt_elections(read_csv_with_header(
        fname[2])), format='fixed')
    store.put('legislatives2002_tour1', fmt_elections(read_csv_with_header(
        fname[1])), format='fixed')

    # Presidential election, 2002:
    fname = get("http://www.regardscitoyens.org/telechargement/donnees_publiques/resultats_elections_presidentielles_2002.tgz")
    store.put('presidentielle2002_tour2', fmt_elections(read_csv_with_header(
        fname[1])), format='fixed')
    store.put('presidentielle2002_tour1', fmt_elections(read_csv_with_header(
        fname[2])), format='fixed')
