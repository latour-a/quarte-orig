"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import pandas as pd

def get_results0(df, names):
    print(df.head())
    nvoters = df['_GAL']['Inscrits']
    res = df[names.pop()].copy()
    for name in names:
        res = res.add(df[name], fill_value=0.)
    return pd.concat([res, nvoters], axis=1)

def get_results(df, names):
    nvoters = df['_GAL']['Inscrits']
    res = df[names.pop()]['VOIX'].copy()
    for name in names:
        res = res.add(df[name]['VOIX'], fill_value=0.)
    return res / nvoters

raise ValueError("pondérer l'OLS par la population")

centre = {}
with pd.HDFStore('data/output/elections.h5', 'r') as store:
    # En tenant compte des alliances :
    centre['pre2002'] = get_results(store['/presidentielle2002_tour1'],
        ['BAYR', 'MADE'])
    centre['leg2002'] = get_results(store['/legislatives2002_tour1'],
        ['UDF', 'DL'])
    centre['pre2007'] = get_results(store['/presidentielle2007_tour1'],
        ['BAYR'])
    centre['pre2012'] = get_results(store['/presidentielle2012_tour1'],
        ['BAYR'])
    centre['leg2012'] = get_results(store['/legislatives2012_tour1'],
        ['CEN', 'NCE', 'PRV'])
    centre['mun2014'] = get_results(store['/municipales2014_tour1'],
        ['LMDM', 'LUD', 'LUDI'])
    centre['eur2014'] = get_results(store['/europeennes2014'],
        ['LUC'])
centre = pd.concat(centre, axis=1)

with pd.HDFStore('data/output/insee.h5', 'r') as store:
    # Nombre de personnes non scolarisées de 15 ans ou plus titulaires d'un
    # diplôme de l'enseignement supérieur long:
    popl = store['diplomes2011'][['P11_POP0205', 'P11_POP0610', 'P11_POP1114',
        'P11_POP1517', 'P11_POP1824', 'P11_POP2529', 'P11_POP30P']].sum(axis=1)
    dipl = store['diplomes2011']['P11_NSCOL15P_SUP']
    all_dipl = store['diplomes2011'][['P11_NSCOL15P_DIPL0', 'P11_NSCOL15P_CEP',
        'P11_NSCOL15P_BEPC', 'P11_NSCOL15P_CAPBEP', 'P11_NSCOL15P_BAC',
        'P11_NSCOL15P_BACP2', 'P11_NSCOL15P_SUP']]
    pdipl = dipl.div(popl, axis=0)
    pall_dipl = all_dipl.div(popl, axis=0)
correl = {col: ser.corr(pdipl) for col, ser in centre.iteritems()}

pd.stats.ols.OLS(centre['pre2007'], pall_dipl)

xyz = pd.concat([centre, pall_dipl], axis=1)
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
fig = plt.figure()
ax = fig.add_subplot(111)
ax.scatter(xyz['P11_NSCOL15P_SUP'], xyz['pre2007'])
fig.show()

bla = centre['pre2007'][centre['pre2007'] > 1.]
with pd.HDFStore('data/output/elections.h5', 'r') as store:
    # En tenant compte des alliances :
    blup = store['/presidentielle2007_tour1']
    test = get_results0(store['/presidentielle2007_tour1'], ['BAYR'])
