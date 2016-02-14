"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import os
import re
import var
import string
import tarfile
import zipfile
import warnings
import itertools
import numpy as np
import pandas as pd
import xlrd
import urllib.request


def get(url):
    """Download and extract files, then return list of filenames.
    """
    print(" * Downloading http://" + url)
    tmp = var.path['tmp']
    path = os.path.join(tmp, url.split('/')[-1])
    urllib.request.urlretrieve(url, path)

    if path.endswith('.zip'):
        with zipfile.ZipFile(path) as archive:
            archive.extractall(tmp)
            return [os.path.join(tmp, f) for f in archive.namelist()]

    elif path.endswith('.tgz'):
        with tarfile.open(path, "r:gz") as archive:
            archive.extractall(tmp)
            return [os.path.join(tmp, f) for f in archive.getnames()]

    return [path]

def grouper(n, iterable, fillvalue=None):
    """Return an iterator over `n`-sized subsets of `iterable`. Found on:
    https://docs.python.org/3.4/library/itertools.html#itertools-recipes.

    Example:
    --------
    >>> grouper(3, 'ABCDEFG', 'x')
        ABC DEF Gxx
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(fillvalue=fillvalue, *args)

def rectangular_csv(fname, nb_common_fields=17):
    """Format a misshaped CSV file (some lines contain too many commas/fields)
    to a "rectangular" CSV (each line contains the same number of fields) -an
    exemple of a misshaped file and of the corresponding rectangular file is
    given below. Return the name of the new CSV file.

    Parameters:
    -----------
    - fname: name of the CSV file containing misshaped data.
    - nb_common_fields, int, default 17: the first fields of each line are
        assumed to be common to all data on the same line.

    Example:
    --------
    >>> fin_content = 'common1,common2,field1,field2,,\n'\
        + 'AA,BB,foo,1.105,bar,0.987,baz,2.046\n'\
        + 'CC,AD,muh,0.145,goo,3.648'
    >>> with open('fin', 'w') as fin: fin.write(fin_content)
    >>> fout = format_file('fin', nb_common_fields=2)
    >>> with open(fout, 'r') as f: print(f.read())
        common1,common2,field1,field2
        AA,BB,foo,1.105
        AA,BB,bar,0.987
        AA,BB,baz,2.046
        CC,AD,muh,0.145
        CC,AD,goo,3.648
    """
    head, tail = os.path.splitext(fname)
    newname = head + '_formatted' + tail
    with open(fname, 'r', encoding='utf8') as fin, open(newname, 'w',
    encoding='utf8') as fout:
        header = fin.readline()
        fout.write(header.replace(',', ';'))
        # Other lines:
        for line in fin:
            line = line[:-1] # Drop line termination.
            line = line.replace('""', '"') # Handle some special cases.
            # Some lines contain commas that should be NOT used as separators...
            for r in re.findall(r'"[^"]*"', line):
                line = line.replace(r, r.replace(',', ''))
            line = line.replace('"', '') # Quotes are now useless, drop them.
            data = line.split(',')
            common = data[:nb_common_fields]
            for group in grouper(9, data[nb_common_fields:]):
                fout.write(';'.join(common + list(group)) + '\n')
    return newname

def read_csv(fname, cols_mapper, cols_to_drop, dtype, encoding='utf8'):
    """Read the content of the CSV file `fname` as a pandas.DataFrame.

    Parameters:
    -----------
    - fname: path of the file to read.
    - cols_mapper: dict of existing columns names -> new names.
    - cols_to_drop: list of columns to drop.
    - dtype: type name or dict of columns -> type. Data type for data or
        columns.
    - encoding, default 'utf8': the encoding of the file `fname`.
    """
    df = pd.read_csv(fname, sep=';', encoding=encoding, dtype=dtype)
    # Drop useless columns:
    for col in cols_to_drop:
        try:
            df.drop(col, axis=1, inplace=True)
        except ValueError:
            # col may not exist in df.columns:
            pass
    # Use labels easier to work with:
    df.rename(columns=cols_mapper, inplace=True)
    return df

def read_csv_with_header(fname, encoding='utf8'):
    """Use this function to read some very specific CSV files found on
    NosDonnées.fr, which all contain a weirdly shaped header.

    Parameters:
    -----------
    - fname: path of the file to read.
    - encoding, default 'utf8': the encoding of the file `fname`.
    """
    names = ['NTOUR', 'DEP', 'COM', 'NOMCOM', 'BV', 'Inscrits', 'Votants',
        u'Exprimés', 'NCAND', 'NOMCAND', 'PRECAND', 'CODNUA', 'VOIX']
    dtype = {'NOMCOM': str, 'NOMCAND': str, 'CODNUA': str, 'DEP': str,
        'COM': str, 'BV': str, 'NTOUR': str}
    df = pd.read_csv(fname, sep=';', encoding=encoding, skiprows=17,
        header=None, names=names, dtype=dtype)
    # One file (PR02_T1_BVot.csv) contains some sort of misplaced comments:
    df = df[-df['DEP'].isnull()]
    df.drop(['NTOUR', 'NOMCOM', 'NCAND', 'PRECAND'], axis=1, inplace=True)
    return df

def fmt_elections(df, copy=False):
    """Format `df`, a dataframe containing elections results as given by
    NosDonnées.fr. Return a new, adequately formatted dataframe.

    Remark: unless `copy` is set to `True` this function *modifies* the argument
    `df`. Please note that, even modified, `df` is *not* the result of the
    function.

    Parameters:
    -----------
    - df, pandas.DataFrame: contains results for a given election. Each line
        correspond to the score achieved by one candidate in a given polling
        station.
    - copy, boolean, default False: indicates wether the function works on `df`
        or on a copy of it. Copying `df` means that it won't be changed by the
        function, but it can be time and memory consuming.
    """
    if copy:
        df = df.copy()

    # Recreate INSEE code (5 digits) and handle 'métropole' and 'outre-mer':
    df['CODGEO'] = '0'
    conv2 = lambda x: '0' * (2 - len(x)) + x
    conv3 = lambda x: '0' * (3 - len(x)) + x
    mask_met = df['DEP'].map(lambda x: len(x) < 3)
    df.loc[mask_met, 'CODGEO'] = df.loc[mask_met, 'DEP'].map(conv2)\
        + df.loc[mask_met, 'COM'].map(conv3)
    mask_om = -mask_met
    df.loc[mask_om, 'CODGEO'] = df.loc[mask_om, 'DEP'].map(conv3)\
        + df.loc[mask_om, 'COM'].map(conv2)

    # Drop some columns, now useless:
    df.drop(['DEP', 'COM'], axis=1, inplace=True)

    # Save general informations in a separate dataframe (for now):
    general_infos = df[['CODGEO', 'BV', u'Inscrits', u'Votants', u'Exprimés']]\
        .copy()
    general_infos.set_index(['CODGEO', 'BV'], inplace=True)
    # `groupby` seems more adequate/easier to use than `drop_duplicates` here:
    general_infos = general_infos.groupby(level=general_infos.index.names)\
        .last()
    general_infos = general_infos.groupby(level=0).sum()
    general_infos.rename(columns={'Inscrits': ('_GAL', 'Inscrits'),
        'Votants': ('_GAL', 'Votants'), 'Exprimés': ('_GAL', 'Exprimés')},
        inplace=True)

    df.drop(['Inscrits', 'Votants', 'Exprimés'], axis=1, inplace=True)
    df.set_index(['CODGEO', 'NOMCAND', 'CODNUA', 'BV'], inplace=True)
    # For each 'commune', add the results of all polling stations (in order to
    # do so, use a Series rather than a dataframe):
    ser = df['VOIX'].groupby(level=[0, 1, 2]).sum()
    # Get back a dataframe:
    df = ser.reset_index()
    # Drop another (now) useless column:
    df.drop('NOMCAND', axis=1, inplace=True)
    # Prepare an appropriate index for future groupbys:
    df.set_index(['CODGEO', 'CODNUA'], inplace=True)
    # Create another dataframe to perform a different computation:
    df2 = df.copy()
    df2['NBCAND'] = 1
    df2.drop('VOIX', axis=1, inplace=True)
    # Number of votes for each political nuance:
    nvotes = df['VOIX'].groupby(level=[0, 1]).sum()
    # Number of candidates for each political nuance:
    ncand = df2['NBCAND'].groupby(level=[0, 1]).sum()
    # Merge the two pieces of information:
    df = pd.DataFrame({'VOIX': nvotes, 'NBCAND': ncand}).reset_index()
    # Reorganize the resulting dataframe:
    df = df.pivot(index='CODGEO', columns='CODNUA').swaplevel(0, 1, axis=1)

    # Merge general informations and results by political nuance:
    res = pd.concat([df, general_infos], axis=1)
    res.sort_index(axis=1, inplace=True)
    return res

def read_xls_sheet(sheet, rows, columns, headers=None):
    """Read in the Excel sheet `sheet` the cells at the intersection of `rows`
    and `columns`.

    Use `headers` to label the columns; if `headers` is `None`, use the first
    row instead.
    """
    # Convert columns id to indexes starting from 0:
    if type(columns ) == str:
        cols_idx = range(xls_column_index(columns), sheet.ncols)
    else:
        cols_idx = [xls_column_index(col) for col in columns]

    # Start index for rows from 0:
    if type(rows) == int:
        rows = rows - 1
    else:
        rows = [r - 1 for r in rows]
        if headers is None:
            hrow = rows.pop(0)

    data = {}
    func = lambda y: np.nan if y == '' else y
    for k, i in enumerate(cols_idx):
        if type(rows) == int:
            if headers is None:
                header = sheet.cell_value(rows, i).strip()
                raw_values = sheet.col_values(i, start_rowx=rows+1)
            else:
                header = headers[k]
                raw_values = sheet.col_values(i, start_rowx=rows)
        else:
            if headers is None:
                header = sheet.cell_value(hrow, i).strip()
            else:
                header = headers[k]
            raw_values = [sheet.cell_value(rowx, i) for rowx in rows]

        data[header] = [func(x) for x in raw_values]

    return pd.DataFrame(data)

def xls_column_index(col):
    """Return the index (starting at 0) corresponding to the XLS column id
    `col`.

    Examples:
    ---------
    >>> xls_column_index('A')
    0
    >>> xls_column_index('F')
    5
    >>> xls_column_index('AZ')
    51
    >>> xls_column_index('XFD')
    16383
    """
    idx = sum([(string.ascii_uppercase.index(letter) + 1) * (26**i)
        for i, letter in enumerate(col[::-1])])
    return (idx - 1)

class InseeXLSWrapper():
    """Simple wrapper allowing to convert an XLS file produced by the INSEE to a
    pandas dataframe.

    Remark: while there seems to be no exact/rigorous specifications for the
    formatting of INSEE's XLS file, they are all relatively similar.
    """

    def __init__(self, source):
        """Initialize a new instance of the `InseeXLSWrapper` class.

        Parameters:
        -----------
        - source: a dictionary describing the XLS file and the parts of it
            which should be read.
        """
        with xlrd.open_workbook(source['file']) as fin:
            data = []
            # Retrieve the data for each commune:
            for sheet, params in source['data'].items():
                df = read_xls_sheet(fin.sheet_by_name(sheet), params['rows'],
                    params['columns'])

                # In some files, the label used to denote the INSEE code is not
                # 'CODGEO':
                if params['index'] != 'CODGEO':
                    df.rename(columns={params['index']: 'CODGEO'}, inplace=True)
                df.set_index('CODGEO', inplace=True)
                data.append(df)

            # Retrieve a short description of the series / a long header, stored
            # in a specific sheet:
            hparams = source['variables_description']
            if 'headers' in hparams:
                headers_cols = hparams['headers']
            else:
                headers_cols = None
            self.header = read_xls_sheet(fin.sheet_by_name(hparams['sheet']),
                hparams['rows'], hparams['columns'], headers_cols)
        # Concatenate the data:
        self.table = pd.concat(data)

        # Remove unwanted white spaces:
        self.header['VAR_ID'] = self.header['VAR_ID'].map(lambda x: x.strip())

        # Drop useless rows in the header and add a column indicating the
        # dataset the variables are referring to:
        mask = self.header['VAR_ID'].map(lambda x: x in self.table.columns)
        self.header = self.header[mask]
        self.header['DATASET'] = source['h5_key']
        self.header.set_index('VAR_ID', inplace=True)

    def drop_ancient_communes(self, correspondances):
        """Drop data associated to communes that do not exist in the index of
        the dataframe `correspondances`. Modify `self.table`.
        """
        #FIXME: dropping the data is a rather harsh way to handle them... Better
        # solutions could be implemented!
        index = np.unique(correspondances.index.get_level_values('CODGEO'))
        self.table = self.table.reindex(index)

    def fill_nans(self, correspondances):
        """Replace missing values in `self.table`, using the mean of the value
        for the corresponding canton. The dataframe `correspondances` is used
        to map communes and cantons.

        Remark: be aware that replacing missing values with a mean is not always
        meaningful.
        """
        #FIXME: use the population to weight the average?
        if not np.any(np.isnan(self.table)):
            # Do not proceed if there is no need to:
            return None

        # Compute the average for each datum on the whole country:
        nat_mean = np.nanmean(self.table, axis=0)

        for dep in np.unique(correspondances.index.get_level_values('DEP')):
            current_corr = correspondances.xs(dep, level='DEP')
            dep_idx = np.unique(current_corr.index.get_level_values('CODGEO'))
            test_dep = np.isnan(self.table.loc[dep_idx, :])
            if np.all(test_dep):
                # If there is not data at all for this département, use the
                # national average:
                self.table.loc[dep_idx, :] = nat_mean

            elif np.any(test_dep):
                with warnings.catch_warnings():
                    # Ignore numpy's 'Mean of empty slice' warning:
                    warnings.simplefilter("ignore", category=RuntimeWarning)

                    # Compute the average for each datum on the current
                    # département (and, for columns for which there is no data
                    # at all, use the national average):
                    dep_mean = np.nanmean(self.table.loc[dep_idx, :], axis=0)
                    dep_mean = np.where(np.isnan(dep_mean), nat_mean, dep_mean)

                    cantons = np.unique(current_corr.index\
                        .get_level_values('CODCAN15'))
                    for can in cantons:
                        can_idx = current_corr.xs(can, level='CODCAN15').index
                        data = self.table.loc[can_idx, :]
                        test_can = np.isnan(data)
                        if np.all(test_can):
                            # If there is not data at all for this canton, use
                            # the département average:
                            self.table.loc[can_idx, :] = dep_mean
                        elif np.any(test_can):
                            can_mean = np.nanmean(data, axis=0)
                            can_mean = np.where(np.isnan(can_mean), dep_mean,
                                can_mean)
                            self.table.loc[can_idx, :] = np.where(
                                np.isnan(data), can_mean, data)
