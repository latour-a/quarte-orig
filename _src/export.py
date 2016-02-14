"""Sylvain Durand, Arnaud de Latour.
MIT License.
"""

import pandas as pd

def to_choropleth_fmt(df, weights=None, cog=None):
    """Conform `df` to a new index, appropriate for choropleth display (please
    see: https://github.com/sylvaindurand/france-choropleth).

    Parameters:
    -----------
    - df: dataframe. Data to reindex.
    - weights: None (default) or dataframe. Use them to compute weighted sums
        when concatenating data over a "canton".
    - cog: None (default) or dataframe containing the so called "Code officiel
        géographique".
    """
    if cog is None:
        cog = pd.read_csv("data/output/cog.csv", index_col=0)

    # Non public function:
    def _concat(x):
        # Add cantons:
        x = pd.concat([x, cog], axis=1).dropna(subset=x.columns)
        # Group by cantons and add current data, in order to have both cantons
        # and  communes in one dataframe:
        return pd.concat([x, x.groupby('canton').sum()]).drop(['canton'],
            axis=1)

    cols = df.columns
    df = _concat(df)
    if weights is not None:
        wcols = weights.columns
        _w = _concat(weights)
        df.loc[:, wcols] = df[wcols] / _w[wcols]
    return df.dropna(subset=cols)
