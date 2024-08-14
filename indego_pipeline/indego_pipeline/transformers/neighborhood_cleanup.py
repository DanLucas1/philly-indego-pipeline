import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):

    # turn columns to snake case
    cols_lower = {col: col.lower() for col in df.columns}
    df = df.rename(columns=cols_lower)

    # drop duplicate name columns and rename remaining column
    df = df.drop(columns=['name', 'listname']).rename(columns={'mapname':'neighborhood_name'})

    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'