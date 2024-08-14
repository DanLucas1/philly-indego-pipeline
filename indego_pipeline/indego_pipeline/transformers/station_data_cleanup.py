if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(df, *args, **kwargs):

    # clean up column names
    clean_cols = {
        'Station_ID': 'station_id',
        'Station_Name': 'station_name',
        'Day of Go_live_date': 'go_live_date',
        'Status': 'status'}

    df = df.rename(columns=clean_cols)

    # drop extra csv columns
    keep_cols = [col for col in df.columns if 'unnamed' not in col.lower()]
    df = df[keep_cols]

    # set dtypes for known columns
    dtypes = {
    'station_id': 'int64',
    'station_name': 'object',
    'go_live_date': 'datetime64[ns]',
    'status': 'object'}

    # drop missing station ids
    df = df.dropna(subset = 'station_id')

    # drop any duplicate station ids (keeping the more recent go-live date)
    df = df.sort_values(by=['station_id', 'go_live_date'], ascending=[True, False])
    df = df.drop_duplicates(subset='station_id', keep='first', ignore_index=True)

    # set dtypes for output
    df = df.astype(dtypes)

    return df


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
