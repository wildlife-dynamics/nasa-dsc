import os

def export_gpkg(df, dir=None, outname=None, lyrname=None):
    df = df.copy()
    check_columns = df.columns.to_list()
    check_columns.remove("geometry")
    df.drop(
        columns=df[check_columns].columns[df[check_columns].applymap(lambda x: isinstance(x, list)).any()],
        errors="ignore",
        inplace=True,
    )
    df.to_file(os.path.join(dir or '.', outname or 'df.gpkg'), layer=lyrname or 'df')