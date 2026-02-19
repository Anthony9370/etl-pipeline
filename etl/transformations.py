import pandas as pd

# Example: Date standardization
def standardize_dates(df, date_cols):
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], utc=True)
    return df

# Example: Status mapping
def map_status(df, status_col, mapping):
    df[f'{status_col}_desc'] = df[status_col].map(mapping)
    return df

# Example: Remove duplicates
def remove_duplicates(df, subset=None):
    return df.drop_duplicates(subset=subset)

# Example: Null handling
def fill_nulls(df, fill_map):
    return df.fillna(fill_map)

# Add more transformation utilities as needed

def auto_fill_nulls(df, numeric_fill=0, string_fill='', bool_fill=False, datetime_fill=pd.NaT):
    """Automatically generate a fill map based on column dtypes and apply it.

    - Numeric columns -> `numeric_fill` (default 0)
    - Boolean columns -> `bool_fill` (default False)
    - Datetime columns -> `datetime_fill` (default pd.NaT)
    - Other (object/string) -> `string_fill` (default '')

    Returns the DataFrame with NaNs filled according to the generated map.
    """
    fill_map = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_numeric_dtype(dtype):
            fill_map[col] = numeric_fill
        elif pd.api.types.is_bool_dtype(dtype):
            fill_map[col] = bool_fill
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            fill_map[col] = datetime_fill
        else:
            # For object columns (strings, mixed), use string_fill
            fill_map[col] = string_fill
    return df.fillna(fill_map)
