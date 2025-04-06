import pandas as pd
from io import BytesIO


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    return df


def process_shipping_data(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {'B/L No.', 'S/Line Name'}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(df.columns)}")

    data_df = df[['B/L No.', 'S/Line Name']].copy()
    data_df.dropna(inplace=True)
    data_df.drop_duplicates(subset=['B/L No.'], inplace=True)
    data_df['SLineTags'] = data_df['S/Line Name'].apply(lambda x: 'MSC' if 'MSC' in x else 'Others')
    return data_df


def get_msc_group_excel(df: pd.DataFrame) -> BytesIO:
    msc_df = df[df['SLineTags'] == 'MSC']
    return msc_df


def map_tracking_dates_to_main_df(main_df: pd.DataFrame, tracking_df: pd.DataFrame) -> tuple[BytesIO, int]:
    main_df = main_df.rename(columns=lambda x: x.strip())
    tracking_df = tracking_df.rename(columns=lambda x: x.strip())

    # Merge the enriched tracking info onto the main DataFrame using B/L No.
    merged_df = main_df.merge(tracking_df, how='left', left_on='B/L No.', right_on='BillOfLading')

    # Drop extra key column after merge
    merged_df.drop(columns=['BillOfLading'], inplace=True)

    # Write final result to Excel in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        merged_df.to_excel(writer, index=False, sheet_name='Mapped Data')

    output.seek(0)
    return output, len(tracking_df)