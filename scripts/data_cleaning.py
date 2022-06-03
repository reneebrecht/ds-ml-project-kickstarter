from re import sub
import pandas as pd
import numpy as np
from pathlib import Path 


def read_all_csvs():
    """Read all different CSVs into one pandas data frame.

    Returns:
        pandas.core.frame.DataFrame consisting of the data of all CSVs.
    """
    csv_name = 'Kickstarter0'
    df = pd.DataFrame()
    # Iterate through the numbers 0 to 55
    for num in range(0,56):
        # Convert number to a string
        num = str(num)
        # If single digit number, add a 0 to the front
        if len(num) == 1:
            num = '0' + num
        # Load the current csv
        curr_df = pd.read_csv(str('./data/' + csv_name + num + '.csv'))
        # Merge all csv data to one data frame.
        df = pd.concat([df, curr_df], axis=0)
    return df


def create_csv(df, file_name):
    """Create a CSV file from a data frame in the data folder.

    Args:
        df (pandas.core.frame.DataFrame): data frame to be converted into csv.
        file_name (str): name of the CSV file.
    """
    # Set the file path and name for the CSV file.
    filepath = Path('./data/' + file_name + '.csv')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    # Save the data frame as CSV in the filepath.
    df.to_csv(filepath, header=True, index=False)


def get_nan_cols(df):
    """Searches for columns with NaNs in a data frame and prints these.

    Args:
        df (pandas.core.frame.DataFrame): data frame to be searched for NaNs.
    """
    print("Number of NaNs per column:")
    nans = [print(f"{colname}:{number}") for colname, number in zip(df.columns.to_list(), list(df.isnull().sum())) if number != 0]


def convert_to_datetime(df, columns):
    """Convert the date columns of a data frame to actual datetime objects.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the date columns to be converted.
        columns (list(str)): columns of the data frame to be converted.

    Returns:
        pandas.core.frame.DataFrame: data frame with the converted columns.
    """
    # Convert the time columns to datetime types.
    for col in columns:
        df[col] = pd.to_datetime(df[col],unit='s')
    return df


def calculate_time_periods(df):
    """Calculate the time periods (in days) until the project status changed and the deadline is reached.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the datetime columns to calculate the time periods.

    Returns:
        pandas.core.frame.DataFrame: data frame with the calculated time periods.
    """
    # Calculate the time spans.
    df['days_till_change'] = df['state_changed_at'].dt.date-df['created_at'].dt.date
    df['days_total'] = df['deadline'].dt.date-df['created_at'].dt.date
    # Convert the days to ints.
    df['days_till_change'] = df['days_till_change'].dt.days
    df['days_total'] = df['days_total'].dt.days
    return df


def get_year_month_day(df, columns):
    """Add columns for the year, month and day of the datetime columns. 
        Drops the old column afterwards.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the datetime columns.
        columns (list(str)): datetime columns.

    Returns:
        pandas.core.frame.DataFrame: data frame with the added columns.
    """
    # Iterate through each column and add the year, month, day
    for col in columns:
        df[str(col + '_year')] = df[col].dt.year
        df[str(col + '_month')] = df[col].dt.month
        df[str(col + '_day')] = df[col].dt.day
    # Drop the old column
    df.drop(columns, axis=1, inplace=True)
    return df


def entangle_column(df, columns):
    """Entangles the content of the given object type columns of a data frame.
        Adds the entangled content as new row to the given data frame.
        And drops the old columns.

    Args:
        df (pandas.core.frame.DataFrame): data frame that contains the columns to be entangled.
        columns (list(str)): columns to be entangeled.

    Returns:
        pandas.core.frame.DataFrame: data frame with the new entangled columns.
    """
    # Iterate through each column and drop the unnecessary  characters
    for col in columns:
        for element in ['{','}', '"']:
            df[col] = df[col].str.replace(element, '')
        # Create empty lists to catch the wanted information
        names = []
        ids = []
        sub_cats = []
        # Iterate through all information and catch the wanted
        for element in df[col].str.split(','):
            # For the category we need different information
            if col == 'category':
                ids.append(element[0].replace('id:', ''))
                sub_cat = element[2].replace('slug:', '').split('/')
                names.append(sub_cat[0])
                # Some projects do not have a subcategory
                if len(sub_cat) != 1:
                    sub_cats.append(sub_cat[1])
                else:
                    sub_cats.append(sub_cat[0])
            else:
                names.append(element[1].replace('name:', ''))
        # Add the wanted information as new columns
        df[str(col + '_name')] = names
        if col == 'category':
            df[str(col + '_id')] = ids
            df[str(col + '_sub')] = sub_cats
        # Drop the old column
        df.drop(col, axis=1, inplace=True)
    return df

def clean_data(df):
    # Drop all columns with more than 50% of the observations missing
    df = df[[column for column in df if df[column].count() / len(df) >= 0.5]]
    # Drop the listed columns
    df.drop([
        'blurb', 
        'converted_pledged_amount',
        'currency_symbol', 
        'currency_trailing_code', 
        'fx_rate',
        'photo',
        'profile',
        'slug',
        'source_url', 
        'static_usd_rate',
        'urls'
        ], axis=1, inplace=True)
    # Drops the last few NaN values
    df.dropna(axis=0, inplace=True)
    # Rename the currency column
    df.rename(columns={
        'currency':'original_currency',
        'id':'project_id'}, inplace=True)
    # Convert the time columns to datetime types
    df = convert_to_datetime(df, ['created_at', 'state_changed_at', 'deadline'])
    # Calculate the time periods
    df = calculate_time_periods(df)
    # Get the years, months and days as separate columns
    df = get_year_month_day(df, ['created_at', 'state_changed_at', 'deadline'])
    # Entangles the category, creator and location column
    df = entangle_column(df, ['category', 'creator', 'location'])
    # Drop the current_currency column
    df.drop('current_currency', axis=1, inplace=True)
    return df