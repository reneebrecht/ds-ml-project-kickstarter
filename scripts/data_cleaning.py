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
    df['days_launched_till_changed'] = df['state_changed_at'].dt.date-df['launched_at'].dt.date
    df['days_prelaunch'] = df['launched_at'].dt.date-df['created_at'].dt.date
    df['days_total'] = df['deadline'].dt.date-df['created_at'].dt.date
    # Convert the days to ints.
    df['days_launched_till_changed'] = df['days_launched_till_changed'].dt.days
    df['days_prelaunch'] = df['days_prelaunch'].dt.days
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
        df[str(col + '_weekday')] = df[col].dt.weekday
    # Drop the old column
    df.drop(columns, axis=1, inplace=True)
    return df


def entangle_column(df, columns):
    """Entangles the content of the given object type columns of a data frame.
        Adds the entangled content as new row to the given data frame.
        And drops the old columns.

    Args:
        df (pandas.core.frame.DataFrame): data frame that contains the columns to be entangled.
        columns (list(str)): columns to be entangled.

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


def get_char_len(df, columns=[
    'project_name', 
    'creator_name', 
    'blurb'
    ]):
    """Gives back the length of the strings in columns.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the columns containing the strings.
        columns (list, optional): list with the columns for which the string lengths should be calculated.
            Defaults to ['project_name', 'creator_name', 'blurb'].

    Returns:
        pandas.core.frame.DataFrame: data frame with the length columns
    """
    for col in columns:
        if col == 'creator_name':
            df[str(col + '_len')] = df[col].str.len()
        else:
            df[str(col + '_len')] = df[col].str.split().str.len()
        df.drop(col, axis=1, inplace=True)
    return df


def one_hot_encode(df, not_to_enc_cols=[
        'state',
        'goal', 
        'days_prelaunch',
        'days_launched_till_changed',
        'days_total',
        'project_name_len',
        'creator_name_len',
        'blurb_len'
        ]):
    """One-hot-encodes the categorical columns of the given data frame, excluding the given columns.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the categorical columns to be encoded. 
        not_to_enc_cols (list, optional): list of the columns to exclude from the encoding.
            Defaults to [ 'state', 'backers_count', 'goal', 'usd_pledged', 'days_prelaunch', 'days_launched_till_changed', 'days_total', 'project_name_len', 'creator_name_len' ].

    Returns:
        pandas.core.frame.DataFrame: data frame with the encoded columns.
    """
    # Get all column names
    col_names = df.columns.to_list()
    # Set the cols which should not be encoded (numerics and the target)
    not_enc_cols = not_to_enc_cols.append('state')
    # Create a list of the cols which need to be encoded
    cols_to_encode = [col for col in col_names if col not in not_to_enc_cols]
    df = pd.get_dummies(df, columns=cols_to_encode, prefix=cols_to_encode)
    return df


def clean_target(df):
    """Cleans the target variable by removing/transforming unnecessary labels.

    Args:
        df (pandas.core.frame.DataFrame): data frame containing the target variable.

    Returns:
        pandas.core.frame.DataFrame: data frame with the cleaned target variable.
    """
    # Get rid of the live state entries
    df = df[df['state'] != 'live']
    # Replaces the labels canceled and suspended with failed in the target variable
    for label in ['failed', 'canceled', 'suspended']:
        df['state'] = df['state'].str.replace(label, 'failed')
    return df


def clean_data(df):
    """The whole data cleaning process.

    Args:
        df (pandas.core.frame.DataFrame): data frame to be cleaned.

    Returns:
        pandas.core.frame.DataFrame: Cleaned data frame
    """
    # Remove the duplicates
    df.drop_duplicates(subset='id', inplace=True)
    # Drop all columns with more than 50% of the observations missing
    df = df[[column for column in df if df[column].count() / len(df) >= 0.5]]
    # Drop the listed columns
    df.drop([
        'backers_count',
        'converted_pledged_amount',
        'currency_symbol', 
        'currency_trailing_code', 
        'fx_rate',
        'id',
        'pledged',
        'photo',
        'profile',
        'slug',
        'source_url', 
        'spotlight',
        'staff_pick',
        'static_usd_rate',
        'urls',
        'usd_pledged'
        ], axis=1, inplace=True)
    # Drops the last few NaN values
    df.dropna(axis=0, inplace=True)
    # Rename the currency column
    df.rename(columns={
        'currency':'original_currency',
        'name':'project_name'}, inplace=True)
    # Convert the time columns to datetime types
    df = convert_to_datetime(df, ['created_at', 'state_changed_at', 'deadline', 'launched_at'])
    # Calculate the time periods
    df = calculate_time_periods(df)
    # Get the years, months and days as separate columns
    df = get_year_month_day(df, ['created_at', 'deadline', 'launched_at'])
    # Drop these columns since this is info we do not have at project start 
    df.drop(['days_launched_till_changed', 'state_changed_at'], axis=1, inplace=True)
    # Entangles the category, creator and location column
    df = entangle_column(df, ['category', 'creator', 'location'])
    df.rename(columns={'category_name':'category'}, inplace=True)
    # Drop the current_currency column
    df.drop('current_currency', axis=1, inplace=True)
    # Drop category_id and location_name
    df.drop(['category_id', 'location_name'], axis=1, inplace=True)
    # Calculate the character length of the columns
    get_char_len(df, columns=['project_name', 'creator_name', 'blurb'])
    # One-hot-encode the categorical features.
    df = one_hot_encode(df)
    # Clean the target variable
    df = clean_target(df)
    return df