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


def convert_dates(df):
    """Convert the date columns of a data frame to actual datetime objects.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the date columns to be converted.

    Returns:
        pandas.core.frame.DataFrame: data frame with the converted columns.
    """
    # Convert the time columns to datetime types.
    df['created_at'] = pd.to_datetime(df['created_at'],unit='s')
    df['state_changed_at'] = pd.to_datetime(df['state_changed_at'],unit='s')
    df['deadline'] = pd.to_datetime(df['deadline'],unit='s')
    return df


def calculate_time_periodes(df):
    """Calculate the time periodes (in days) until the project status changed and the deadline is reached.

    Args:
        df (pandas.core.frame.DataFrame): data frame with the datetime columns to calculate the time periodes in days.

    Returns:
        pandas.core.frame.DataFrame: data frame with the calculated time periodes in days.
    """
    # Calculate the time spans.
    df['days_till_change'] = df['state_changed_at'].dt.date-df['created_at'].dt.date
    df['days_total'] = df['deadline'].dt.date-df['created_at'].dt.date
    # Convert the days to ints.
    df['days_till_change'] = df['days_till_change'].dt.days
    df['days_total'] = df['days_total'].dt.days
    return df