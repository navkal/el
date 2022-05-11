# Copyright 2019 Energize Andover.  All rights reserved.

import pandas as pd
import sqlite3

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 1000)


# Save dataframe as table in SQL database
def make_sql_table( df, table_name ):

    # Drop old table
    cur.execute( 'DROP TABLE IF EXISTS ' + table_name )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data from dataframe into table
    df.to_sql( table_name, conn, if_exists='append', index=False )


# Column name mappings
MASTER_ACCOUNT_NUMBER = 'master_account_number'
ACCOUNT_NUMBER = 'account_number'
ACCOUNT_NAME_1 = 'account_name_1'
ACCOUNT_NAME_2 = 'account_name_2'
AMOUNT = 'amount_$'
SERVICE_ADDRESS = 'service_address'

COL_NAME_MAP = \
{
    # From master account spreadsheet
    'Master Account': MASTER_ACCOUNT_NUMBER,
    'Account Name': ACCOUNT_NAME_1,
    'Account Number': ACCOUNT_NUMBER,
    'Amount': AMOUNT,

    # From mixed accounts spreadsheet
    'Acct Name': ACCOUNT_NAME_2,
    'Acct #': ACCOUNT_NUMBER,
    'Service Address': SERVICE_ADDRESS,
}

COL_NAME_ORDER = \
[
    MASTER_ACCOUNT_NUMBER,
    ACCOUNT_NUMBER,
    ACCOUNT_NAME_1,
    ACCOUNT_NAME_2,
    SERVICE_ADDRESS,
    AMOUNT,
]


# Main program
if __name__ == '__main__':

    # Read spreadsheets
    df_master = pd.read_excel( './in/lawrence_electric_master.xlsx' )
    df_mixed = pd.read_excel( './in/lawrence_electric_mixed.xlsx' )

    # Rename columns
    df_master = df_master.rename( columns=COL_NAME_MAP )
    df_mixed = df_mixed.rename( columns=COL_NAME_MAP )

    # Merge the dataframes
    df_merge = pd.merge( df_master, df_mixed, how='outer', on=[ACCOUNT_NUMBER] )

    # Reorder columns
    df_merge = df_merge.reindex( columns=COL_NAME_ORDER )

    # Open the database
    conn = sqlite3.connect( './out/lawrence_electric_meters.sqlite' )
    cur = conn.cursor()

    # Generate tables in database
    make_sql_table( df_master, 'RawMaster' )
    make_sql_table( df_mixed, 'RawMixed' )
    make_sql_table( df_merge, 'LawrenceElectricMeters' )

    # Commit changes
    conn.commit()
