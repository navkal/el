# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.options.display.width = 0
import numpy as np
import sqlite3
import sqlalchemy


######################
#
# Sample parameter sequences
#
# -i input_db.sqlite -o output_workbook.xlsx [-t tables.xlsx]
#
# The tables file contains the following columns:
# - 'table_name': Lists tables to include, in the desired order
# - 'tab_name': Lists alternate labels for tabs.  (To keep original table name, leave blank.)
#
######################


TABLE_NAME = 'table_name'
TAB_NAME = 'tab_name'
WORKBOOK_STRUCTURE = 'Workbook Structure'

EXCEL_TAB_NAME_MAX_LEN = 31


# Open the SQLite database
def open_database():

    print( '\nOpening database: ' + args.input_filename )

    print( ' Connecting...' )
    conn = sqlite3.connect( args.input_filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + args.input_filename )

    return conn, cur, engine


# Read SQLite database
def read_database():

    # Initialize empty tab order dictionary
    tab_order = {}

    # Open the input database
    conn, cur, engine = open_database()

    # Fetch names of all tables
    cur.execute( 'SELECT name FROM sqlite_master WHERE type="table" OR type="view" AND name NOT LIKE "sqlite_%";' )
    rows = cur.fetchall()

    # Built pandas representation of input database
    print( '' )
    input_db = {}
    for row in rows:
        table_name = row[0]

        # Set default tab name and sort position
        tab_name = table_name
        tab_position = len( tab_order.keys() )

        # Look for mapping from table name to tab name
        if df_tabs is not None:

            # Select the row for current table name
            table_row = df_tabs[df_tabs[TABLE_NAME] == table_name]

            if len( table_row ):
                # Row found; get position and optional tab name
                tab_position = table_row.index[0]
                tab_name = table_row[TAB_NAME].values[0] or tab_name
            else:
                # Row not found for this table; exclude from output
                tab_name = ''

        if tab_name:
            print( 'Reading table "{}" for worksheet "{}" at position {}'.format( table_name, tab_name, tab_position ) )
            input_db[tab_name] = pd.read_sql_table( table_name, engine, parse_dates=True )
            tab_order[tab_position] = tab_name
        else:
            print( 'Excluding table "{}"'.format( table_name ) )

    tab_keys = sorted( tab_order )

    return input_db, tab_order, tab_keys


def build_structure():

    # Build dataframe to describe workbook structure
    df_structure = pd.DataFrame()

    for tab_key in tab_keys:

        # Retrieve tab name and table
        tab_name = make_tab_name( tab_key )
        df_table = input_db[tab_order[tab_key]]

        # Create a single-column dataframe describing the tab
        df_column = pd.DataFrame( columns=[tab_name], data=pd.Series( df_table.columns ) )

        # Add current tab description to structure dataframe
        df_structure = df_structure.join( df_column, how='outer' )

    return df_structure


def write_workbook():

    # Open output Excel file
    with pd.ExcelWriter( args.output_filename, engine='xlsxwriter', engine_kwargs={ 'options': {'strings_to_numbers': True} } ) as writer:

        # Write structure table to Excel
        print( '' )
        print( 'Creating worksheet "{}"'.format( WORKBOOK_STRUCTURE ) )
        df_structure.to_excel( writer, sheet_name=WORKBOOK_STRUCTURE, index=False )

        for tab_key in tab_keys:

            # Get name and dataframe for next tab
            tab_name = make_tab_name( tab_key )
            df = input_db[tab_order[tab_key]]

            # Write the dataframe with specified tab name
            print( 'Creating worksheet "{}"'.format( tab_name ) )
            df.to_excel( writer, sheet_name=tab_name, index=False )


# Generate a tab name, adhering to MS Excel's length restriction
def make_tab_name( tab_key ):
    num_width = len( str( len( tab_order ) ) )
    tab_name = '{} {}'.format( str( tab_key + 1 ).zfill( num_width ), tab_order[tab_key] )
    truncated_tab_name = tab_name[:EXCEL_TAB_NAME_MAX_LEN]
    return truncated_tab_name


# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate Excel file from SQLite database' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename - Name of SQLite database file', required=True )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of MS Excel file', required=True )
    parser.add_argument( '-t', dest='tabs_filename',  help='Tabs filename - Name of CSV file specifying order and naming tabs in output Excel file' )
    args = parser.parse_args()

    # Report
    print( 'Input filename:', args.input_filename )
    print( 'Output filename:', args.output_filename )

    # Report and read optional Tabs file
    if args.tabs_filename != None:
        print( 'Tabs filename:', args.tabs_filename )

        # Read table-name -> tab-name mapping
        df_tabs = pd.read_excel( args.tabs_filename, dtype=object )

        # Strip whitespace from all cells
        df_tabs = df_tabs.applymap( lambda x: x.strip() if type(x)==str else x )

        # Replace null with empty string
        df_tabs = df_tabs.fillna( '' )

        # Drop rows with empty table name
        df_tabs = df_tabs[ df_tabs[TABLE_NAME] != '' ]
        df_tabs = df_tabs.reset_index( drop=True )

        # Prohibit duplicates
        df_test = df_tabs.copy()

        for col in df_test.columns:
            df_test = df_test[ df_test[col] != '' ]
            bf_len = len( df_test )
            df_test = df_test.drop_duplicates( subset=col )

            if len( df_test ) != bf_len:
                print( '' )
                print( 'Error: Duplicates found in {} column'.format( col ) )
                exit()
    else:
        df_tabs = None

    # Read database file
    input_db, tab_order, tab_keys = read_database()

    # Build self-describing dataframe
    df_structure = build_structure()

    # Write Excel workboook
    write_workbook()

    print( '' )
    print( 'Done' )
