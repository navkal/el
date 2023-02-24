# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.options.display.width = 0
import numpy as np
import sqlite3
import sqlalchemy
import datetime


######################
#
# db2xl.py publishes a sqlite database as an Excel workbook, each table corresponding to a single worksheet in the output file.
#
# Sample parameter sequence:
#
# -i input_db.sqlite -o output_workbook.xlsx [-t tables.xlsx]
#
# Tables file
#   By default (i.e., if the tables file is not supplied), db2xl.py publishes the entire database.
#   The tables file allows you to specify which tables to publish, in what order, and under what names.
#   It also allows you to specify, for individual tables, which columns to publish, in what order, and under what names.
#
#   The tables file consists of one or more worksheets (tabs).
#
#   The first worksheet specifies which tables to publish, in what order, and under what names.
#   It contains the following columns:
#
#   - 'table_name': Lists tables to include, in the desired order
#   - 'tab_name': Lists alternate labels for tabs.  (To keep original table name, leave blank.)
#
#   Each (optional) subsequent worksheet describes, for a specific output worksheet, which columns to publish, in what order, and under what names.
#   It must bear the name of the output tab to which it applies.  It contains the following columns:
#
#   - 'old_column_name': Lists columns to include, in the desired order
#   - 'new_column_name': Lists new name to be used for each column.  During publication, db2xl.py will insert a numeric prefix into each of these names.
#
######################


TABLE_NAME = 'table_name'
TAB_NAME = 'tab_name'
OLD_COLUMN_NAME = 'old_column_name'
NEW_COLUMN_NAME = 'new_column_name'
WORKBOOK_STRUCTURE = 'Workbook Structure'
COPYRIGHT = 'Copyright'

EXCEL_TAB_NAME_MAX_LEN = 31


# Read Excel input file that describes operation to be performed
def read_tabs_file():

    df_tabs = None
    dc_sheets = {}

    workbook = pd.read_excel( args.tabs_filename, dtype=object, sheet_name=None )

    for sheet_name in workbook:

        df = workbook[sheet_name]

        if len( df ):

            # Clean up whitespace and NaN
            df = df.applymap( lambda x: x.strip() if type(x)==str else x )
            df = df.fillna( '' )

            # Drop rows with empty first column
            df = df[df[df.columns[0]] != '' ]
            df = df.reset_index( drop=True )

            # Prohibit duplicates
            df_test = df.copy()

            for col in df_test.columns:
                df_test = df_test[ df_test[col] != '' ]
                bf_len = len( df_test )
                df_test = df_test.drop_duplicates( subset=col )

                if len( df_test ) != bf_len:
                    print( '' )
                    print( '!!! Duplicates found: sheet "{}", column "{}"'.format( sheet_name, col ) )
                    exit()

            # Save as master tabs list or sheet detail
            if not isinstance( df_tabs, pd.DataFrame ):
                df_tabs = df
            else:
                dc_sheets[sheet_name] = df

    return df_tabs, dc_sheets



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

        if not table_name.startswith( '_About' ):

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


# Edit columns as specified by tab input file
def edit_database( input_db, dc_sheets ):

    for sheet_name in input_db:

        if sheet_name in dc_sheets:

            # Select columns to be published
            input_db[sheet_name] = input_db[sheet_name][dc_sheets[sheet_name][OLD_COLUMN_NAME]]

            # Rename columns to be published
            dc_rename = dict( zip( dc_sheets[sheet_name][OLD_COLUMN_NAME], dc_sheets[sheet_name][NEW_COLUMN_NAME] ) )
            input_db[sheet_name] = input_db[sheet_name].rename( columns=dc_rename )

            # Number columns to be published
            n_cols = len( input_db[sheet_name].columns )
            num_width = len( str( n_cols ) )
            col_idx = 0
            for column_name in input_db[sheet_name].columns:
                col_idx += 1
                input_db[sheet_name] = input_db[sheet_name].rename( columns={ column_name: str( col_idx ).zfill( num_width ) + '-' + column_name } )

            print( '' )
            print( 'Editing worksheet "{}"'.format( sheet_name ) )
            print( '', 'Selecting columns:' )
            print( '', list( dc_rename.keys() ) )
            print( '', 'Publishing as:')
            print( '', list( input_db[sheet_name].columns ) )

        return input_db


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


def make_copyright():
    copyright_notice = '© {} Energize Lawrence.  All rights reserved.'.format( datetime.date.today().year )
    df_copyright = pd.DataFrame( columns=['copyright'], data=[copyright_notice] )
    return df_copyright


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

        # Write copyright table to Excel
        print( 'Creating worksheet "{}"'.format( COPYRIGHT ) )
        df_copyright.to_excel( writer, sheet_name=COPYRIGHT, index=False )


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
        df_tabs, dc_sheets = read_tabs_file()
    else:
        df_tabs = None
        dc_sheets = {}


    # Read database file
    input_db, tab_order, tab_keys = read_database()

    # Edit tables as specified in Tabs file
    input_db = edit_database( input_db, dc_sheets )

    # Build self-describing dataframe
    df_structure = build_structure()

    # Create copyright dataframe
    df_copyright = make_copyright()

    # Write Excel workboook
    write_workbook()

    print( '' )
    print( 'Done' )
