# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.options.display.width = 0

import sqlite3
import sqlalchemy

import datetime
import os

import warnings


######################
#
# publish_db.py publishes all or selected parts of a sqlite database, saving the output to an Excel workbook or sqlite database.
# Each table published from the input database corresponds in the output to a single (Excel) worksheet or single (sqlite) table.
#
# Sample parameter sequence:
#
#   -i input_db.sqlite -o output_workbook.<xlsx|sqlite> [-t tables.xlsx]
#
# Tables file
#
#   By default (i.e., if the tables file is not supplied), publish_db.py publishes the entire input database.
#   The tables file allows you to specify which tables to publish, in what order, and under what names.
#   It also allows you to specify, for individual tables, which columns to publish, in what order, and under what names.
#
#   The tables file consists of one or more worksheets (tabs).
#
#   The first worksheet specifies which tables to publish, in what order, and under what names.
#   It must contain the following columns:
#
#   - 'table_name': Lists tables to include, in the desired order
#   - 'tab_name': Lists alternate labels for tabs.  (To keep original table name, leave blank.)
#   - 'column_map': References a subsequent worksheet describing how to publish the table's columns.  (To keep original columns, leave blank.)
#
#   Each (optional) subsequent worksheet bears the name of a specific output worksheet (or table),
#   and describes, for that worksheet (or table), which columns to publish, in what order, and under what names.
#
#   - 'old_column_name': Lists columns to include, in the desired order
#   - 'new_column_name': Lists new name to be used for each column.  During publication, publish_db.py will insert a numeric prefix into each of these names.
#
######################


TABLE_NAME = 'table_name'
TAB_NAME = 'tab_name'
COLUMN_MAP = 'column_map'
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
                # Look for table row that references this sheet name as its column map
                df_sheet_name = df_tabs[ df_tabs[COLUMN_MAP]==sheet_name ]
                if len( df_sheet_name ):
                    # Determine output sheet name - either specified tab name or original table name
                    row = df_sheet_name.iloc[0]
                    output_sheet_name = row[TAB_NAME] if row[TAB_NAME] != '' else row[TABLE_NAME]
                    print( sheet_name, '===>', output_sheet_name )
                    dc_sheets[output_sheet_name] = df

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
                print( 'Reading table "{}" for output as "{}" at position {}'.format( table_name, tab_name, tab_position ) )
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
                input_db[sheet_name] = input_db[sheet_name].rename( columns={ column_name: str( col_idx ).zfill( num_width ) + ': ' + column_name } )

            print( '' )
            print( 'Editing table "{}"'.format( sheet_name ) )
            print( '', 'Selecting columns:' )
            print( '', list( dc_rename.keys() ) )
            print( '', 'Publishing as:')
            print( '', list( input_db[sheet_name].columns ) )

    return input_db


# Build dataframe describing workbook structure
def build_structure():

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


# Synthesize copyright worksheet
def make_copyright():
    copyright_notice = '© {} Energize Lawrence.  All rights reserved.'.format( datetime.date.today().year )
    df_copyright = pd.DataFrame( columns=['copyright'], data=[copyright_notice] )
    return df_copyright


# Save results to output workbook
def write_workbook():

    # Open output Excel file
    with pd.ExcelWriter( args.output_filename, engine='xlsxwriter', engine_kwargs={ 'options': {'strings_to_numbers': True} } ) as writer:

        # Write structure table to Excel
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


# Save results to output database
def write_database():

    # Open the database
    conn = sqlite3.connect( args.output_filename )
    cur = conn.cursor()

    for tab_key in tab_keys:

        # Get name and dataframe for next table
        table_name = make_table_name( tab_key )
        df = input_db[tab_order[tab_key]]

        # Write the table to the database
        write_table( table_name, df, cur, conn )

    # Add the copyright notice
    write_table( '_About', make_copyright(), cur, conn )


# Save a table to the output database
def write_table( table_name, df, cur, conn ):

    print( 'Creating table "{}"'.format( table_name ) )

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + table_name )

    # Fix numeric columns
    df = fix_numeric_columns( df )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df.columns:
        sqltype = pdtype_to_sqltype( df, col_name )
        create_sql += ', "{0}" {1}'.format( col_name, sqltype )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    warnings.filterwarnings( 'ignore', category=UserWarning, module='pandas' )
    df.to_sql( table_name, conn, if_exists='append', index=False )
    warnings.resetwarnings()


# Fix numeric columns in specified dataframe
def fix_numeric_columns( df ):

    for column_name in df.columns:

        if ( column_name.lower().find( 'zip' ) == -1 ) and df[column_name].dtype == object:
            df[column_name] = pd.to_numeric( df[column_name], errors='ignore' )

    return df


# Map pandas datatype to SQL datatype
def pdtype_to_sqltype( df, col_name ):

    if df is None:
        sqltype = 'TEXT'
    else:
        pdtype = str( df[col_name].dtype )

        if pdtype.startswith( 'float' ):
            sqltype = 'FLOAT'
        elif pdtype.startswith( 'int' ):
            sqltype = 'INT'
        else:
            sqltype = 'TEXT'

    return sqltype


# Generate a tab name, adhering to MS Excel's length restriction
def make_tab_name( tab_key ):
    num_width = len( str( len( tab_order ) ) )
    tab_name = '{} {}'.format( str( tab_key + 1 ).zfill( num_width ), tab_order[tab_key] )
    truncated_tab_name = tab_name[:EXCEL_TAB_NAME_MAX_LEN]
    return truncated_tab_name


# Generate a table name, adhering to SQLite constraints
def make_table_name( tab_key ):
    tab_name = make_tab_name( tab_key )
    table_name = '_'.join( tab_name.split()[1:] )
    return table_name


# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate Excel file from SQLite database' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename - Name of SQLite database file', required=True )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of Excel workbook or SQLite database', required=True )
    parser.add_argument( '-t', dest='tabs_filename',  help='Tabs filename - Name of CSV file specifying order and naming tabs in output Excel file' )
    args = parser.parse_args()

    output_ext = os.path.splitext( args.output_filename )[1]
    if output_ext == '.xlsx':
        b_xl = True
    elif output_ext == '.sqlite':
        b_xl = False
    else:
        print( 'Output file extension "{}" is not supported.  Please use ".xlsx" or ".sqlite".  '.format( output_ext ) )
        exit()

    # Report
    print( '' )
    print( 'Input filename:', args.input_filename )
    print( 'Output filename:', args.output_filename )

    # Report and read optional Tabs file
    if args.tabs_filename != None:
        print( '' )
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

    print( '' )

    if b_xl:
        # Write Excel workboook
        write_workbook()
    else:
        # Write SQLite database
        write_database()

    print( '' )
    print( 'Done' )
