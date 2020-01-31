# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import time

import sys
sys.path.append( '../util' )
import util



START_TIME = time.time()

ELECTION_TYPE_MAP = \
{
    'PP': 'Presidential Primary',
    'LE': 'Local Election',
    'SP': 'State Primary',
    'S': 'State Election',
    'SSP': 'Special State Primary',
    'SS': 'Special State',
    'EV': 'Early Voting',
    'TM': 'Town Meeting'
}



TABLE_NAME_PREFIX = 'ElectionModel_'


def report_empty_columns( df ):

    bf = df.columns
    af = df.dropna( axis='columns', how='all' ).columns
    empty = list( bf.difference( af ) )

    if len( empty ):
        print( 'Empty columns -'.format( len( empty ) ) )
        print( '  {0}'.format( empty ) )


# Load dictionary of elections based on sheets in Excel input file
def make_elections():

    # Read the Excel file
    print( 'Reading', args.input_filename )
    xls = pd.ExcelFile( args.input_filename )
    print( 'Read time: {0} seconds'.format( round( ( time.time() - START_TIME ) * 1000 ) / 1000 ) )

    print( '\n--- Sheets ---' )

    # Initialize empty dictionary
    dc_elections = {}

    # Load dictionary of elections
    n_sheet = 0
    for sheet_name in xls.sheet_names:

        # Initialize dictionary representing current election
        election = {}

        # Extract election type and date
        ls_name = sheet_name.split()
        if args.election_type == None:
            election['type'] = ELECTION_TYPE_MAP[ls_name[0]]
            date = ls_name[1]
            election['date'] = date[0:2] + '-' + date[2:4] + '-20' + date[4:6]
        else:
            election['type'] = ELECTION_TYPE_MAP[args.election_type]
            election['date'] = '{:02d}'.format( int( ls_name[0] ) ) + '-' + '{:02d}'.format( int( ls_name[1] ) ) + '-20' + ls_name[2]

        # Report what we're looking at
        n_sheet += 1
        print( "\n{0}: '{1}', {2}, {3}".format( n_sheet, sheet_name, election['type'], election['date'] ) )

        # Extract voter statistics
        stats = xls.parse( sheet_name, header=None, index_col=0, usecols=range(2), nrows=2 )
        stats.index = stats.index.str.strip()
        stats = stats.dropna()
        if len( stats ) and ( args.election_type == None ):
            election['n_voters_registered'] = stats.loc['All Registered Voters'][1]
            print('All Registered Voters:', election['n_voters_registered'] )
            election['n_voters_active'] = stats.loc['Active Voters'][1]
            print( 'Active Voters:', election['n_voters_active'] )
        else:
            election['n_voters_registered'] = None
            election['n_voters_active'] = None

        # Extract table data
        df = xls.parse( sheet_name, skiprows=(2 if args.election_type == None else 0) )

        # Prepare data for saving to database
        df = util.prepare_for_database( df, 'Elections' )

        # Report any columns that contain no data
        report_empty_columns( df )

        # Save the dataframe
        election['df'] = df

        # Save current election in elections dictionary
        dc_elections[sheet_name] = election

    return dc_elections


# Load list of election representation models
def make_models( dc_elections ):

    dc_models = {}

    for sheet_name in dc_elections:

        # Find columns for current election
        model = dc_elections[sheet_name]['df'].columns

        # Search for matching model
        found = False
        for model_name in dc_models:
            test_model = dc_elections[model_name]['df'].columns
            found = ( len( model.difference( test_model ) ) == 0 ) and ( len( test_model.difference( model ) ) == 0 )
            if found:
                dc_models[model_name]['sheet_names'].append( sheet_name )
                break

        if not found:
            dc_models[sheet_name] = { 'model': model, 'sheet_names': [sheet_name] }

    print( '\n\n-- Models --')

    print( '\nNumber of models found: {0}'.format( len( dc_models ) ) )

    for model_name in dc_models:
        model = dc_models[model_name]['model']
        print( "\nModel name: '{0}'\n   Model: {1}\n   Number of examples: {2}\n   List of examples: {3}".format( model_name, model, len( dc_models[model_name]['sheet_names'] ), dc_models[model_name]['sheet_names'] ) )

    return dc_models


def compare_models( dc_models ):

    print( '\n\n-- Election model comparisons --')

    n_models = len( dc_models )
    ls_model_names = list( dc_models.keys() )

    # Compare pairs of models
    for i in range( n_models ):

        # Initialize comparison model index
        j = i + 1

        # Compare current model (i) to all subsequent models
        while j < n_models:

            # Get two models to be compared
            name_i = ls_model_names[i]
            name_j = ls_model_names[j]
            model_i = dc_models[name_i]['model']
            model_j = dc_models[name_j]['model']

            print( '\nComparing models {0} and {1}:'.format( name_i, name_j ) )

            # Compare model i to model j
            print( '\n- Intersection -' )
            print( list( model_i.intersection( model_j ) ) )

            # Subtract model j from model i
            print( "\n- '{0}' minus '{1}' -".format( name_i, name_j ) )
            print( list( model_i.difference( model_j ) ) )

            # Subtract model i from model j
            print( "\n- '{0}' minus '{1}' -".format( name_j, name_i ) )
            print( list( model_j.difference( model_i ) ) )

            print( '---' )

            # Increment comparison model index
            j += 1


# Find per-table and global constants
def find_constants( dc_elections ):

    print( '\n-- Per-sheet Constants and Variables --\n' )

    global_constants = {}
    for name in dc_elections:
        election = dc_elections[name]

        # Get dataframe representing current election
        df = election['df']

        # Find columns for current election
        columns = df.columns

        # Identify constants and variables in current dataframe
        ls_constants = []
        ls_variables = []
        for column_name in columns:
            sr_counts = df[column_name].value_counts( dropna=False )
            if len( sr_counts ) == 1:
                value = sr_counts.index[0]
                ls_constants.append( { 'name': column_name, 'value': value } )
                if column_name not in global_constants:
                    global_constants[column_name] = {}
                global_constants[column_name][value] = None
            else:
                ls_variables.append( column_name )

        # Save in election data structure
        election['constants'] = ls_constants
        election['variables'] = ls_variables

        # Report
        print( name )
        print( ' Constants:', ls_constants )
        print( ' Variables:', ls_variables )

    # Finish generating list of global constants
    ls_not_global = []
    for column_name in global_constants:
        column_values = global_constants[column_name]
        n_distinct_values = len ( column_values.keys() )
        if n_distinct_values > 1:
            ls_not_global.append( column_name )
    for column_name in ls_not_global:
        global_constants.pop( column_name )
    for column_name in global_constants:
        global_constants[column_name] = list( global_constants[column_name].keys() )[0]
    print( '\n-- Global Constants --' )
    print( global_constants )


# Build a dictionary of election models represented in the database
def get_db_models( conn, cur ):

    # Initialize empty dictionary
    dc_db_models = {}

    # Fetch names of all tables
    cur.execute( 'SELECT name FROM sqlite_master WHERE type="table";' )
    rows = cur.fetchall()

    # Traverse list of table names
    for row in rows:
        table_name = row[0]

        # If table represents an election, add the model to the list
        if table_name.startswith( TABLE_NAME_PREFIX ):

            # Get list of column names
            cur.execute( 'PRAGMA table_info( "' + table_name + '" );' )
            table_info = cur.fetchall()

            ls_col_names = []
            for info in table_info:
                ls_col_names.append( info[1] )

            # Save model as a set for easy comparison
            dc_db_models[table_name] = set( ls_col_names ) - { util.ID }

    return dc_db_models


# Save all sheets that share the same model to the corresponding table
def save_model( target_table_name, xl_model, conn, cur, engine ):

    # Get list of sheets to be saved in target table
    sheet_names = xl_model['sheet_names']
    print( '\nSheets {0}'.format( sheet_names ) )
    print( '  Saving to table {0}'.format( target_table_name ) )

    # Load dataframe with contents of target table
    df = pd.read_sql_table( target_table_name, engine, index_col=util.ID, parse_dates=util.get_date_columns( dc_elections[sheet_names[0]]['df'] ) )

    # Append all sheets to the dataframe
    for sheet_name in sheet_names:
        dc_elections[sheet_name]['df'] = dc_elections[sheet_name]['df'].reindex( columns=df.columns )
        df = df.append( dc_elections[sheet_name]['df'] )

    # Drop duplicate rows
    df = df.drop_duplicates()

    # Recreate the table in the database
    util.create_table( target_table_name, conn, cur, df=df )


# Save elections in database
def save_elections( dc_elections, dc_models ):

    print( '\n\n-- Loading database --' )

    # Open the database
    conn, cur, engine = util.open_database( args.output_filename, args.create )

    # Get list of election models from database
    dc_db_models = get_db_models( conn, cur )

    # Iterate over Excel models to determine how sheets following that model should be saved
    for model_name in dc_models:

        # Get next Excel model
        xl_model = set( dc_models[model_name]['model'] )
        sheet_names = dc_models[model_name]['sheet_names']

        # Search for a matching model among database models
        target_table_name = None
        for table_name in dc_db_models:

            # Get next database model
            db_model = dc_db_models[table_name]

            # Calculate differences between Excel and database models
            db_minus_xl = db_model - xl_model
            xl_minus_db = xl_model - db_model

            # If they match, set the target table name and quit loop
            if ( len( db_minus_xl ) == 0 ) and ( len( xl_minus_db ) == 0 ):
                target_table_name = table_name
                break


        # If table with matching model not found in database, create an empty one
        if not target_table_name:
            target_table_name = TABLE_NAME_PREFIX + '{:02d}'.format( len( dc_db_models ) + 1 )
            util.create_table( target_table_name, conn, cur, columns=dc_models[model_name]['model'] )
            dc_db_models = get_db_models( conn, cur )

        save_model( target_table_name, dc_models[model_name], conn, cur, engine )


#######################################


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Process election data' )

    # Arguments for reading Excel file and writing to database
    parser.add_argument( '-i', dest='input_filename',  help='Input filename - Name of MS Excel file' )
    parser.add_argument( '-o', dest='output_filename',  help='Output filename - Name of SQLite database file' )
    parser.add_argument( '-e', dest='election_type',  help='Type of election' )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )

    args = parser.parse_args()

    # Load dictionary of elections
    dc_elections = make_elections()

    # Load list of models
    dc_models = make_models( dc_elections )

    if args.output_filename is not None:
        # Load election data into database
        save_elections( dc_elections, dc_models )
    else:
        # Explore the data
        compare_models( dc_models )
        find_constants( dc_elections )


    # Report elapsed time
    util.report_elapsed_time()
