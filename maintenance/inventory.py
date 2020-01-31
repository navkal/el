# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import re

import sys
sys.path.append( '../util' )
import util

import os
import glob


#######################################


def save_inventory( db_name, db ):

    xlsx = '../inventory/' + db_name + '.xlsx'

    print( '' )
    print( 'Saving inventory at', xlsx )
    print( '' )

    # Write dataframes to multiple sheets in single Excel file
    with pd.ExcelWriter( xlsx ) as writer:

        # Get sorted list of table names
        table_names = list( db.keys() )
        table_names.sort()

        # Create one sheet per table
        for table_name in table_names:
            print( 'Saving sheet', table_name )
            df = pd.DataFrame( data={ 'columns': db[table_name].columns } )
            df.to_excel( writer, sheet_name=table_name, index=False )


#######################################


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate copy of master database for publication' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename' )
    args = parser.parse_args()

    # Create list of databases to be inventoried
    if args.input_filename is not None:

        # Caller specified a specific database
        ls_dbs = [ args.input_filename ]

    else:

        # Default - build list of available databases
        cwd = os.getcwd()

        dirname = '../db/'
        os.chdir( dirname )
        ls_dbs = []
        for filename in glob.glob( '*.sqlite' ):
            ls_dbs.append( dirname + filename )

        os.chdir( cwd )

    # Save inventory of listed databases
    for filename in ls_dbs:
        db = util.read_database( filename )
        if len( db ) > 3:
            save_inventory( os.path.splitext( os.path.basename( filename ) )[0] , db )

    # Report elapsed time
    util.report_elapsed_time()

