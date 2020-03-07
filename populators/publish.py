# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
import re

import sys
sys.path.append( '../util' )
import util



def publish_database( input_db, output_filename, publish_info ):

    drop_table_names = publish_info['drop_table_names']
    drop_column_names = publish_info['drop_column_names']
    encipher_column_names = publish_info['encipher_column_names']
    number_columns = publish_info['number_columns']

    print( '' )
    print( 'Publishing database', output_filename )
    print( '- Dropping tables {0}'.format( drop_table_names ) )
    print( '- Dropping columns {0}'.format( drop_column_names ) )
    print( '- Enciphering columns {0}'.format( encipher_column_names ) )
    print( '' )

    output_db = {}
    for table_name in input_db:
        if table_name not in drop_table_names:
            print( 'Sanitizing table {0}'.format( table_name ) )
            output_db[table_name] = input_db[table_name].drop( columns=drop_column_names, errors='ignore' )
            for col_name in encipher_column_names:
                output_db[table_name] = util.encipher_column( output_db[table_name], col_name )

            if number_columns:
                n_cols = len( output_db[table_name].columns )
                num_width = len( str( n_cols ) )
                col_idx = 0
                for column_name in output_db[table_name].columns:
                    col_idx += 1
                    output_db[table_name] = output_db[table_name].rename( columns={ column_name: str( col_idx ).zfill( num_width ) + '-' + column_name } )


    # Open output database
    conn, cur, engine = util.open_database( output_filename, True )

    # Save result to database
    print( '' )
    for table_name in output_db:
        print( 'Publishing table', table_name )
        df = output_db[table_name]
        df.to_sql( table_name, conn, index=False )


#######################################


# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Generate copy of master database for publication' )
    parser.add_argument( '-i', dest='input_filename',  help='Input filename' )
    parser.add_argument( '-o', dest='output_folder',  help='Output folder' )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Generate debug versions of databases?' )
    args = parser.parse_args()

    # Read input database
    input_db = util.read_database( args.input_filename )

    # Publish output databases
    debug = '_debug' if args.debug else ''
    for output_db_name, publish_info in util.PUBLISH_INFO.items():
        publish_database( input_db, args.output_folder + '/' + output_db_name + debug + '.sqlite', publish_info )

    # Report elapsed time
    util.report_elapsed_time()

