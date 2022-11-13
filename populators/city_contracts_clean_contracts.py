# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Clean up raw vendors table' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-i', dest='input_table', help='Input table name' )
    parser.add_argument( '-o', dest='output_table', help='Output table name' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read table from database
    df = pd.read_sql_table( args.input_table, engine, index_col=util.ID )

    # Clean
    df[util.VENDOR_NUMBER] = df[util.VENDOR].str.extract( r'\(([0-9]+)\)').astype( int )
    df[util.AMOUNT] = pd.to_numeric( df[util.AMOUNT], errors='coerce' )

    # Save result to database
    util.create_table( args.output_table, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
