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
    parser = argparse.ArgumentParser( description='Clean up raw energy usage table from MassSave' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    parser.add_argument( '-z', dest='zip_code_filename',  help='Input file containing list of ZIP codes' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read tables from database
    df_eu = pd.read_sql_table( 'ElectricUsage', engine, index_col=util.ID )
    df_gu = pd.read_sql_table( 'GasUsage', engine, index_col=util.ID )
    df_gr = pd.read_sql_table( 'GeographicReport', engine, index_col=util.ID )

    # Create list of all towns
    sr_towns = df_eu[util.TOWN_NAME].append( df_gu[util.TOWN_NAME].append( df_gr[util.TOWN_NAME], ignore_index=True ), ignore_index=True ).drop_duplicates()

    # Load towns into dataframe and sort
    df_towns = pd.DataFrame( { util.TOWN_NAME: sr_towns } )
    df_towns = df_towns.sort_values( by=[util.TOWN_NAME] )
    df_towns = df_towns.reset_index( drop=True )

    # Read zip code data
    df_zip = pd.read_csv( args.zip_code_filename )
    df_zip = df_zip[ (df_zip['state'] == 'MA') & (df_zip['type'] == 'STANDARD') ]
    df_zip = pd.DataFrame( { util.ZIP: df_zip['zip'], util.TOWN_NAME: df_zip['primary_city'] } )

    # Merge towns and zip codes
    df_merge = pd.merge( df_towns, df_zip, how='left', on=[util.TOWN_NAME] )

    # Associate each town with one or more zip codes
    for idx, df_group in df_merge.groupby( by=[util.TOWN_NAME] ):

        df_group = df_group.fillna( '' )

        # Format list of zip codes
        zip_list = util.fix_zip_code( df_group[util.ZIP] ).tolist()
        zip_string = ','.join( zip_list )

        # Save zip code list with current town
        current_town = df_group[util.TOWN_NAME].iloc[0]
        current_town_index = df_towns[df_towns[util.TOWN_NAME] == current_town].index[0]
        df_towns.at[ current_town_index, util.ZIP_CODES ] = zip_string


    # Add column representing percent low income
    df_towns[util.PCT_LOW_INCOME] = 5

    # Save result to database
    util.create_table( "Towns", conn, cur, df=df_towns )

    # Report elapsed time
    util.report_elapsed_time()
