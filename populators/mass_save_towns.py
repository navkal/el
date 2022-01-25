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
    parser.add_argument( '-p', dest='population_filename',  help='Input file containing list of town populations' )
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

    # Read population data
    df_pop = pd.read_excel( args.population_filename, dtype=object )
    df_pop = df_pop.rename( columns={ 'Municipality': util.TOWN_NAME, '2020': util.POPULATION } )
    df_pop = df_pop[ [util.TOWN_NAME, util.POPULATION] ]

    # Fix some naming discrepancies
    df_pop.at[ df_pop[ df_pop[util.TOWN_NAME] == 'Manchester By The Sea' ].index, util.TOWN_NAME] = 'Manchester'
    df_pop.at[ df_pop[ df_pop[util.TOWN_NAME] == 'North Attleborough' ].index, util.TOWN_NAME] = 'North Attleboro'

    # Merge towns and populations
    df_towns = pd.merge( df_towns, df_pop, how='left', on=[util.TOWN_NAME] )

    # Add column representing percent low income
    df_towns[util.PCT_ENERGY_BURDENED] = 5

    # Save result to database
    util.create_table( "Towns", conn, cur, df=df_towns )

    # Report elapsed time
    util.report_elapsed_time()
