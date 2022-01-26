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

    # Read MassSave tables from database
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

    #
    # Add energy burden columns
    #

    # Read energy burden data from database
    df_ej = pd.read_sql_table( 'EjCommunities', engine, index_col=util.ID )
    df_ej = df_ej[ [util.TOWN_NAME, util.TRACT_POPULATION, util.PCT_LOW_INCOME] ]
    df_merge = pd.merge( df_towns, df_ej, how='left', on=[util.TOWN_NAME] )

    # Set default values
    df_towns[util.ENERGY_BURDENED_POPULATION] = None
    df_towns[util.PCT_ENERGY_BURDENED] = 5

    # Use energy burden data to override default values
    for idx, df_group in df_merge.groupby( by=[util.TOWN_NAME] ):

        # If we have energy burden data for this town...
        if not df_group[util.TRACT_POPULATION].isnull().all():

            # Calculate energy burden statistics
            population = df_group[util.POPULATION].values[0]
            energy_burdened_population = int( ( df_group[util.TRACT_POPULATION] * df_group[util.PCT_LOW_INCOME] ).sum() )
            pct_energy_burdened = 100 * energy_burdened_population / population

            # Save statistics in town dataframe
            town = df_group[util.TOWN_NAME].iloc[0]
            town_index = df_towns[df_towns[util.TOWN_NAME] == town].index[0]
            df_towns.at[ town_index, util.ENERGY_BURDENED_POPULATION ] = energy_burdened_population
            df_towns.at[ town_index, util.PCT_ENERGY_BURDENED ] = pct_energy_burdened

    # Save result to database
    df_towns[util.POPULATION] = df_towns[util.POPULATION].astype(int)
    df_towns[util.ENERGY_BURDENED_POPULATION] = df_towns[util.ENERGY_BURDENED_POPULATION]
    df_towns[util.PCT_ENERGY_BURDENED] = df_towns[util.PCT_ENERGY_BURDENED].astype(int)
    util.create_table( "Towns", conn, cur, df=df_towns )

    # Report elapsed time
    util.report_elapsed_time()
