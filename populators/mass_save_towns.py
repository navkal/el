# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

DEFAULT_POVERTY_RATE = 6.5

DATA_YEAR = '2019'
MWH_USED = DATA_YEAR + util.ELECTRIC_USAGE
THERMS_USED = DATA_YEAR + util.GAS_USAGE
KWH_USED_PER_CAPITA = DATA_YEAR + '_kwh_used_per_capita'
THERMS_USED_PER_CAPITA = DATA_YEAR + '_therms_used_per_capita'

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Create table of MA towns' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    parser.add_argument( '-p', dest='population_filename',  help='Input file containing list of town populations' )
    parser.add_argument( '-e', dest='energy_burden_filename',  help='Input file containing poverty data' )
    parser.add_argument( '-u', dest='elec_utilities_filename',  help='Input file mapping town to electric utility' )
    parser.add_argument( '-v', dest='gas_utilities_filename',  help='Input file mapping town to gas utility' )
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

    # Merge towns and populations, first fixing mismatched names
    df_pop.at[ df_pop[ df_pop[util.TOWN_NAME] == 'Manchester By The Sea' ].index, util.TOWN_NAME] = 'Manchester'
    df_pop.at[ df_pop[ df_pop[util.TOWN_NAME] == 'North Attleborough' ].index, util.TOWN_NAME] = 'North Attleboro'
    df_towns = pd.merge( df_towns, df_pop, how='left', on=[util.TOWN_NAME] )

    # Read energy burden data
    df_eb = pd.read_excel( args.energy_burden_filename, dtype=object )
    df_eb = df_eb.rename( columns={ 'town_name': util.TOWN_NAME, 'poverty_rate': util.PCT_ENERGY_BURDENED } )

    # Merge energy burden data
    df_towns = pd.merge( df_towns, df_eb, how='left', on=[util.TOWN_NAME] )
    df_towns[util.PCT_ENERGY_BURDENED] = df_towns[util.PCT_ENERGY_BURDENED] * 100
    df_towns[util.PCT_ENERGY_BURDENED] = df_towns[util.PCT_ENERGY_BURDENED].fillna( DEFAULT_POVERTY_RATE ).round( 1 )

    # Isolate 2019 residential usage statistics
    df_usage = df_gr.copy()
    df_usage = df_usage[ ( df_usage[util.SECTOR]==util.SECTOR_RES_AND_LOW ) & ( df_usage[util.YEAR]==int(DATA_YEAR) ) ]
    df_usage = df_usage[ [util.TOWN_NAME, util.ANNUAL_ELECTRIC_USAGE, util.ANNUAL_GAS_USAGE] ]

    # Calculate per-capita energy usage
    df_towns = pd.merge( df_towns, df_usage, how='left', on=[util.TOWN_NAME] )
    df_towns = df_towns.rename( columns={ util.ANNUAL_ELECTRIC_USAGE: MWH_USED, util.ANNUAL_GAS_USAGE: THERMS_USED } )
    df_towns[KWH_USED_PER_CAPITA] = 1000 * df_towns[MWH_USED] / df_towns[util.POPULATION]
    df_towns[THERMS_USED_PER_CAPITA] = df_towns[THERMS_USED] / df_towns[util.POPULATION]

    # Read and merge electric utility data
    df_e_ut = pd.read_excel( args.elec_utilities_filename, dtype=object )
    df_e_ut = df_e_ut.rename( columns={ 'Town': util.TOWN_NAME, 'Electric Utility': util.ELECTRIC_UTILITY, 'Electric Utility URL': util.ELECTRIC_UTILITY_URL } )
    df_towns = pd.merge( df_towns, df_e_ut, how='left', on=[util.TOWN_NAME] )

    # Read and merge gas utility data
    df_g_ut = pd.read_excel( args.gas_utilities_filename, dtype=object )
    df_g_ut = df_g_ut.rename( columns={ 'Town': util.TOWN_NAME, 'Gas Utility 1': util.GAS_UTILITY_1, 'Gas Utility URL 1': util.GAS_UTILITY_URL_1, 'Gas Utility 2': util.GAS_UTILITY_2, 'Gas Utility URL 2': util.GAS_UTILITY_URL_2 } )
    df_towns = pd.merge( df_towns, df_g_ut, how='left', on=[util.TOWN_NAME] )

    # Fix datatypes and precision
    df_towns[util.POPULATION] = df_towns[util.POPULATION].astype(int)
    df_towns[MWH_USED] = df_towns[MWH_USED].fillna(0).astype(int)
    df_towns[THERMS_USED] = df_towns[THERMS_USED].fillna(0).astype(int)
    df_towns[KWH_USED_PER_CAPITA] = df_towns[KWH_USED_PER_CAPITA].fillna(0).round( 2 )
    df_towns[THERMS_USED_PER_CAPITA] = df_towns[THERMS_USED_PER_CAPITA].fillna(0).round( 2 )

    # Save result to database
    util.create_table( "Towns", conn, cur, df=df_towns )

    # Report elapsed time
    util.report_elapsed_time()
