# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


FOSSIL_FUEL = 'fossil_fuel'
HEAVY_DUTY = 'heavy_duty'
MEDIUM_DUTY = 'medium_duty'
LIGHT_DUTY = 'light_duty'
HYBRID = 'hybrid'
ZERO_EMISSION = 'zero_emission'
EV = 'ev'
PHEV = 'phev'

COLUMNS= \
[
    util.CENSUS_TRACT,
    util.TOTAL,
    HEAVY_DUTY,
    MEDIUM_DUTY,
    LIGHT_DUTY,
    FOSSIL_FUEL,
    HYBRID,
    ZERO_EMISSION,
    EV,
    PHEV,
]

######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Summarize data on motor vehicles in Lawrence' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read table from database
    df = pd.read_sql_table( 'MotorVehicles_L', engine, index_col=util.ID, parse_dates=True )

    # Initialize summary table
    df_summary = pd.DataFrame( columns=COLUMNS )

    # Generate summary table
    for idx, df_group in df.groupby( by=[util.CENSUS_TRACT] ):

        # Initialize empty summary row
        summary_row = dict( ( el, 0 ) for el in COLUMNS )

        # Load summary row
        summary_row[util.CENSUS_TRACT] = idx
        summary_row[util.TOTAL] = df_group[util.COUNT].sum()

        col = util.FUEL_CLASS
        summary_row[FOSSIL_FUEL] = len( df_group[ df_group[col] == 'Fossil Fuel'] )
        summary_row[ZERO_EMISSION] = len( df_group[ df_group[col] == 'Zero-Emission'] )
        summary_row[HYBRID] = len( df_group[ df_group[col] == 'Hybrid'] )

        col = util.GVWR_CATEGORY
        summary_row[HEAVY_DUTY] = len( df_group[ df_group[col] == 'Heavy Duty'] )
        summary_row[MEDIUM_DUTY] = len( df_group[ df_group[col] == 'Medium Duty'] )
        summary_row[LIGHT_DUTY] = len( df_group[ df_group[col] == 'Light Duty'] )

        col = util.ADVANCED_VEHICLE_TYPE
        summary_row[EV] = len( df_group[ df_group[col] == 'Electric Vehicle'] )
        summary_row[PHEV] = len( df_group[ df_group[col] == 'Plug-in Hybrid Electric Vehicle'] )

        df_summary = df_summary.append( summary_row, ignore_index=True )


    # Save to database
    util.create_table( 'MotorVehicleSummary_L', conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
