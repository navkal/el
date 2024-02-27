# Copyright 2024 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util


COLUMNS= \
[
    util.CENSUS_TRACT,
    util.TOTAL_MV,
    util.HEAVY_DUTY_MV,
    util.MEDIUM_DUTY_MV,
    util.LIGHT_DUTY_MV,
    util.FOSSIL_FUEL_MV,
    util.HYBRID_MV,
    util.ZERO_EMISSION_MV,
    util.EV,
    util.PHEV,
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
        summary_row[util.TOTAL_MV] = df_group[util.COUNT].sum()

        col = util.GVWR_CATEGORY
        summary_row[util.HEAVY_DUTY_MV] = df_group[ df_group[col] == 'Heavy Duty'][util.COUNT].sum()
        summary_row[util.MEDIUM_DUTY_MV] = df_group[ df_group[col] == 'Medium Duty'][util.COUNT].sum()
        summary_row[util.LIGHT_DUTY_MV] = df_group[ df_group[col] == 'Light Duty'][util.COUNT].sum()

        col = util.FUEL_CLASS
        summary_row[util.FOSSIL_FUEL_MV] = df_group[ df_group[col] == 'Fossil Fuel'][util.COUNT].sum()
        summary_row[util.ZERO_EMISSION_MV] = df_group[ df_group[col] == 'Zero-Emission'][util.COUNT].sum()
        summary_row[util.HYBRID_MV] = df_group[ df_group[col] == 'Hybrid'][util.COUNT].sum()

        col = util.ADVANCED_VEHICLE_TYPE
        summary_row[util.EV] = df_group[ df_group[col] == 'Electric Vehicle'][util.COUNT].sum()
        summary_row[util.PHEV] = df_group[ df_group[col] == 'Plug-in Hybrid Electric Vehicle'][util.COUNT].sum()

        df_summary = df_summary.append( summary_row, ignore_index=True )


    # Save to database
    util.create_table( 'MotorVehicleSummary_L', conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
