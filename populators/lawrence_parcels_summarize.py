# Copyright 2023 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append('../util')
import util
import vision


# Column labels
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCU = util.OCCUPANCY_HOUSEHOLDS
FUEL = util.HEATING_FUEL_DESC
KTCH = util.KITCHENS
BATH = util.BATHS
BLDS = util.BUILDING_COUNT
ISRS = util.IS_RESIDENTIAL


# Column name fragments for summary table
SUMMARY_FUEL_TYPES = \
{
    'Oil': util.OIL_,
    'Gas': util.GAS_,
    'Electric': util.ELEC_,
}
KTCH_TOT = util.KITCHENS_ + util.TOTAL
BATH_TOT = util.BATHS_ + util.TOTAL
OCCU_TOT = util.OCCUPANCY_ + util.TOTAL


def make_summary_row( idx, df_group ):
    sr_summary = pd.Series( dtype=object )
    sr_summary[STREET_NAME] = idx

    # Street features
    sr_summary[util.PARCEL_COUNT] = len( df_group )
    sr_summary[BLDS] = df_group[BLDS].sum()

    # Building features
    sr_summary[KTCH_TOT] = df_group[KTCH].sum()
    sr_summary[BATH_TOT] = df_group[BATH].sum()
    sr_summary[OCCU_TOT] = df_group[OCCU].sum()

    # Building features by fuel type
    for fuel_type in SUMMARY_FUEL_TYPES.items():

        df_fuel = df_group[ df_group[FUEL]==fuel_type[0] ]

        if len( df_fuel ):
            sr_summary[fuel_type[1] + KTCH_TOT] = df_fuel[KTCH].sum()
            sr_summary[fuel_type[1] + BATH_TOT] = df_fuel[BATH].sum()
            sr_summary[fuel_type[1] + OCCU_TOT] = df_fuel[OCCU].sum()

    return sr_summary


def make_summary( s_res, df ):

    print( 'Summarizing {} {} VISION IDs'.format( len( df ), s_res ) )

    # Drop empty street names
    df = df[ df[STREET_NAME].str.len() > 0 ]

    # Initialize summary dataframe
    df_summary = pd.DataFrame()

    # Iterate through street groups
    for idx, df_group in df.groupby( by=[STREET_NAME] ):

        # Summarize current street
        sr_summary = make_summary_row( idx, df_group )

        # Append row to summary dataframe
        if df_summary.empty:
            df_summary = pd.DataFrame( columns=sr_summary.index )
        df_summary = df_summary.append( sr_summary, ignore_index=True )

    # Convert float to integer
    for col_name in df_summary.columns[1:]:
        df_summary[col_name] = vision.clean_integer( df_summary[col_name] )

    return df_summary


######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Summarize cleaned parcel assessment data' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read cleaned table from database
    df = pd.read_sql_table( 'Parcels_L', engine, index_col=util.ID, parse_dates=True )

    # Summarize residential parcels
    df_res = make_summary( 'Residential', df[ df[ISRS]==util.YES ] )
    df_res[ISRS] = util.YES

    # Summarize commercial parcels
    df_com = make_summary( 'Commercial', df[ df[ISRS]==util.NO ] )
    df_com[ISRS] = util.NO

    # Combine and sort
    df_summary = pd.concat( [df_res, df_com ] )
    df_summary = df_summary.sort_values( by=[STREET_NAME, ISRS], ascending=[True, False] )

    # Save to database
    util.create_table( 'ParcelSummary_L', conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
