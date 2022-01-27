# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

def get_usage( df_group, sector ):

    try:
        # Retrieve residential row
        row = df_group[ df_group[util.SECTOR] == sector ]

        # Retrieve usage
        usage_mwh = row[util.ANNUAL_ELECTRIC_USAGE].values[0]
        usage_therms = row[util.ANNUAL_GAS_USAGE].values[0]

    except:
        usage_mwh = None
        usage_therms = None

    return ( usage_mwh, usage_therms )

def report_findings( sector, usage_mwh, usage_therms, proceeds_mwh, proceeds_therms ):

    print( sector )
    print( '  mwh: used {}, collected ${}'.format( usage_mwh, proceeds_mwh ) )
    print( '  therms: used {}, collected ${}'.format( usage_therms, proceeds_therms ) )



def do_town( town_row ):

    town = town_row[util.TOWN_NAME]

    eb_factor = town_row[util.PCT_ENERGY_BURDENED] / 100

    print( '' )
    print( '=== {} ==='.format( town ) )

    # Get geographic report for this town
    df_gr_town = df_gr[ df_gr[util.TOWN_NAME] == town ]

    # Group this town's report by year
    for idx, df_group in df_gr_town.groupby( by=[util.YEAR] ):

        # Get year of group
        year = df_group.iloc[0][util.YEAR]
        print( '' )
        print( '>', year, '<' )

        # Retrieve surcharge values used in this year
        surcharge_row = df_ees[ df_ees[util.YEAR] == year ]

        #
        # df_group typically contains 3 rows: Residential sector, Commercial sector, and Total
        # We look at Residential and Commercial rows
        #

        # Residential
        sector = 'Residential & Low-Income'
        usage_mwh, usage_therms = get_usage( df_group, sector )

        if usage_mwh and usage_therms:

            # Calculate proceeds
            proceeds_r1 = ( 1 - eb_factor ) * usage_mwh * surcharge_row[util.RESIDENTIAL_DOL_PER_MWH].values[0]
            proceeds_r2 = eb_factor * usage_mwh * surcharge_row[util.DISCOUNT_DOL_PER_MWH].values[0]
            proceeds_mwh = int( proceeds_r1 + proceeds_r2 )
            proceeds_therms = int( usage_therms * surcharge_row[util.RESIDENTIAL_DOL_PER_THERM].values[0] )

            # Report findings
            report_findings( sector, usage_mwh, usage_therms, proceeds_mwh, proceeds_therms )

        # Commercial
        sector = 'Commercial & Industrial'
        usage_mwh, usage_therms = get_usage( df_group, sector )

        if usage_mwh and usage_therms:

            # Calculate proceeds
            proceeds_mwh = int( usage_mwh * surcharge_row[util.COMMERCIAL_DOL_PER_MWH].values[0] )
            proceeds_therms = int( usage_therms * surcharge_row[util.COMMERCIAL_DOL_PER_THERM].values[0] )

            # Report findings
            report_findings( sector, usage_mwh, usage_therms, proceeds_mwh, proceeds_therms )


if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Make calculations based on MassSave data' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read tables from database
    df_gr = pd.read_sql_table( 'GeographicReport', engine, index_col=util.ID )
    df_towns = pd.read_sql_table( 'Towns', engine, index_col=util.ID )
    df_ees = pd.read_sql_table( 'EnergyEfficiencySurcharge', engine, index_col=util.ID )

    for index, row in df_towns.iterrows():
        do_town( row )



    exit()



    util.create_table( "Towns", conn, cur, df=df_towns )

    # Report elapsed time
    util.report_elapsed_time()
