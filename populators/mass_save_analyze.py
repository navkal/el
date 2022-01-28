# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

SECTOR_RES_AND_LOW = 'Residential & Low-Income'
SECTOR_COM_AND_IND = 'Commercial & Industrial'
SECTOR_TOTAL = 'Total'

ELECTRIC_SURCHARGE_PROCEEDS = 'electric_surcharge_proceeds'
GAS_SURCHARGE_PROCEEDS = 'gas_surcharge_proceeds'
SURPLUS_ELECTRIC_PROCEEDS = 'surplus_electric_proceeds'
SURPLUS_GAS_PROCEEDS = 'surplus_gas_proceeds'
SURPLUS_PROCEEDS_TOTAL = 'surplus_proceeds_total'


def get_usage_values( df_group, sector ):

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


def report_findings( year, town, sector, proceeds_electric, proceeds_gas ):

    # Find row and retrieve values
    row = df_analysis[ ( df_analysis[util.YEAR] == year ) & ( df_analysis[util.TOWN_NAME] == town ) & ( df_analysis[util.SECTOR] == sector ) ]
    surplus_proceeds_electric = proceeds_electric - row[util.ELECTRIC_INCENTIVES]
    surplus_proceeds_gas = proceeds_gas - row[util.GAS_INCENTIVES]
    surplus_proceeds_total = surplus_proceeds_electric + surplus_proceeds_gas

    index = row.index.values[0]
    df_analysis.at[index, ELECTRIC_SURCHARGE_PROCEEDS] = proceeds_electric
    df_analysis.at[index, GAS_SURCHARGE_PROCEEDS] = proceeds_gas
    df_analysis.at[index, SURPLUS_ELECTRIC_PROCEEDS] = surplus_proceeds_electric
    df_analysis.at[index, SURPLUS_GAS_PROCEEDS] = surplus_proceeds_gas
    df_analysis.at[index, SURPLUS_PROCEEDS_TOTAL] = surplus_proceeds_total


def report_totals( year, town ):
    df_findings = df_analysis[ ( df_analysis[util.YEAR] == year ) & ( df_analysis[util.TOWN_NAME] == town ) & ( df_analysis[util.SECTOR] != SECTOR_TOTAL ) ]
    df_totals = df_analysis[ ( df_analysis[util.YEAR] == year ) & ( df_analysis[util.TOWN_NAME] == town ) & ( df_analysis[util.SECTOR] == SECTOR_TOTAL ) ]

    if len( df_findings ) and len( df_totals ):
        index = df_totals.index.values[0]
        df_analysis.at[index, ELECTRIC_SURCHARGE_PROCEEDS] = df_findings[ELECTRIC_SURCHARGE_PROCEEDS].sum()
        df_analysis.at[index, GAS_SURCHARGE_PROCEEDS] = df_findings[GAS_SURCHARGE_PROCEEDS].sum()
        df_analysis.at[index, SURPLUS_ELECTRIC_PROCEEDS] = df_findings[SURPLUS_ELECTRIC_PROCEEDS].sum()
        df_analysis.at[index, SURPLUS_GAS_PROCEEDS] = df_findings[SURPLUS_GAS_PROCEEDS].sum()
        df_analysis.at[index, SURPLUS_PROCEEDS_TOTAL] = df_findings[SURPLUS_PROCEEDS_TOTAL].sum()


def analyze_town( town_row ):

    town = town_row[util.TOWN_NAME]

    eb_factor = town_row[util.PCT_ENERGY_BURDENED] / 100

    print( town )

    # Get geographic report for this town
    df_gr_town = df_gr[ df_gr[util.TOWN_NAME] == town ]

    # Group this town's report by year
    for idx, df_group in df_gr_town.groupby( by=[util.YEAR] ):

        # Get year of group
        year = df_group.iloc[0][util.YEAR]

        # Retrieve surcharge values used in this year
        surcharge_row = df_ees[ df_ees[util.YEAR] == year ]

        #
        # df_group typically contains 3 rows: Residential sector, Commercial sector, and Total
        # We look at Residential and Commercial rows
        #

        # Residential
        sector = SECTOR_RES_AND_LOW
        usage_mwh, usage_therms = get_usage_values( df_group, sector )

        if usage_mwh and usage_therms:

            # Calculate proceeds
            proceeds_r1 = ( 1 - eb_factor ) * usage_mwh * surcharge_row[util.RESIDENTIAL_DOL_PER_MWH].values[0]
            proceeds_r2 = eb_factor * usage_mwh * surcharge_row[util.DISCOUNT_DOL_PER_MWH].values[0]
            proceeds_electric = int( proceeds_r1 + proceeds_r2 )
            proceeds_gas = int( usage_therms * surcharge_row[util.RESIDENTIAL_DOL_PER_THERM].values[0] )

            # Report findings
            report_findings( year, town, sector, proceeds_electric, proceeds_gas )

        # Commercial
        sector = SECTOR_COM_AND_IND
        usage_mwh, usage_therms = get_usage_values( df_group, sector )

        if usage_mwh and usage_therms:

            # Calculate proceeds
            proceeds_electric = int( usage_mwh * surcharge_row[util.COMMERCIAL_DOL_PER_MWH].values[0] )
            proceeds_gas = int( usage_therms * surcharge_row[util.COMMERCIAL_DOL_PER_THERM].values[0] )

            # Report findings
            report_findings( year, town, sector, proceeds_electric, proceeds_gas )

        report_totals( year, town )


if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Analyze MassSave data' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read tables from database
    df_gr = pd.read_sql_table( 'GeographicReport', engine, index_col=util.ID )
    df_towns = pd.read_sql_table( 'Towns', engine, index_col=util.ID )
    df_ees = pd.read_sql_table( 'EfficiencySurcharge', engine, index_col=util.ID )

    # Initialize dataframe of analysis results
    df_analysis = df_gr.copy( deep=True )
    df_analysis[ELECTRIC_SURCHARGE_PROCEEDS] = 0
    df_analysis[GAS_SURCHARGE_PROCEEDS] = 0
    df_analysis[SURPLUS_ELECTRIC_PROCEEDS] = 0
    df_analysis[SURPLUS_GAS_PROCEEDS] = 0
    df_analysis[SURPLUS_PROCEEDS_TOTAL] = 0

    # Analyze the towns
    for index, row in df_towns.iterrows():
        analyze_town( row )

    # Save analysis results to database
    util.create_table( "Analysis", conn, cur, df=df_analysis )

    # Report elapsed time
    util.report_elapsed_time()
