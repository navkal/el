# Copyright 2022 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

ELECTRIC_EES = 'electric_ees_$'
GAS_EES = 'gas_ees_$'
ELECTRIC_EES_MINUS_INCENTIVES = 'electric_ees_minus_incentives_$'
GAS_EES_MINUS_INCENTIVES = 'gas_ees_minus_incentives_$'

FIRST_NUMERIC_COLUMN = 4

def get_usage_values( df_group, sector ):

    try:
        # Retrieve row for given sector
        row = df_group[ df_group[util.SECTOR] == sector ]

        # Retrieve usage
        usage_mwh = row[util.ANNUAL_ELECTRIC_USAGE].values[0]
        usage_therms = row[util.ANNUAL_GAS_USAGE].values[0]

    except:
        usage_mwh = None
        usage_therms = None

    return ( usage_mwh, usage_therms )


def report_findings( year, town, sector, electric_ees, gas_ees ):

    # Retrieve values
    row = df_analysis[ ( df_analysis[util.YEAR] == year ) & ( df_analysis[util.TOWN_NAME] == town ) & ( df_analysis[util.SECTOR] == sector ) ]
    electric_incentives = row[util.ELECTRIC_INCENTIVES].values[0]
    gas_incentives = row[util.GAS_INCENTIVES].values[0]
    mwh_saved = row[util.ANNUAL_ELECTRIC_SAVINGS].values[0]
    therms_saved = row[util.ANNUAL_GAS_SAVINGS].values[0]

    # Calculate findings
    electric_ees_minus_incentives = electric_ees - electric_incentives
    gas_ees_minus_incentives = gas_ees - gas_incentives
    incentives_per_saved_mwh = ( electric_incentives / mwh_saved ) if mwh_saved else 0
    incentives_per_saved_therm = ( gas_incentives / therms_saved ) if therms_saved else 0
    combined_ees_in = electric_ees + gas_ees
    combined_incentives_out = electric_incentives + gas_incentives
    combined_ees_minus_incentives = electric_ees_minus_incentives + gas_ees_minus_incentives

    # Report findings in analysis dataframe
    index = row.index.values[0]
    df_analysis.at[index, ELECTRIC_EES] = electric_ees
    df_analysis.at[index, GAS_EES] = gas_ees
    df_analysis.at[index, ELECTRIC_EES_MINUS_INCENTIVES] = electric_ees_minus_incentives
    df_analysis.at[index, GAS_EES_MINUS_INCENTIVES] = gas_ees_minus_incentives
    df_analysis.at[index, util.INCENTIVES_PER_SAVED_MWH] = incentives_per_saved_mwh
    df_analysis.at[index, util.INCENTIVES_PER_SAVED_THERM] = incentives_per_saved_therm
    df_analysis.at[index, util.COMBINED_EES_IN] = combined_ees_in
    df_analysis.at[index, util.COMBINED_INCENTIVES_OUT] = combined_incentives_out
    df_analysis.at[index, util.COMBINED_EES_MINUS_INCENTIVES] = combined_ees_minus_incentives


def report_totals( year, town ):

    global df_analysis_totals

    df_findings = df_analysis[ ( df_analysis[util.YEAR] == year ) & ( df_analysis[util.TOWN_NAME] == town ) & ( df_analysis[util.SECTOR] != util.SECTOR_TOTAL ) ]

    if len( df_findings ):

        df_totals = df_analysis[ ( df_analysis[util.YEAR] == year ) & ( df_analysis[util.TOWN_NAME] == town ) & ( df_analysis[util.SECTOR] == util.SECTOR_TOTAL ) ]

        if len( df_totals ):
            # Load new totals into existing row provided by Mass Save

            index = df_totals.index.values[0]

            for column in df_totals.columns[FIRST_NUMERIC_COLUMN:]:
                df_analysis.at[index, column] = df_findings[column].sum()

        else:
            # Mass Save did not provide a row of totals

            # Create missing row
            df_totals = df_findings.copy().reset_index( drop=True )
            df_totals = df_totals.loc[[df_totals.index[0]]]
            df_totals[util.SECTOR] = util.SECTOR_TOTAL

            # Load totals into new row
            for column in df_totals.columns[FIRST_NUMERIC_COLUMN:]:
                df_totals[column] = df_findings[column].sum()

            # Save new row to be incorporated into analysis
            df_analysis_totals = pd.concat( [df_analysis_totals, df_totals], ignore_index=True ).reset_index( drop=True )



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
        sector = util.SECTOR_RES_AND_LOW
        usage_mwh, usage_therms = get_usage_values( df_group, sector )

        if usage_mwh is not None and usage_therms is not None:

            # Calculate EES
            r1_ees = ( 1 - eb_factor ) * usage_mwh * surcharge_row[util.RESIDENTIAL_DOL_PER_MWH].values[0]
            r2_ees = eb_factor * usage_mwh * surcharge_row[util.DISCOUNT_DOL_PER_MWH].values[0]
            electric_ees = int( r1_ees + r2_ees )
            gas_ees = int( usage_therms * surcharge_row[util.RESIDENTIAL_DOL_PER_THERM].values[0] )

            # Report findings
            report_findings( year, town, sector, electric_ees, gas_ees )

        # Commercial
        sector = util.SECTOR_COM_AND_IND
        usage_mwh, usage_therms = get_usage_values( df_group, sector )

        if usage_mwh is not None and usage_therms is not None:

            # Calculate EES
            electric_ees = int( usage_mwh * surcharge_row[util.COMMERCIAL_DOL_PER_MWH].values[0] )
            gas_ees = int( usage_therms * surcharge_row[util.COMMERCIAL_DOL_PER_THERM].values[0] )

            # Report findings
            report_findings( year, town, sector, electric_ees, gas_ees )

        # Calculate totals of Residential and Commercial values
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
    df_analysis[ELECTRIC_EES] = 0
    df_analysis[GAS_EES] = 0
    df_analysis[ELECTRIC_EES_MINUS_INCENTIVES] = 0
    df_analysis[GAS_EES_MINUS_INCENTIVES] = 0
    df_analysis[util.INCENTIVES_PER_SAVED_MWH] = 0.0
    df_analysis[util.INCENTIVES_PER_SAVED_THERM] = 0.0
    df_analysis[util.COMBINED_EES_IN] = 0
    df_analysis[util.COMBINED_INCENTIVES_OUT] = 0
    df_analysis[util.COMBINED_EES_MINUS_INCENTIVES] = 0

    # Create dataframe for 'Total' rows that were missing from original data
    df_analysis_totals = pd.DataFrame( columns=df_analysis.columns )

    # Analyze the towns
    for index, row in df_towns.iterrows():
        analyze_town( row )

    # Insert missing Total rows into analysis dataframe
    df_analysis = pd.concat( [df_analysis, df_analysis_totals], ignore_index=True ).reset_index( drop=True )

    # Sort analysis dataframe
    df_analysis[util.SECTOR] = pd.Categorical( df_analysis[util.SECTOR], [util.SECTOR_RES_AND_LOW, util.SECTOR_COM_AND_IND, util.SECTOR_TOTAL] )
    df_analysis = df_analysis.sort_values( by=[util.YEAR, util.TOWN_NAME, util.SECTOR] ).reset_index( drop=True )

    # Set int type on numeric columns in analysis dataframe
    float_columns = \
    {
        util.INCENTIVES_PER_SAVED_MWH: 2,
        util.INCENTIVES_PER_SAVED_THERM: 2,
    }

    for column in df_analysis.columns[FIRST_NUMERIC_COLUMN:]:
        if column in float_columns:
            df_analysis[column] = df_analysis[column].round( float_columns[column] )
        else:
            df_analysis[column] = df_analysis[column].astype(int)

    # Save analysis results to database
    util.create_table( "Analysis", conn, cur, df=df_analysis )

    # Report elapsed time
    util.report_elapsed_time()
