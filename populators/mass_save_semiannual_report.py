# Copyright 2025 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util

INCOME_LOW = 'Low-Income'
INCOME_MOD = 'Moderate Income'
INCOME_NON = 'Not LMI-Qualified'

LMI_ = 'lmi_'
LMI_PROJECT_COUNT = LMI_ + util.PROJECT_COUNT
LMI_HP_COUNT = LMI_ + util.HP_COUNT
LMI_WX_COUNT = LMI_ + util.WX_COUNT
_INCENTIVES = '_' + util.INCENTIVES
LMI_HP_INCENTIVES = LMI_ + util.HP + _INCENTIVES
LMI_WX_INCENTIVES = LMI_ + util.WX + _INCENTIVES
LMI_TOTAL_INCENTIVES = LMI_ + util.TOTAL + _INCENTIVES

NON_ = 'non_lmi_'
NON_PROJECT_COUNT = NON_ + util.PROJECT_COUNT
NON_HP_COUNT = NON_ + util.HP_COUNT
NON_WX_COUNT = NON_ + util.WX_COUNT
_INCENTIVES = '_' + util.INCENTIVES
NON_HP_INCENTIVES = NON_ + util.HP + _INCENTIVES
NON_WX_INCENTIVES = NON_ + util.WX + _INCENTIVES
NON_TOTAL_INCENTIVES = NON_ + util.TOTAL + _INCENTIVES

EQUITY_ZIP_ = 'equity_zip_'
EQUITY_ZIP_HOUSEHOLDS = EQUITY_ZIP_ + util.HOUSEHOLDS
EQUITY_ZIP_POPULATION = EQUITY_ZIP_ + util.POPULATION

REPORT_COLUMNS = \
[
    util.TOWN_NAME,
    util.YEAR,
    util.QUARTER,
    LMI_PROJECT_COUNT,
    LMI_TOTAL_INCENTIVES,
    LMI_HP_COUNT,
    LMI_WX_COUNT,
    LMI_HP_INCENTIVES,
    LMI_WX_INCENTIVES,
    NON_PROJECT_COUNT,
    NON_TOTAL_INCENTIVES,
    NON_HP_COUNT,
    NON_WX_COUNT,
    NON_HP_INCENTIVES,
    NON_WX_INCENTIVES,
    EQUITY_ZIP_HOUSEHOLDS,
    EQUITY_ZIP_POPULATION,
]

#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Create table of MA towns' )
    parser.add_argument( '-d', dest='db_filename',  help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read tables from database
    df_raw = pd.read_sql_table( 'RawSemiannualReport', engine, index_col=util.ID )
    df_zip = pd.read_sql_table( 'EquityZipCodes', engine, index_col=util.ID )

    # Merge raw report with zip code statistics
    df_raw = pd.merge( df_raw, df_zip, how='inner', on=util.ZIP )

    # Initialize empty report
    df_report = pd.DataFrame( columns=REPORT_COLUMNS )

    # Generate the report
    for ( s_town, s_year, s_quarter ), df_town in df_raw.groupby( by=[util.TOWN_NAME, util.YEAR, util.QUARTER] ):

        # Initialize report row for current town
        dc_row = { util.TOWN_NAME: s_town, util.YEAR: s_year, util.QUARTER: s_quarter.upper() }

        # Count households and population of the town's equity zip codes
        dc_row[EQUITY_ZIP_HOUSEHOLDS] = df_town[util.HOUSEHOLDS].sum()
        dc_row[EQUITY_ZIP_POPULATION] = df_town[util.POPULATION].sum()

        # Isolate LMI rows
        df_lmi = df_town[df_town[util.INCOME_CATEGORY].isin( [INCOME_LOW, INCOME_MOD] )]

        # Partition by project type
        df_hp = df_lmi[df_lmi[util.MEASURE_TYPE].str.endswith( ' HP' )]
        df_wx = df_lmi[df_lmi[util.MEASURE_TYPE] == 'Weatherization']

        # Count heat pump and weatherization projects
        dc_row[LMI_HP_COUNT] = df_hp[util.HOMES].sum()
        dc_row[LMI_WX_COUNT] = df_wx[util.HOMES].sum()

        # Tally LMI incentive spending
        dc_row[LMI_HP_INCENTIVES] = df_hp[util.INCENTIVES].sum()
        dc_row[LMI_WX_INCENTIVES] = df_wx[util.INCENTIVES].sum()

        # Isolate non-LMI rows
        df_non = df_town[df_town[util.INCOME_CATEGORY] == INCOME_NON]

        # Partition by project type
        df_hp = df_non[df_non[util.MEASURE_TYPE].str.endswith( ' HP' )]
        df_wx = df_non[df_non[util.MEASURE_TYPE] == 'Weatherization']

        # Count heat pump and weatherization projects
        dc_row[NON_HP_COUNT] = df_hp[util.HOMES].sum()
        dc_row[NON_WX_COUNT] = df_wx[util.HOMES].sum()

        # Tally LMI incentive spending
        dc_row[NON_HP_INCENTIVES] = df_hp[util.INCENTIVES].sum()
        dc_row[NON_WX_INCENTIVES] = df_wx[util.INCENTIVES].sum()

        # Add row to dataframe
        df_row = pd.DataFrame( [dc_row] )
        df_report = pd.concat( [df_report, df_row] )

    # Finish LMI calculations
    df_report[LMI_PROJECT_COUNT] = df_report[LMI_WX_COUNT] + df_report[LMI_HP_COUNT]
    df_report[LMI_HP_INCENTIVES] = df_report[LMI_HP_INCENTIVES].round( 2 )
    df_report[LMI_WX_INCENTIVES] = df_report[LMI_WX_INCENTIVES].round( 2 )
    df_report[LMI_TOTAL_INCENTIVES] = df_report[LMI_HP_INCENTIVES] + df_report[LMI_WX_INCENTIVES]

    # Finish non-LMI calculations
    df_report[NON_PROJECT_COUNT] = df_report[NON_WX_COUNT] + df_report[NON_HP_COUNT]
    df_report[NON_HP_INCENTIVES] = df_report[NON_HP_INCENTIVES].round( 2 )
    df_report[NON_WX_INCENTIVES] = df_report[NON_WX_INCENTIVES].round( 2 )
    df_report[NON_TOTAL_INCENTIVES] = df_report[NON_HP_INCENTIVES] + df_report[NON_WX_INCENTIVES]

    # Sort
    df_report[util.YEAR] = df_report[util.YEAR].astype( int )
    df_report = df_report.sort_values( by=[util.TOWN_NAME, util.YEAR, util.QUARTER] )

    # Save result to database
    util.create_table( 'SemiannualReport', conn, cur, df=df_report )

    # Report elapsed time
    util.report_elapsed_time()
