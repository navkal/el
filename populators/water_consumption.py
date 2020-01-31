# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import time
import math

import sys
sys.path.append( '../util' )
import util

#------------------------------------------------------

def summarize( df ):

    t = time.time()
    print( '' )
    print( 'summarize() starting at', time.strftime( '%H:%M:%S', time.localtime( t ) ) )

    df_summary =  pd.DataFrame( columns=df.columns )

    for idx, df_group in df.groupby( by=[util.SERVICE_ID] ):

        # Copy dataframe and find last row
        df_meter = df_group.copy()
        row_n = df_meter.iloc[-1]

        # Remove data pertaining to old meters, if any
        df_meter = df_meter[ df_group[util.METER_NUMBER] == row_n[util.METER_NUMBER] ]

        # Determine whether meter readings look reasonable
        sr_cur = df_meter[util.CURRENT_READING].iloc[:-1]
        sr_pri = df_meter[util.PRIOR_READING].shift(-1).iloc[:-1]
        sr_eq = sr_cur == sr_pri
        readings_look_good = False not in sr_eq.value_counts()

        # Look for wraps
        if readings_look_good:
            sr_wrap = df_meter[util.CURRENT_READING] < df_meter[util.PRIOR_READING]
            vc_wrap = sr_wrap.value_counts()
            n_wraps = vc_wrap[True] if True in vc_wrap else 0
        else:
            n_wraps = 0

        # Calculate water usage hidden by wraps
        max_reading = df_meter[util.CURRENT_READING].max()
        wrap_extent = 10 ** ( 1 + int( math.log10( max_reading ) ) ) if ( max_reading > 0 ) else 0
        usage_hidden_by_wraps = wrap_extent * n_wraps

        # Summarize readings in one row
        row_0 = df_meter.iloc[0]
        row_n[util.PRIOR_DATE] = row_0[util.PRIOR_DATE]
        row_n[util.PRIOR_READING] = row_0[util.PRIOR_READING] - usage_hidden_by_wraps

        # Report meter status
        if readings_look_good:
            row_n[util.METER_STATUS] = util.METER_WRAP if n_wraps > 0 else util.METER_NORMAL
        else:
            row_n[util.METER_STATUS] = util.METER_ANOMALY

        # Save summary row
        df_summary = df_summary.append( row_n )

    util.report_elapsed_time( prefix='summarize() done: ', start_time=t )

    return df_summary


#------------------------------------------------------

if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Calculate per-period water consumption' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-s', dest='summary', action='store_true', help='Generate summary table?' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read water table from database
    df_water = pd.read_sql_table( 'Water', engine, parse_dates=[util.CURRENT_DATE, util.PRIOR_DATE] )

    # Isolate interesting rows and columns
    df_water = df_water[ df_water[util.SERVICE] == 'WATER' ]
    df_water = df_water.drop( columns=[util.ID, util.ACCOUNT_NUMBER, util.SERVICE, util.TRANSACTION_DATE, util.TRANSACTION_TYPE, util.UNITS] )
    df_water = df_water.dropna( subset=[ util.CURRENT_DATE, util.PRIOR_DATE ], how='any' )

    # Sort
    df_water = df_water.sort_values( by=[util.SERVICE_ID, util.CURRENT_DATE] )

    # Optionally remove detail
    if args.summary:
        df_water = summarize( df_water )

    # Calculate usage
    df_water[util.CU_FT] = ( df_water[util.CURRENT_READING] - df_water[util.PRIOR_READING] ).astype(int)
    df_water[util.ELAPSED_DAYS] = ( df_water[util.CURRENT_DATE] - df_water[util.PRIOR_DATE] ).astype( 'timedelta64[D]' ).astype(int)
    df_water = df_water[ df_water[util.ELAPSED_DAYS] > 0 ]
    df_water[util.CU_FT_PER_DAY] = round( df_water[util.CU_FT] / df_water[util.ELAPSED_DAYS], 1 )
    df_water[util.GAL_PER_DAY] = round( df_water[util.CU_FT_PER_DAY] * util.GAL_PER_CU_FT ).astype(int)

    # Save result to database
    util.create_table( ( 'WaterCustomers' if args.summary else 'WaterConsumption' ), conn, cur, df=df_water )

    # Report elapsed time
    util.report_elapsed_time()
