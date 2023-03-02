# Copyright 2023 Energize Andover.  All rights reserved.

import argparse

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import datetime
THIS_YEAR = datetime.date.today().year

import sys
sys.path.append('../util')
import util
import vision
import normalize


ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO


# Column labels
VSID = util.VISION_ID
ACCT = util.ACCOUNT_NUMBER
MBLU = util.MBLU
LOCN = util.LOCATION
OWN1 = util.OWNER_1_NAME
OWN2 = util.OWNER_2_NAME
OADR = util.OWNER_ADDRESS
OZIP = util.OWNER_ZIP
ASMT = util.TOTAL_ASSESSED_VALUE
STYL = util.STYLE
OCCU = util.OCCUPANCY_HOUSEHOLDS
HEAT = util.HEATING_TYPE + util._DESC
FUEL = util.HEATING_FUEL + util._DESC
AIRC = util.AC_TYPE + util._DESC
HTAC = util.HEAT_AC
FLR1 = util.FIRST_FLOOR_USE
RESU = util.RESIDENTIAL_UNITS
KTCH = util.KITCHENS
BATH = util.BATHS
LAND = util.LAND_USE_CODE
DESC = util.LAND_USE_CODE + util._DESC
ACRE = util.TOTAL_ACRES
SLPR = util.SALE_PRICE
SLDT = util.SALE_DATE
ZONE = util.ZONE
YEAR = util.YEAR_BUILT
AREA = util.LIVING_AREA
BLDS = util.BUILDING_COUNT
TOT_OCCU = util.TOTAL_OCCUPANCY
TOT_BATH = util.TOTAL_BATHS
TOT_KTCH = util.TOTAL_KITCHENS
TOT_AREA = util.TOTAL_AREA
ISRS = util.IS_RESIDENTIAL
OLCL = util.OWNER_IS_LOCAL


# Calculate building age
def calculate_age( year_built ):
    age = ( THIS_YEAR - year_built ) if year_built else -1
    return age


# Encode hyperlink destined for Excel spreadsheet
def make_vision_link( town_name, row ):
    url = vision.URL_BASE.format( town_name ) + str( row[VSID] )
    hyperlink_code = '=HYPERLINK("%s", "%s")' % ( url, row[MBLU] )
    return hyperlink_code


######################

# Main program
if __name__ == '__main__':

    # Retrieve arguments
    parser = argparse.ArgumentParser( description='Clean parcel assessment data scraped from Vision Government Solutions website' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-l', dest='luc_filename',  help='Land use codes spreadsheet filename', required=True )
    parser.add_argument( '-i', dest='input_table_name',  help='Name of source table', required=True )
    parser.add_argument( '-o', dest='output_table_name',  help='Name of destination table', required=True )
    parser.add_argument( '-t', dest='town_name',  help='Name of town', required=True )
    parser.add_argument( '-n', dest='normalize', action='store_true', help='Normalize addresses?' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read raw table from database
    df = pd.read_sql_table( args.input_table_name, engine, index_col=util.ID, parse_dates=True )

    # Clean up data
    df[ACCT] = vision.clean_string( df[ACCT] )
    df[MBLU] = vision.clean_string( df[MBLU], remove_all_spaces=True )
    df[LOCN] = vision.clean_string( df[LOCN] )
    df[OWN1] = vision.clean_string( df[OWN1] )
    df[OWN2] = vision.clean_string( df[OWN2] )
    df[OADR] = vision.clean_string( df[OADR] )
    df[ASMT] = vision.clean_integer( df[ASMT] )
    df[STYL] = vision.clean_string( df[STYL] )
    df[OCCU] = vision.clean_integer( df[OCCU] )
    df[HEAT] = vision.clean_string( df[HEAT] )
    df[FUEL] = vision.clean_string( df[FUEL] )
    df[AIRC] = vision.clean_string( df[AIRC] )
    df[HTAC] = vision.clean_string( df[HTAC] )
    df[FLR1] = vision.clean_string( df[FLR1] )
    df[RESU] = vision.clean_integer( df[RESU] )
    df[KTCH] = vision.clean_integer( df[KTCH] )
    df[BATH] = vision.clean_integer( df[BATH] )
    df[LAND] = vision.clean_string( df[LAND] )
    df[DESC] = vision.clean_string( df[DESC] )
    df[ACRE] = vision.clean_float( df[ACRE] )
    df[SLPR] = vision.clean_integer( df[SLPR] )
    df[SLDT] = vision.clean_date( df[SLDT] )
    df[ZONE] = vision.clean_string( df[ZONE] )
    df[YEAR] = vision.clean_integer( df[YEAR] )
    df[AREA] = vision.clean_integer( df[AREA] )
    df[BLDS] = vision.clean_integer( df[BLDS] )
    df[TOT_OCCU] = vision.clean_integer( df[TOT_OCCU] )
    df[TOT_BATH] = vision.clean_integer( df[TOT_BATH] )
    df[TOT_KTCH] = vision.clean_integer( df[TOT_KTCH] )
    df[TOT_AREA] = vision.clean_integer( df[TOT_AREA] )

    # Add residential flag column
    ls_res_codes = util.get_residential_land_use_codes( args.luc_filename)
    df[LAND] = util.clean_residential_land_use_codes( df[LAND] )
    df[ISRS] = util.NO
    df.loc[df[LAND].isin( ls_res_codes ), ISRS] = util.YES

    # Add local-owner flag column
    df[OLCL] = util.NO
    df.loc[df[OADR].str.contains( ', *' + args.town_name + ' *,', case=False ), OLCL] = util.YES

    # If we got account numbers or Mblu values on every row, ensure that they are unique
    if len( df[ACCT][df[ACCT] != ''] ) == len( df ):
        df = df.drop_duplicates( subset=[ACCT], keep='last' )
    elif len( df[MBLU][df[MBLU] != ''] ) == len( df ):
        df = df.drop_duplicates( subset=[MBLU], keep='last' )
    # else don't drop anything; we already dropped on VISION ID

    # Calculate age
    df[util.AGE] = df[YEAR].apply( lambda year_built: calculate_age( year_built ) )

    # Optionally normalize addresses.  Use result_type='expand' to load multiple columns!
    if args.normalize:
        df[ADDR] = df[LOCN]
        df[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

    # Encode Vision URL as Excel hyperlink
    df[util.VISION_LINK] = df.apply( lambda row: make_vision_link( args.town_name, row ), axis=1 )

    # Preserve current progress in database
    util.create_table( args.output_table_name, conn, cur, df=df, alt_column_order='Vision_Clean' )

    # Report elapsed time
    util.report_elapsed_time()
