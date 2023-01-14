# Copyright 2023 Energize Andover.  All rights reserved.

import argparse
import requests
from bs4 import BeautifulSoup

import warnings
warnings.filterwarnings( 'ignore' )

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import numpy as np
import re
import signal

import datetime
THIS_YEAR = datetime.date.today().year

import sys
sys.path.append('../util')
import util
import normalize


URL_BASE = 'https://gis.vgsi.com/lawrencema/parcel.aspx?pid='

ID_RANGE_MIN = 266
ID_RANGE_MAX = 105888
ID_RANGE_STOP = ID_RANGE_MAX + 1

PARCELS_TABLE = 'Parcels_L'

ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
OCCUPANCY = util.NORMALIZED_OCCUPANCY
ADDITIONAL = util.NORMALIZED_ADDITIONAL_INFO

RE_OCCU = r'Occupancy\:?'
RE_BATH = r'Total Ba?thr?m?s\:?'
RE_KTCH = r'Num Kitchens\:?'

# Column labels
VSID = util.VISION_ID
ACCT = util.ACCOUNT_NUMBER
LOCN = util.LOCATION
OWN1 = util.OWNER_1_NAME
OWN2 = util.OWNER_2_NAME
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
ISRS = util.IS_RESIDENTIAL


# Column order
COLS = \
[
    VSID,
    ACCT,
    LOCN,
    OWN1,
    OWN2,
    ASMT,
    STYL,
    OCCU,
    HEAT,
    FUEL,
    AIRC,
    HTAC,
    FLR1,
    RESU,
    KTCH,
    BATH,
    LAND,
    DESC,
    ACRE,
    SLPR,
    SLDT,
    ZONE,
    YEAR,
    AREA,
    BLDS,
    TOT_OCCU,
    TOT_BATH,
    TOT_KTCH,
    ISRS,
]

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
    sr_summary = pd.Series()
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
        df_summary[col_name] = clean_integer( df_summary[col_name] )

    return df_summary


def summarize_and_exit( df ):

    print( '' )

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
    util.create_table( 'ParcelSummary', conn, cur, df=df_summary )

    # Report elapsed time
    util.report_elapsed_time()
    sys.exit()


def clean_string( col ):
    col = col.str.strip().replace( r'\s+', ' ', regex=True )
    return col

def clean_integer( col ):
    col = col.replace( '[\$,]', '', regex=True )
    col = col.replace( r'^\s*$', np.nan, regex=True )
    col = col.fillna( '0' ).astype( float ).astype( int )
    return col

def clean_date( col ):
    col = pd.to_datetime( col, infer_datetime_format=True, errors='coerce' )
    return col

def calculate_age( row ):
    year_built = row[YEAR]
    age = ( THIS_YEAR - year_built ) if year_built else -1
    return age

b_save_and_exit_done = False
def save_and_exit( signum, frame ):

    # Suppress repeat execution of handler
    global b_save_and_exit_done
    if b_save_and_exit_done:
        sys.exit()
    else:
        b_save_and_exit_done = True

    global df

    # Report current status
    if not args.post_process:
        print( '' )
        print( 'Stopping at VISION ID {}'.format( vision_id ) )

    if len( df ):

        # Reorganize rows
        df[VSID] = df[VSID].astype( int )
        df = df.drop_duplicates( subset=[VSID], keep='last' )
        df = df.sort_values( by=[VSID] )

        # Clean up data
        df[ACCT] = clean_string( df[ACCT] )
        df[LOCN] = clean_string( df[LOCN] )
        df[OWN1] = clean_string( df[OWN1] )
        df[OWN2] = clean_string( df[OWN2] )
        df[ASMT] = clean_integer( df[ASMT] )
        df[STYL] = clean_string( df[STYL] )
        df[OCCU] = clean_integer( df[OCCU] )
        df[HEAT] = clean_string( df[HEAT] )
        df[FUEL] = clean_string( df[FUEL] )
        df[AIRC] = clean_string( df[AIRC] )
        df[HTAC] = clean_string( df[HTAC] )
        df[FLR1] = clean_string( df[FLR1] )
        df[RESU] = clean_integer( df[RESU] )
        df[KTCH] = clean_integer( df[KTCH] )
        df[BATH] = clean_integer( df[BATH] )
        df[LAND] = clean_string( df[LAND] )
        df[DESC] = clean_string( df[DESC] )
        df[ACRE] = df[ACRE].astype( float )
        df[SLPR] = clean_integer( df[SLPR] )
        df[SLDT] = clean_date( df[SLDT] )
        df[ZONE] = clean_string( df[ZONE] )
        df[YEAR] = clean_integer( df[YEAR] )
        df[AREA] = clean_integer( df[AREA] )
        df[BLDS] = clean_integer( df[BLDS] )
        df[TOT_OCCU] = clean_integer( df[TOT_OCCU] )
        df[TOT_BATH] = clean_integer( df[TOT_BATH] )
        df[TOT_KTCH] = clean_integer( df[TOT_KTCH] )

        # Calculate age
        df[util.AGE] = df.apply( lambda row: calculate_age( row ), axis=1 )

        # Normalize addresses.  Use result_type='expand' to load multiple columns!
        df[ADDR] = df[LOCN]
        df[[ADDR,STREET_NUMBER,STREET_NAME,OCCUPANCY,ADDITIONAL]] = df.apply( lambda row: normalize.normalize_address( row, ADDR, city='LAWRENCE', return_parts=True ), axis=1, result_type='expand' )

        # Report size of output
        print( '' )
        print( 'Saving {} VISION IDs'.format( len( df ) ) )

        # Preserve current progress in database
        util.create_table( PARCELS_TABLE, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
    sys.exit()


# Scrape HTML element by id
def scrape_element( soup, tag, id ):
    element = soup.find( tag, id=id )
    text = element.string if element else ''
    return text


# Scrape cell of Building Attributes table, identified by label regex
def scrape_building_attribute( soup, re_label, building=1 ):

    b_found = False
    s_attribute = ''

    table = soup.find( 'table', id='MainContent_ctl{:02d}_grdCns'.format( building ) )

    if table:
        for tr in table:
            if not tr.string:
                for td in tr:
                    if b_found:
                        s_attribute = td.string
                    b_found = re.match( re_label, td.string )

    return s_attribute


# Scrape and summarize building values
def scrape_buildings( soup, sr_row ):

    # Initialize tallies
    n_occu = 0
    n_bath = 0
    n_ktch = 0

    # Initialize counter
    n_building = 0

    while True:

        # Increment counter
        n_building += 1

        # Attempt to extract field value and add to total
        scr_occu = scrape_building_attribute( soup, RE_OCCU, building=n_building )
        if scr_occu:
            s = str( scr_occu.string.strip() )
            if len( s ):
                n_occu += int( float( s ) )

        # Attempt to extract field value and add to total
        scr_bath = scrape_building_attribute( soup, RE_BATH, building=n_building )
        if scr_bath:
            s = str( scr_bath.string.strip() )
            if len( s ):
                n_bath += int( float( s ) )

        # Attempt to extract field value and add to total
        scr_ktch = scrape_building_attribute( soup, RE_KTCH, building=n_building )
        if scr_ktch:
            s = str( scr_ktch.string.strip() )
            if len( s ):
                n_ktch += int( float( s ) )

        # If we didn't get any field values, quit
        if not ( scr_occu or scr_bath or scr_ktch ):
            break

    # Insert totals in row
    sr_row[TOT_OCCU] = n_occu
    sr_row[TOT_BATH] = n_bath
    sr_row[TOT_KTCH] = n_ktch

    return sr_row


######################

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Scrape parcel assessment data from Vision Government Solutions website' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-l', dest='luc_filename',  help='Land use codes spreadsheet filename' )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    parser.add_argument( '-r', dest='refresh', action='store_true', help='Refresh records in existing database?' )
    parser.add_argument( '-p', dest='post_process', action='store_true', help='Post-process only?' )
    parser.add_argument( '-s', dest='summarize', action='store_true', help='Summarize only?' )
    args = parser.parse_args()

    # Flag precedence for refresh
    if args.refresh:
        args.create = False

    # Flag precedence for post-process
    if args.post_process:
        args.create = False
        args.refresh = True

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, args.create )

    # Read pre-existing table from database
    try:
        df = pd.read_sql_table( PARCELS_TABLE, engine, index_col=util.ID, parse_dates=True )
        df[SLDT] = pd.to_datetime( df[SLDT] ).dt.strftime( '%m/%d/%Y' )
    except:
        df = pd.DataFrame( columns=COLS )

    # Take early exit with summarize option
    if args.summarize:
        summarize_and_exit( df )

    # Determine range of Vision IDs to process
    if len( df ):
        # Table exists
        if args.refresh:
            # Refresh existing records
            id_range = df[VSID].to_list()
        else:
            # Discover remainder of range
            id_range = range( 1 + df[VSID].max(), ID_RANGE_STOP )
    else:
        # Table does not exist.  Discover full range.
        id_range = range( ID_RANGE_MIN, ID_RANGE_STOP )

    print( '' )
    s_doing_what =  '{} {}'.format( ( 'Post-processing' if args.post_process else 'Refreshing' ), len( id_range ) ) if ( len( df ) and args.refresh ) else 'Discovering'
    print( '{} VISION IDs in range {} to {}'.format( s_doing_what, id_range[0], id_range[-1] ) )
    print( '' )

    # Take early exit with post-process option
    if args.post_process:
        save_and_exit( None, None )
    else:
        signal.signal( signal.SIGINT, save_and_exit )

    # Retrieve residential codes
    df_res_codes = pd.read_excel( args.luc_filename, dtype=object )
    sr_res_codes = df_res_codes['Residential Land Use Code']
    sr_res_codes = sr_res_codes.astype(str).str.zfill( 4 )
    ls_res_codes = list( sr_res_codes )

    # Initialize counter
    n_processed = 0
    n_last_reported = -1

    for vision_id in id_range:

        if ( n_processed % 50 == 0 ) and ( n_processed != n_last_reported ):
            n_last_reported = n_processed
            util.report_elapsed_time( prefix='' )
            s_status = ' Processed {} ({}%) of {}, requesting VISION ID {}'.format( n_processed, round( 100 * n_processed / len( id_range ), 2 ), len( id_range ), vision_id )
            print( s_status )

        url = URL_BASE + str( vision_id )
        try:
            rsp = requests.get( url, verify=False )
        except:
            save_and_exit( None, None )

        if ( rsp.url == url ):

            # Initialize new dataframe row
            sr_row = pd.Series( index=COLS )
            sr_row[VSID] = vision_id

            # Extract values
            soup = BeautifulSoup( rsp.text, 'html.parser' )
            sr_row[ACCT] = scrape_element( soup, 'span', 'MainContent_lblAcctNum' )
            sr_row[LOCN] = scrape_element( soup, 'span', 'MainContent_lblTab1Title' )
            sr_row[OWN1] = scrape_element( soup, 'span', 'MainContent_lblOwner' )
            sr_row[OWN2] = scrape_element( soup, 'span', 'MainContent_lblCoOwner' )
            sr_row[ASMT] = scrape_element( soup, 'span', 'MainContent_lblGenAssessment' )
            sr_row[STYL] = scrape_building_attribute( soup, r'Style\:?' )
            sr_row[OCCU] = scrape_building_attribute( soup, RE_OCCU )
            sr_row[HEAT] = scrape_building_attribute( soup, r'Heat(ing)? Type\:?' )
            sr_row[FUEL] = scrape_building_attribute( soup, r'Heat(ing)? Fuel\:?' )
            sr_row[AIRC] = scrape_building_attribute( soup, r'AC Type\:?' )
            sr_row[HTAC] = scrape_building_attribute( soup, r'Heat\/AC\:?' )
            sr_row[FLR1] = scrape_building_attribute( soup, r'1st Floor Use\:?' )
            sr_row[RESU] = scrape_building_attribute( soup, r'Residential Units\:?' )
            sr_row[KTCH] = scrape_building_attribute( soup, RE_KTCH )
            sr_row[BATH] = scrape_building_attribute( soup, RE_BATH )
            sr_row[LAND] = scrape_element( soup, 'span', 'MainContent_lblUseCode' )
            sr_row[DESC] = scrape_element( soup, 'span', 'MainContent_lblUseCodeDescription' )
            sr_row[ACRE] = scrape_element( soup, 'span', 'MainContent_lblLndAcres' )
            sr_row[SLPR] = scrape_element( soup, 'span', 'MainContent_lblPrice' )
            sr_row[SLDT] = scrape_element( soup, 'span', 'MainContent_lblSaleDate' )
            sr_row[ZONE] = scrape_element( soup, 'span', 'MainContent_lblZone' )
            sr_row[YEAR] = scrape_element( soup, 'span', 'MainContent_ctl01_lblYearBuilt' )
            sr_row[AREA] = scrape_element( soup, 'span', 'MainContent_ctl01_lblBldArea' )
            sr_row[BLDS] = scrape_element( soup, 'span', 'MainContent_lblBldCount' )

            # Extract summary building values
            sr_row = scrape_buildings( soup, sr_row )

            # Set residential flag
            sr_row[ISRS] = util.YES if sr_row[LAND] in ls_res_codes else util.NO

            # Load new row into dataframe
            df = df.append( sr_row, ignore_index=True )

            # Increment count
            n_processed += 1

    save_and_exit( None, None )
