# Copyright 2023 Energize Andover.  All rights reserved.

import argparse
import requests
from bs4 import BeautifulSoup

import warnings
warnings.filterwarnings( 'ignore' )

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re
import signal

import sys
sys.path.append('../util')
import util
import printctl

SAVE_INTERVAL = 500

CONTINUE_AT_TABLE = '_ContinueAtVisionId'

URL_BASE = 'https://gis.vgsi.com/{}ma/parcel.aspx?pid='

RE_OCCU = r'Occupancy\:?'
RE_BATH = r'Total Ba?thr?m?s\:?'
RE_KTCH = r'Num Kitchens\:?'

# Column labels
VSID = util.VISION_ID
ACCT = util.ACCOUNT_NUMBER
MBLU = util.MBLU
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
TOT_AREA = util.TOTAL_AREA


# Column order
COLS = \
[
    VSID,
    ACCT,
    MBLU,
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
    TOT_AREA,
]


b_loop_ran_to_completion = False
def save_continue_at():

    # If not running in refresh mode, update continue-at table
    if not args.refresh:
        df_continue_at = pd.DataFrame() if b_loop_ran_to_completion else pd.DataFrame( data={ VSID: [vision_id] } )
        printctl.off()
        util.create_table( CONTINUE_AT_TABLE, conn, cur, df=df_continue_at )
        printctl.on()


def save_progress( df ):

    if len( df ):

        # Reorganize rows
        df[VSID] = df[VSID].astype( int )
        df = df.drop_duplicates( subset=[VSID], keep='last' )
        df = df.sort_values( by=[VSID] )

        # Report size of output
        print( '' )
        print( 'Saving {} VISION IDs'.format( len( df ) ) )

        # Preserve current progress in database
        util.create_table( parcels_table_name, conn, cur, df=df )
        print( '' )

        # Save vision ID at which to continue discovery
        save_continue_at()


b_save_and_exit_done = False
def save_and_exit( signum, frame ):

    # Suppress repeat execution of handler
    global b_save_and_exit_done
    if b_save_and_exit_done:
        sys.exit()
    else:
        b_save_and_exit_done = True

    # Report current status
    print( '' )
    print( 'Stopping at VISION ID {}'.format( vision_id ) )

    # Save what we have scraped
    save_progress( df )

    # Report elapsed time
    util.report_elapsed_time()
    sys.exit()


# Scrape HTML element by id
def scrape_element( soup, tag, id ):
    element = soup.find( tag, id=id )
    text = element.string if element else ''
    return text


# Scrape cell of Building Attributes table, identified by label regex
def scrape_building_attribute( soup, re_label, building=1, is_numeric=False ):

    b_found = False
    s_attribute = ''

    table = soup.find( 'table', id='MainContent_ctl{:02d}_grdCns'.format( building ) )

    if table:
        for tr in table:
            if not tr.string:
                for td in tr:
                    if b_found:
                        s_attribute = str( td.string ).strip()
                    b_found = re.match( re_label, td.string )

    # Optionally replace letter O with zero
    if is_numeric:
        s_attribute = s_attribute.replace( 'O', '0' )

    return s_attribute


# Scrape and summarize building values
def scrape_buildings( soup, sr_row ):

    # Initialize tallies
    n_occu = 0
    n_bath = 0
    n_ktch = 0
    n_area = 0

    # Initialize counter
    n_building = 0

    while True:

        # Increment counter
        n_building += 1

        # Attempt to extract field value and add to total
        scr_occu = scrape_building_attribute( soup, RE_OCCU, building=n_building, is_numeric=True )
        if len( scr_occu ):
            n_occu += int( float( scr_occu ) )

        # Attempt to extract field value and add to total
        scr_bath = scrape_building_attribute( soup, RE_BATH, building=n_building, is_numeric=True )
        if len( scr_bath ):
            n_bath += int( float( scr_bath ) )

        # Attempt to extract field value and add to total
        scr_ktch = scrape_building_attribute( soup, RE_KTCH, building=n_building, is_numeric=True )
        if len( scr_ktch ):
            n_ktch += int( float( scr_ktch ) )

        # Attempt to extract field value and add to total
        scr_area = scrape_element( soup, 'span', 'MainContent_ctl{:02d}_lblBldArea'.format( n_building ) )
        if scr_area:
            s = str( scr_area.string.strip().replace( ',', '' ) )
            if len( s ):
                n_area += int( float( s ) )

        # If we didn't get any field values, quit
        if not ( scr_occu or scr_bath or scr_ktch or scr_area ):
            break

    # Insert totals in row
    sr_row[TOT_OCCU] = n_occu
    sr_row[TOT_BATH] = n_bath
    sr_row[TOT_KTCH] = n_ktch
    sr_row[TOT_AREA] = n_area

    return sr_row


######################

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Scrape parcel assessment data from Vision Government Solutions website' )
    parser.add_argument( '-m', dest='municipality',  help='Name of municipality in MA', required=True )
    parser.add_argument( '-v', dest='vision_id_range',  help='Range of Vision IDs to request' )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    parser.add_argument( '-r', dest='refresh', action='store_true', help='Refresh records in existing database?' )
    args = parser.parse_args()

    # Refresh and range arguments
    if args.refresh:
        args.create = False
    else:
        if args.vision_id_range:
            vision_id_range = args.vision_id_range.split( ',' )
        else:
            # Default discovery range
            vision_id_range = [1,200000]
        vision_id_range = [int(s) for s in vision_id_range]
        vision_id_range[1] += 1

    # Open the database
    municipality = args.municipality.lower()
    db_filename = '../db/vision_{}.sqlite'.format( municipality )
    conn, cur, engine = util.open_database( db_filename, args.create )

    # Read pre-existing table from database
    parcels_table_name = 'Vision_' + municipality.capitalize()
    try:
        df = pd.read_sql_table( parcels_table_name, engine, index_col=util.ID, parse_dates=True )
    except:
        df = pd.DataFrame( columns=COLS )

    # Determine range of Vision IDs to process
    if len( df ):
        # Table exists
        if args.refresh:
            # Refresh existing records
            id_range = df[VSID].to_list()
        else:
            # Discover remainder of range
            try:
                df_continue = pd.read_sql_table( CONTINUE_AT_TABLE, engine, index_col=util.ID, parse_dates=True )
                range_min = df_continue[VSID].iloc[0]
            except:
                range_min = vision_id_range[1]
            id_range = range( range_min, vision_id_range[1] )
    else:
        # Table does not exist.  Discover full range.
        id_range = range( vision_id_range[0], vision_id_range[1] )

    print( '' )
    s_doing_what =  '{} {}'.format( 'Refreshing' , len( id_range ) ) if ( len( df ) and args.refresh ) else 'Discovering'
    if len( id_range ):
        print( '{} VISION IDs in range {} to {}'.format( s_doing_what, id_range[0], id_range[-1] ) )
    else:
        exit( 'No VISION IDs to process in {}'.format( id_range ) )
    print( '' )

    # Prepare URL base
    url_base = URL_BASE.format( municipality )

    # Initialize counter
    n_processed = 0 if args.refresh else len( df )
    n_last_reported = -1
    n_tried = 0
    n_saved = n_processed

    # Set condition handler
    signal.signal( signal.SIGINT, save_and_exit )

    for vision_id in id_range:

        if ( ( n_processed % 50 == 0 ) and ( n_processed != n_last_reported ) ) or ( n_tried % 100 == 0 ):
            n_last_reported = n_processed
            util.report_elapsed_time( prefix='' )
            s_status = ' Tried {} ({}%) and processed {} ({}%) of {}; requesting VISION ID {}'.format( n_tried, round( 100 * n_tried / len( id_range ), 2 ), n_processed, round( 100 * n_processed / len( id_range ), 2 ), len( id_range ), vision_id )
            print( s_status )

            # Save current vision ID at which to continue if this process is interrupted
            save_continue_at()

        url = url_base + str( vision_id )
        try:
            rsp = requests.get( url, verify=False )
        except Exception as e:
            print( '' )
            print( '==>' )
            print( '==> Exiting due to exception:' )
            print( '==> {}'.format( str( e ) ) )
            print( '==>' )
            save_and_exit( None, None )

        if ( rsp.url == url ):

            # Initialize new dataframe row
            sr_row = pd.Series( index=COLS )
            sr_row[VSID] = vision_id

            # Extract values
            soup = BeautifulSoup( rsp.text, 'html.parser' )
            sr_row[ACCT] = scrape_element( soup, 'span', 'MainContent_lblAcctNum' )
            sr_row[MBLU] = scrape_element( soup, 'span', 'MainContent_lblMblu' )
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

            # Load new row into dataframe
            df = df.append( sr_row, ignore_index=True )

            # Increment count
            n_processed += 1

            # Intermittently save what we have scraped
            if n_processed - n_saved >= SAVE_INTERVAL:
                save_progress( df )
                n_saved = n_processed if args.refresh else len( df )

        # Increment count
        n_tried += 1

    b_loop_ran_to_completion = True

    save_and_exit( None, None )
