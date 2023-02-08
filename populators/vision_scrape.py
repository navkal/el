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

BUILDING_TABLE_ID_FORMAT = 'MainContent_ctl{:02d}_grdCns'
BUILDING_AREA_ID_FORMAT = 'MainContent_ctl{:02d}_lblBldArea'
BUILDING_YEAR_ID_FORMAT = 'MainContent_ctl{:02d}_lblYearBuilt'
BUILDING_TABLE_ID = 'building_id'
BUILDING_AREA_ID = 'area_id'
BUILDING_YEAR_ID = 'year_id'

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


def save_continue_at():

    # If not running in refresh mode, update continue-at table
    if not args.refresh:
        printctl.off()
        util.create_table( CONTINUE_AT_TABLE, conn, cur, df=pd.DataFrame( data={ VSID: [vision_id] } ) )
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


# Find HTML IDs associated with building tables, areas, and years
def find_all_building_ids( soup, building_count ):

    ls_building_ids = []
    first_building_id = ''
    first_area_id = ''
    first_year_id = ''

    # Set range limit for the search
    try:
        range_max = min( 5 + ( 3 * int( building_count ) ), 100 )
    except:
        range_max = 100

    # Search for HTML IDs, using 2-digit integers from 01 to range max
    for n_index in range( 1, range_max ):

        # Initialize dictionary of building and area IDs
        dc_ids = { BUILDING_TABLE_ID: '', BUILDING_AREA_ID: '' }

        # If page contains building table with current index, save the ID
        building_id = BUILDING_TABLE_ID_FORMAT.format( n_index )
        if soup.find( 'table', id=building_id ):
            dc_ids[BUILDING_TABLE_ID] = building_id
            if first_building_id == '':
                first_building_id = building_id

        # If page contains building area with current index, save the ID
        area_id = BUILDING_AREA_ID_FORMAT.format( n_index )
        if soup.find( 'span', id=area_id ):
            dc_ids[BUILDING_AREA_ID] = area_id
            if first_area_id == '':
                first_area_id = area_id

        # If page contains year built with current index, save the ID
        year_id = BUILDING_YEAR_ID_FORMAT.format( n_index )
        if soup.find( 'span', id=year_id ):
            if first_year_id == '':
                first_year_id = year_id

        # If we got anything in the dictionary, append to the list
        if dc_ids[BUILDING_TABLE_ID] or dc_ids[BUILDING_AREA_ID]:
            ls_building_ids.append( dc_ids )

    return ls_building_ids, first_building_id, first_area_id, first_year_id


# Scrape HTML element by id
def scrape_element( soup, tag, id ):
    element = soup.find( tag, id=id )
    text = element.string if element else ''
    return text


# Scrape cell of Building Attributes table, identified by label regex
def scrape_building_attribute( soup, building_id, re_label, is_numeric=False ):

    b_found = False
    s_attribute = ''

    table = soup.find( 'table', id=building_id )

    if table:
        for tr in table:
            if not tr.string:
                for td in tr:
                    if b_found:
                        s_attribute = str( td.string ).strip()
                    b_found = re.match( re_label, td.string )

    if is_numeric:

        # Replace letter O with zero (e.g. 'O1' - thanks, Tewksbury)
        s_attribute = s_attribute.replace( 'O', '0' )

        # Strip trailing non-numeric text (e.g. '2 Full' - thanks, Quincy)
        s_attribute = re.sub( r'(^\d+)(.*)', r'\1', s_attribute )

    return s_attribute


# Scrape and summarize building values
def scrape_buildings( soup, ls_building_ids, sr_row ):

    # Initialize tallies
    n_occu = 0
    n_bath = 0
    n_ktch = 0
    n_area = 0

    for dc_ids in ls_building_ids:

        building_id = dc_ids[BUILDING_TABLE_ID]
        area_id = dc_ids[BUILDING_AREA_ID]

        # Attempt to extract field value and add to total
        scr_occu = scrape_building_attribute( soup, building_id, RE_OCCU, is_numeric=True )
        if len( scr_occu ):
            n_occu += int( float( scr_occu ) )

        # Attempt to extract field value and add to total
        scr_bath = scrape_building_attribute( soup, building_id, RE_BATH, is_numeric=True )
        if len( scr_bath ):
            n_bath += int( float( scr_bath ) )

        # Attempt to extract field value and add to total
        scr_ktch = scrape_building_attribute( soup, building_id, RE_KTCH, is_numeric=True )
        if len( scr_ktch ):
            n_ktch += int( float( scr_ktch ) )

        # Attempt to extract field value and add to total
        scr_area = scrape_element( soup, 'span', area_id )
        if scr_area:
            s = str( scr_area.string.strip().replace( ',', '' ) )
            if len( s ):
                n_area += int( float( s ) )

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

    # Open the database
    municipality = args.municipality.lower()
    db_filename = '../db/vision_{}.sqlite'.format( municipality )
    conn, cur, engine = util.open_database( db_filename, args.create )

    # Read pre-existing table from database
    parcels_table_name = 'Vision_Raw_' + municipality.capitalize()
    try:
        df = pd.read_sql_table( parcels_table_name, engine, index_col=util.ID, parse_dates=True )
    except:
        df = pd.DataFrame( columns=COLS )

    # Read continuation table from database
    try:
        df_continue = pd.read_sql_table( CONTINUE_AT_TABLE, engine, index_col=util.ID, parse_dates=True )
    except:
        df_continue = pd.DataFrame()

    # Determine lower and upper bounds of Vision IDs to request
    if args.vision_id_range:
        # Supplied discovery range
        vision_id_range = args.vision_id_range.split( ',' )
    else:
        # Default discovery range
        vision_id_range = [1,200000]
    vision_id_range = [int(s) for s in vision_id_range]
    vision_id_range[1] += 1

    # Determine range of Vision IDs to request
    if args.refresh:
        # Refresh mode - update existing records, optionally restricted by specified vision ID bounds
        id_range = df[VSID].to_list()
        if args.vision_id_range:
            id_range = [ n for n in id_range if ( ( n >= vision_id_range[0] ) and ( n < vision_id_range[1] ) ) ]
    else:
        # Discover by consecutive integers
        if args.create:
            # Create mode - start at lower bound
            range_min = vision_id_range[0]
        elif len( df_continue ):
            # Continue mode - start at saved value
            range_min = df_continue[VSID].iloc[0]
        else:
            # Continue mode - start value not available, use lower bound
            range_min = vision_id_range[0]
        id_range = range( range_min, vision_id_range[1] )

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

            # Parse the HTML
            soup = BeautifulSoup( rsp.text, 'html.parser' )

            # Find IDs of all building tables and areas
            building_count = scrape_element( soup, 'span', 'MainContent_lblBldCount' )
            ls_building_ids, first_building_id, first_area_id, first_year_id = find_all_building_ids( soup, building_count )

            # Initialize new dataframe row
            sr_row = pd.Series( index=COLS )
            sr_row[VSID] = vision_id

            # Extract values
            sr_row[ACCT] = scrape_element( soup, 'span', 'MainContent_lblAcctNum' )
            sr_row[MBLU] = scrape_element( soup, 'span', 'MainContent_lblMblu' )
            sr_row[LOCN] = scrape_element( soup, 'span', 'MainContent_lblTab1Title' )
            sr_row[OWN1] = scrape_element( soup, 'span', 'MainContent_lblOwner' )
            sr_row[OWN2] = scrape_element( soup, 'span', 'MainContent_lblCoOwner' )
            sr_row[ASMT] = scrape_element( soup, 'span', 'MainContent_lblGenAssessment' )
            sr_row[STYL] = scrape_building_attribute( soup, first_building_id, r'Style\:?' )
            sr_row[OCCU] = scrape_building_attribute( soup, first_building_id, RE_OCCU, is_numeric=True )
            sr_row[HEAT] = scrape_building_attribute( soup, first_building_id, r'Heat(ing)? Type\:?' )
            sr_row[FUEL] = scrape_building_attribute( soup, first_building_id, r'Heat(ing)? Fuel\:?' )
            sr_row[AIRC] = scrape_building_attribute( soup, first_building_id, r'AC Type\:?' )
            sr_row[HTAC] = scrape_building_attribute( soup, first_building_id, r'Heat\/AC\:?' )
            sr_row[FLR1] = scrape_building_attribute( soup, first_building_id, r'1st Floor Use\:?' )
            sr_row[RESU] = scrape_building_attribute( soup, first_building_id, r'Residential Units\:?' )
            sr_row[KTCH] = scrape_building_attribute( soup, first_building_id, RE_KTCH, is_numeric=True )
            sr_row[BATH] = scrape_building_attribute( soup, first_building_id, RE_BATH, is_numeric=True )
            sr_row[LAND] = scrape_element( soup, 'span', 'MainContent_lblUseCode' )
            sr_row[DESC] = scrape_element( soup, 'span', 'MainContent_lblUseCodeDescription' )
            sr_row[ACRE] = scrape_element( soup, 'span', 'MainContent_lblLndAcres' )
            sr_row[SLPR] = scrape_element( soup, 'span', 'MainContent_lblPrice' )
            sr_row[SLDT] = scrape_element( soup, 'span', 'MainContent_lblSaleDate' )
            sr_row[ZONE] = scrape_element( soup, 'span', 'MainContent_lblZone' )
            sr_row[YEAR] = scrape_element( soup, 'span', first_year_id )
            sr_row[AREA] = scrape_element( soup, 'span', first_area_id )
            sr_row[BLDS] = building_count

            # Extract summary building values
            sr_row = scrape_buildings( soup, ls_building_ids, sr_row )

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

    save_and_exit( None, None )
