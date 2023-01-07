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

import sys
sys.path.append('../util')
import util


URL_BASE = 'https://gis.vgsi.com/lawrencema/parcel.aspx?pid='

TABLE_NAME = 'LawrenceProperties'

# Column labels
VSID = util.VISION_ID
ACCT = util.ACCOUNT_NUMBER
LOCN = util.LOCATION
OWN1 = util.OWNER_1_NAME
OWN2 = util.OWNER_2_NAME
ASMT = util.TOTAL_ASSESSED_VALUE
STYL = util.STYLE
OCCY = util.OCCUPANCY_HOUSEHOLDS
HEAT = util.HEATING_TYPE + util._DESC
FUEL = util.HEATING_FUEL + util._DESC
AIRC = util.AC_TYPE + util._DESC
HTAC = util.HEAT_AC
FLR1 = util.FIRST_FLOOR_USE
RESU = util.RESIDENTIAL_UNITS
LAND = util.LAND_USE_CODE
DESC = util.LAND_USE_CODE + util._DESC
ACRE = util.TOTAL_ACRES
BLDS = util.BUILDING_COUNT
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
    OCCY,
    HEAT,
    FUEL,
    AIRC,
    HTAC,
    FLR1,
    RESU,
    LAND,
    DESC,
    ACRE,
    BLDS,
    ISRS,
]

ID_RANGE_MIN = 250
ID_RANGE_MAX = 107000
ID_RANGE_STOP = ID_RANGE_MAX + 1



def save_and_exit( signum, frame ):

    global df

    if len( df ):

        # Prepare to save in database
        df[VSID] = df[VSID].astype( int )
        df[ACCT] = df[ACCT].str.strip().replace( r'\s+', ' ', regex=True )
        df[LOCN] = df[LOCN].str.strip().replace( r'\s+', ' ', regex=True )
        df[OWN1] = df[OWN1].str.strip().replace( r'\s+', ' ', regex=True )
        df[OWN2] = df[OWN2].str.strip().replace( r'\s+', ' ', regex=True )

        df[ASMT] = df[ASMT].replace( '[\$,]', '', regex=True )
        df[ASMT] = df[ASMT].fillna( '0' ).astype( int )

        df[OCCY] = df[OCCY].replace( r'^\s*$', np.nan, regex=True )
        df[OCCY] = df[OCCY].fillna( '0' ).astype( float ).astype( int )

        df[HEAT] = df[HEAT].str.strip().replace( r'\s+', ' ', regex=True )
        df[FUEL] = df[FUEL].str.strip().replace( r'\s+', ' ', regex=True )
        df[AIRC] = df[AIRC].str.strip().replace( r'\s+', ' ', regex=True )

        df[RESU] = df[RESU].replace( r'^\s*$', np.nan, regex=True )
        df[RESU] = df[RESU].fillna( '0' ).astype( float ).astype( int )

        df[DESC] = df[DESC].str.strip().replace( r'\s+', ' ', regex=True )
        df[ACRE] = df[ACRE].astype( float )

        # Reorganize rows
        df = df.drop_duplicates( subset=[VSID], keep='last' )
        df = df.sort_values( by=[VSID] )

        # Report current status
        print( 'Stopping at VISION ID: {}'.format( vision_id ) )

        # Preserve current progress in database
        util.create_table( TABLE_NAME, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
    sys.exit()

signal.signal( signal.SIGINT, save_and_exit )



# Scrape HTML element by id
def scrape_element( soup, tag, id ):
    element = soup.find( tag, id=id )
    text = element.string if element else ''
    return text


# Scrape cell of Building Attributes table, identified by label regex
def scrape_building_attribute( soup, re_label ):

    table = soup.find( 'table', id='MainContent_ctl01_grdCns' )

    b_found = False
    s_result = ''

    for tr in table:
        if not tr.string:
            for td in tr:
                if b_found:
                    s_result = td.string
                b_found = re.match( re_label, td.string )

    return s_result



######################

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Scrape property assessment data from Vision Government Solutions website' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    parser.add_argument( '-l', dest='luc_filename',  help='Land use codes spreadsheet filename' )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    parser.add_argument( '-r', dest='refresh', action='store_true', help='Refresh records in existing database?' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, args.create )

    # Retrieve residential codes and prepare for processing
    df_res_codes = pd.read_excel( args.luc_filename, dtype=object )
    sr_res_codes = df_res_codes['Residential Land Use Code']
    sr_res_codes = sr_res_codes.astype(str).str.zfill( 4 )
    ls_res_codes = list( sr_res_codes )

    # Read pre-existing table from database
    try:
        df = pd.read_sql_table( TABLE_NAME, engine, index_col=util.ID )
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
            id_range = range( 1 + df[VSID].max(), ID_RANGE_STOP )
    else:
        # Table does not exist.  Discover full range.
        id_range = range( ID_RANGE_MIN, ID_RANGE_STOP )

    print( '' )
    s_doing_what = 'Refreshing {}'.format( len( id_range ) ) if ( len( df ) and args.refresh ) else 'Discovering'
    print( '{} VISION IDs in range: {} to {}'.format( s_doing_what, id_range[0], id_range[-1] ) )

    for vision_id in id_range:

        if vision_id % 50 == 0:
            print( 'Processing VISION ID: {}'.format( vision_id ) )

        url = URL_BASE + str( vision_id )
        rsp = requests.get( url, verify=False )

        if ( rsp.url == url ):

            # Initialize new dataframe row
            sr_row = pd.Series( index=COLS )
            sr_row[VSID] = vision_id

            soup = BeautifulSoup( rsp.text, 'html.parser' )
            sr_row[ACCT] = scrape_element( soup, 'span', 'MainContent_lblAcctNum' )
            sr_row[LOCN] = scrape_element( soup, 'span', 'MainContent_lblTab1Title' )
            sr_row[OWN1] = scrape_element( soup, 'span', 'MainContent_lblOwner' )
            sr_row[OWN2] = scrape_element( soup, 'span', 'MainContent_lblCoOwner' )
            sr_row[ASMT] = scrape_element( soup, 'span', 'MainContent_lblGenAssessment' )
            sr_row[STYL] = scrape_building_attribute( soup, r'Style\:?' )
            sr_row[OCCY] = scrape_building_attribute( soup, r'Occupancy\:?' )
            sr_row[HEAT] = scrape_building_attribute( soup, r'Heat(ing)? Type\:?' )
            sr_row[FUEL] = scrape_building_attribute( soup, r'Heat(ing)? Fuel\:?' )
            sr_row[AIRC] = scrape_building_attribute( soup, r'AC Type\:?' )
            sr_row[HTAC] = scrape_building_attribute( soup, r'Heat\/AC\:?' )
            sr_row[FLR1] = scrape_building_attribute( soup, r'1st Floor Use\:?' )
            sr_row[RESU] = scrape_building_attribute( soup, r'Residential Units\:?' )
            sr_row[LAND] = scrape_element( soup, 'span', 'MainContent_lblUseCode' )
            sr_row[DESC] = scrape_element( soup, 'span', 'MainContent_lblUseCodeDescription' )
            sr_row[ACRE] = scrape_element( soup, 'span', 'MainContent_lblLndAcres' )
            sr_row[BLDS] = scrape_element( soup, 'span', 'MainContent_lblBldCount' )
            sr_row[ISRS] = util.YES if sr_row[LAND] in ls_res_codes else util.NO

            df = df.append( sr_row, ignore_index=True )

    save_and_exit( None, None )
