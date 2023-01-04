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

import signal

import sys
sys.path.append('../util')
import util


def handle_ctrl_c( signum, frame ):

    # Preserve current progress in database
    util.create_table( TABLE_NAME, conn, cur, df=df )

    # Report elapsed time
    util.report_elapsed_time()
    exit()

signal.signal( signal.SIGINT, handle_ctrl_c )




URL_BASE = 'https://gis.vgsi.com/lawrencema/parcel.aspx?pid='

TABLE_NAME = 'LawrenceProperties'

# Column labels
VSID = util.VISION_ID
ACCT = util.ACCOUNT_NUMBER
LOCN = util.LOCATION
OWN1 = util.OWNER_1_NAME
OWN2 = util.OWNER_2_NAME
OCCY = util.OCCUPANCY_HOUSEHOLDS
HEAT = util.HEATING_TYPE + util._DESC
FUEL = util.HEATING_FUEL + util._DESC
AIRC = util.AC_TYPE + util._DESC
HTAC = util.HEAT_AC
FLR1 = util.FIRST_FLOOR_USE
LAND = util.LAND_USE_CODE
BLDS = util.BUILDING_COUNT

# Column order
COLS = \
[
    VSID,
    ACCT,
    LOCN,
    OWN1,
    OWN2,
    OCCY,
    HEAT,
    FUEL,
    AIRC,
    HTAC,
    FLR1,
    LAND,
    BLDS,
]

ID_RANGE_START = 250
ID_RANGE_END = 107000

#
# Documentation of Python requests.Response object
# https://www.w3schools.com/python/ref_requests_response.asp
#


# Scrape HTML element by id
def scrape_element( soup, tag, id ):
    element = soup.find( tag, id=id )
    text = element.string if element else ''
    return text


# Scrape cell of Building Attributes table, identified by text label
def scrape_building_attribute( soup, label, alt_label=None ):

    table = soup.find( 'table', id='MainContent_ctl01_grdCns' )

    b_found = False
    s_result = ''

    for tr in table:
        if not tr.string:
            for td in tr:
                if b_found:
                    s_result = td.string

                b_found = ( td.string == label )

    if ( not s_result ) and ( alt_label != None ):
        s_result = scrape_building_attribute( soup, alt_label )

    return s_result



######################

# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Scrape property assessment data from Vision Government Solutions website' )
    parser.add_argument( '-d', dest='db_filename', help='Database filename' )
    args = parser.parse_args()

    # Open the database
    conn, cur, engine = util.open_database( args.db_filename, False )

    # Read pre-existing table from database
    try:
        df = pd.read_sql_table( TABLE_NAME, engine, index_col=util.ID )
    except:
        df = pd.DataFrame( columns=COLS )

    if len( df ):
        id_range_start = 1 + df[util.VISION_ID].max()
    else:
        id_range_start = ID_RANGE_START

    print( 'Processing VISION ID: {}'.format( id_range_start ) )

    for i in range( id_range_start, ID_RANGE_END ):
        if i % 50 == 0:
            print( 'Processing VISION ID: {}'.format( i ) )

        url = URL_BASE + str( i )
        rsp = requests.get( url, verify=False )

        if ( rsp.url == url ):

            # Initialize new dataframe row
            sr_row = pd.Series( index=COLS )
            sr_row[VSID] = i

            soup = BeautifulSoup( rsp.text, 'html.parser' )
            sr_row[ACCT] = scrape_element( soup, 'span', 'MainContent_lblAcctNum' )
            sr_row[LOCN] = scrape_element( soup, 'span', 'MainContent_lblTab1Title' )
            sr_row[OWN1] = scrape_element( soup, 'span', 'MainContent_lblOwner' )
            sr_row[OWN2] = scrape_element( soup, 'span', 'MainContent_lblCoOwner' )
            sr_row[OCCY] = scrape_building_attribute( soup, 'Occupancy' )
            sr_row[HEAT] = scrape_building_attribute( soup, 'Heat Type:', 'Heating Type' )
            sr_row[FUEL] = scrape_building_attribute( soup, 'Heating Fuel' )
            sr_row[AIRC] = scrape_building_attribute( soup, 'AC Type:', 'AC Type' )
            sr_row[HTAC] = scrape_building_attribute( soup, 'Heat/AC' )
            sr_row[FLR1] = scrape_building_attribute( soup, '1st Floor Use:' )
            sr_row[LAND] = scrape_element( soup, 'span', 'MainContent_lblUseCode' )
            sr_row[BLDS] = scrape_element( soup, 'span', 'MainContent_lblBldCount' )

            df = df.append( sr_row, ignore_index=True )

            # Prepare to save in database
            df = df.drop_duplicates( subset=[VSID], keep='last' )
            df = df.sort_values( by=[VSID] )
            df[ACCT] = df[ACCT].replace( r'\s+', ' ', regex=True )
            df[OCCY] = df[OCCY].replace( r'^\s*$', np.nan, regex=True )
            df[OCCY] = df[OCCY].fillna( '0' ).astype( float ).astype( int )
            df[VSID] = df[VSID].astype( int )

