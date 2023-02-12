# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse
import os
import requests
from bs4 import BeautifulSoup

import warnings

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import printctl
import util
import vision


LABEL = 'label'
LABEL_TABLE = 'VisionLabels'

SAVE_FREQUENCY = 50


# Extract labels from building tables on current HTML page
def extract_labels( soup, ls_building_ids, town_l, df_labels ):

    for dc_ids in ls_building_ids:
        table = soup.find( 'table', id=dc_ids[vision.BUILDING_TABLE_ID] )

        if table:
            # Iterate over rows
            trs = table.find_all( 'tr' )
            for tr in trs:
                # If current row has 2 cells and first cell matches label...
                tds = tr.find_all( 'td' )
                if ( len( tds ) == 2 ):

                    # Save the label in the dataframe
                    df_labels = df_labels.append( { LABEL: tds[0].string }, ignore_index=True )

    df_labels = df_labels.drop_duplicates()
    return df_labels


# Save current work
def save_progress( town_c, vision_id, df_labels, out_conn, out_cur ):
    util.report_elapsed_time( prefix='' )
    print( '  {} {}: {} labels'.format( town_c, vision_id, len( df_labels ) ) )
    df_labels = df_labels.sort_values( by=[LABEL] )
    printctl.off()
    util.create_table( LABEL_TABLE, out_conn, out_cur, df=df_labels )
    printctl.on()


###########################################


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Extract all labels used in Building tables shown in Vision parcel display' )
    parser.add_argument( '-l', dest='label_filename',  help='Output filename - Name of label database file', required=True )
    parser.add_argument( '-t', dest='towns',  help='List of towns to include', required=True )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    args = parser.parse_args()

    # Retrieve list of towns
    ls_towns = args.towns.split( ',' )
    print( '\n=======> Extracting labels used by towns: {}'.format( ls_towns ) )

    # Open the output database
    out_conn, out_cur, out_engine = util.open_database( args.label_filename, args.create )

    # Retrieve existing dataframe of labels
    try:
        df_labels = pd.read_sql_table( LABEL_TABLE, out_engine, index_col=util.ID )
    except:
        df_labels = pd.DataFrame( columns=[LABEL] )

    # Process list of towns
    for town in ls_towns:

        town_c = town.capitalize()
        town_l = town.lower()

        print( '\n=======> {}'.format( town_c ) )

        # Retrieve table from input database
        in_conn, in_cur, in_engine = util.open_database( '../db/vision_{}.sqlite'.format( town_l ), False )
        df = pd.read_sql_table( 'Vision_Raw_{}'.format( town_c ), in_engine, index_col=util.ID, columns=[util.VISION_ID] )

        # Extract list of vision IDs to scrape
        id_range = df[util.VISION_ID].to_list()

        # Prepare URL base
        url_base = vision.URL_BASE.format( town )

        print( '' )
        save_counter = 0

        # Iterate over vision IDs for current town
        for vision_id in id_range:

            url = url_base + str( vision_id )

            warnings.filterwarnings( 'ignore' )
            try:
                rsp = requests.get( url, verify=False )
            except Exception as e:
                print( '' )
                print( '==>' )
                print( '==> Exiting due to exception:' )
                print( '==> {}'.format( str( e ) ) )
                print( '==>' )
                exit()
            warnings.resetwarnings()

            # If we got the URL we requested...
            if ( rsp.url == url ):

                # Parse the HTML
                soup = BeautifulSoup( rsp.text, 'html.parser' )

                # Find IDs of all building tables
                building_count = vision.scrape_element( soup, 'span', 'MainContent_lblBldCount' )
                ls_building_ids, not_used_1, not_used_2, not_used_3 = vision.find_all_building_ids( soup, building_count )

                # Extract labels from tables
                df_labels = extract_labels( soup, ls_building_ids, town_l, df_labels )

            # Periodically save progress
            save_counter += 1
            if save_counter == SAVE_FREQUENCY:
                save_progress( town_c, vision_id, df_labels, out_conn, out_cur )
                save_counter = 0

        # Save progress
        save_progress( town_c, 'done', df_labels, out_conn, out_cur )

    util.report_elapsed_time()
