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
VALUE = 'value'
VSID = util.VISION_ID
EXAMPLE_COLUMNS = [LABEL,VALUE,VSID]

SAVE_FREQUENCY = 50


# Extract label examples from building tables on current HTML page
def extract_labels( soup, ls_building_ids, town_l, df_labels, ls_labels ):

    for dc_ids in ls_building_ids:
        table = soup.find( 'table', id=dc_ids[vision.BUILDING_TABLE_ID] )

        if table:
            # Iterate over rows
            trs = table.find_all( 'tr' )
            for tr in trs:

                # If current row has 2 cells...
                tds = tr.find_all( 'td' )
                if ( len( tds ) == 2 ):
                    s_label = tds[0].string.strip()
                    s_value = tds[1].string.strip()

                    # If we are interested in this label and value is not empty...
                    if s_label in ls_labels and s_value not in ['', '0']:

                        # Save the example in the dataframe
                        df_labels = df_labels.append( { LABEL: tds[0].string, VALUE: s_value, VSID: vision_id }, ignore_index=True )

                        # As soon as we have a non-empty example for this vision_id, quit the loop
                        break

    df_labels = df_labels.drop_duplicates()
    return df_labels


# Save current work
def save_progress( town_c, vision_id, df_labels, table_name, out_conn, out_cur ):
    util.report_elapsed_time( prefix='' )
    print( '  {} {}: {} examples'.format( town_c, vision_id, len( df_labels ) ) )
    df_labels = df_labels.sort_values( by=EXAMPLE_COLUMNS )
    printctl.off()
    util.create_table( table_name, out_conn, out_cur, df=df_labels )
    printctl.on()


###########################################


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Find non-empty examples of Vision Building labels' )
    parser.add_argument( '-o', dest='output_filename',  help='Output database filename', required=True )
    parser.add_argument( '-t', dest='town',  help='Town to search', required=True )
    parser.add_argument( '-e', dest='example_labels',  help='Example labels to find', required=True )
    parser.add_argument( '-c', dest='create', action='store_true', help='Create new database?' )
    args = parser.parse_args()

    # Open the output database
    out_conn, out_cur, out_engine = util.open_database( args.output_filename, args.create )

    # Initialize town name formats
    town_c = args.town.capitalize()
    town_l = args.town.lower()
    table_name = '_'.join( ['VisionExamples', town_c] )

    # Retrieve existing dataframe of labels
    try:
        df_labels = pd.read_sql_table( table_name, out_engine, index_col=util.ID )
    except:
        df_labels = pd.DataFrame( columns=EXAMPLE_COLUMNS )

    # Retrieve table from input database
    in_conn, in_cur, in_engine = util.open_database( '../db/vision_{}.sqlite'.format( town_l ), False )
    df = pd.read_sql_table( 'Vision_Raw_{}'.format( town_c ), in_engine, index_col=util.ID, columns=[util.VISION_ID] )

    # Extract list of vision IDs to scrape
    id_range = df[util.VISION_ID].to_list()

    # Prepare URL base
    url_base = vision.URL_BASE.format( town_l )

    # Initialize list of labels
    ls_labels = args.example_labels.split( ',' )
    print( '' )
    print( 'Searching {} for examples of labels {}'.format( town_c, ls_labels ) )

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
            df_labels = extract_labels( soup, ls_building_ids, town_l, df_labels, ls_labels )

        # Periodically save progress
        save_counter += 1
        if save_counter == SAVE_FREQUENCY:
            save_progress( town_c, vision_id, df_labels, table_name, out_conn, out_cur )
            save_counter = 0

    # Save progress
    save_progress( town_c, 'done', df_labels, table_name, out_conn, out_cur )

    util.report_elapsed_time()
