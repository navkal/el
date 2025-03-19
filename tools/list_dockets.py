# Copyright 2025 Energize Andover.  All rights reserved.

import argparse

import os

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sqlite3
import sqlalchemy

import warnings


B_DEBUG = False
if B_DEBUG:
    print( '' )
    print( '---------------------' )
    print( 'Running in debug mode' )
    print( '---------------------' )
    print( '' )


DASHBOARD_URL = 'https://eeaonline.eea.state.ma.us/dpu/fileroom/#/dashboard'

XPATH_DASHBOARD = '//input[@id="mat-input-1"]'
XPATH_TABLE = '//mat-table[@role="grid"]'
XPATH_CHILDREN = './child::*'


# --> Reporting of elapsed time -->
import time
START_TIME = time.time()
def report_elapsed_time( prefix='\n', start_time=START_TIME ):
    elapsed_time = round( ( time.time() - start_time ) * 1000 ) / 1000
    minutes, seconds = divmod( elapsed_time, 60 )
    ms = round( ( elapsed_time - int( elapsed_time ) ) * 1000 )
    print( prefix + 'Elapsed time: {:02d}:{:02d}.{:d}'.format( int( minutes ), int( seconds ), ms ) )
# <-- Reporting of elapsed time <--


# Click element
def click_element( el ):
    if B_DEBUG:
        driver.execute_script( 'arguments[0].style.backgroundColor = "red"', el )
    el.click()


# Get the Chrome driver
def get_driver():

    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    if not B_DEBUG:
        options.add_argument( '--headless' )

    # Initialize the driver
    try:
        driver = webdriver.Chrome( service=webdriver.ChromeService( ChromeDriverManager().install() ), options=options )
    except:
        # Retry using syntax from a different Selenium version
        driver = webdriver.Chrome( ChromeDriverManager().install(), options=options )

    if B_DEBUG:
        driver.maximize_window()

    return driver


# Initialize the dashboard
def init_dashboard( s_tile, s_option ):

    # Open the dashboard
    driver.get( DASHBOARD_URL )

    print( '' )
    print( 'Waiting for dashboard' )
    print( '' )

    try:
        # Wait for dashboard to load
        WebDriverWait( driver, 30 ).until( EC.presence_of_element_located( ( By.XPATH, XPATH_DASHBOARD ) ) )

    except Exception as e:
        print( '' )
        print( 'Error loading dashboard.' )
        if isinstance( e, TimeoutException ):
            print( 'Request timed out.' )
        print( '' )
        print( '  URL: {}'.format( DASHBOARD_URL ) )
        print( '' )
        driver.quit()
        exit()


    # Click input element of specified dashboard tile; opens overlay dropdown
    xpath_tile = '//span[text()="{}"]/ancestor::mat-form-field//input'.format( s_tile )
    el_input = WebDriverWait( driver, 10 ).until( EC.presence_of_element_located( ( By.XPATH, xpath_tile ) ) )
    click_element( el_input )

    # Enter option text into input element; reduces overlay dropdown to only one option
    xpath_dropdown = '//div[@class="cdk-overlay-pane"]'
    WebDriverWait( driver, 10 ).until( EC.presence_of_element_located( ( By.XPATH, xpath_dropdown ) ) )
    el_input.send_keys( s_option )

    # Click the option; triggers loading of docket list
    xpath_option = '//mat-option[@role="option"]'
    el_option = WebDriverWait( driver, 10 ).until( EC.presence_of_element_located( ( By.XPATH, xpath_option ) ) )
    click_element( el_option )

    return


def get_docket_list( df, n_page=1 ):

    print( f'Waiting for table, page {n_page}' )

    # Wait for table to load
    try:
        el_table = WebDriverWait( driver, 30 ).until( EC.presence_of_element_located( ( By.XPATH, XPATH_TABLE ) ) )
    except:
        print( '' )
        print( 'Error loading table page.' )
        if isinstance( e, TimeoutException ):
            print( 'Request timed out.' )
        print( '' )
        driver.quit()
        exit()

    # Extract children of table element
    ls_table_kids = el_table.find_elements( By.XPATH, XPATH_CHILDREN )

    # First time, initialize empty dataframe with scraped column names
    if len( df.columns ) == 0:

        # Extract list of column headers
        ls_headers = ls_table_kids[0].find_elements( By.XPATH, XPATH_CHILDREN )

        # Build list of columns for dataframe
        ls_cols = []
        for el_header in ls_headers:
            ls_cols.append( el_header.text )

        # Set columns in dataframe
        df = pd.DataFrame( columns=ls_cols )

    # Load rows of scraped data into dataframe
    for el_row in ls_table_kids[1:]:

        # Get list of cell elements
        ls_cells = el_row.find_elements( By.XPATH, XPATH_CHILDREN )

        # Build row from cells
        dc_row = {}
        for el_cell in ls_cells:
            dc_row[ df.columns[ len( dc_row ) ] ] = el_cell.text

        # Add row to dataframe
        df = pd.concat( [df, pd.DataFrame( [dc_row] )], ignore_index=True )

    # Find the Next Page button
    ls_next_buttons = driver.find_elements( By.CSS_SELECTOR, 'button.mat-paginator-navigation-next' )

    # If Next Page button exists, process it
    if ls_next_buttons:
        next_button = ls_next_buttons[0]

        # If the button is enabled, load and process the next page
        if next_button.is_enabled():

            # Scroll the button into view
            driver.execute_script( 'arguments[0].scrollIntoView(true);', next_button)

            action = ActionChains( driver )

            # Try to click button until it succeeds
            for i in range( 5 ):
                try:
                    click_element( next_button )
                    break
                except:
                    if B_DEBUG:
                        print( '==> Pressing PAGE_DOWN to expose Next Page button' )
                    time.sleep( 0.5 )
                    action.send_keys( Keys.PAGE_DOWN ).perform()

            df = get_docket_list( df, n_page + 1 )

    return df


# Generate database table name from tile and option input values
def make_table_name( s_tile, s_option ):

    s_tile = '_'.join( s_tile.split( ' ' ) )
    s_option = '_'.join( s_option.split( ' ' ) )
    s_table_name = s_tile + '__' + s_option
    return s_table_name


# Open the SQLite database
def open_database( db_filename ):

    conn = sqlite3.connect( db_filename )
    cur = conn.cursor()

    engine = sqlalchemy.create_engine( 'sqlite:///' + db_filename )

    return conn, cur, engine


# Prepare dataframe for storing in database
def prepare_for_database( df ):

    # Clean up column names
    df.columns = df.columns.str.replace(' ', '_')

    # Clean up whitespace
    df = df.replace( r'\n', ' ', regex=True )

    # Suppress version-specific warnings about pd.to_datetime()
    with warnings.catch_warnings():
        warnings.simplefilter( 'ignore' )

        # Clean up date columns
        for col in df.columns:
            try:
                # Convert date to sortable format
                df[col] = pd.to_datetime( df[col] )
                df[col] = df[col].dt.strftime( '%Y-%m-%d' )

            except:
                pass

    # Clean up index
    df = df.reset_index( drop=True )

    return df


# Save scraped docket list
def save_docket_list( db_filename, s_tile, s_option, df ):

    # Generate table name
    table_name = make_table_name( s_tile, s_option )
    print( '' )
    print( 'Saving to table:', table_name )

    # Open the database
    conn, cur, engine = open_database( db_filename )

    # Drop table if it already exists
    cur.execute( 'DROP TABLE IF EXISTS ' + table_name )

    # Generate SQL command to create table
    create_sql = 'CREATE TABLE ' + table_name + ' ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE'
    for col_name in df.columns:
        create_sql += ', "{0}"'.format( col_name )
    create_sql += ' )'

    # Create the empty table
    cur.execute( create_sql )

    # Load data into table
    df.to_sql( table_name, conn, if_exists='append', index=False )

    # Commit changes
    conn.commit()
    return



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Scrape list of dockets and save in database' )
    parser.add_argument( '-t', dest='tile', help='Tile of dashboard', required=True )
    parser.add_argument( '-o', dest='option', help='Option to select from tile', required=True )
    parser.add_argument( '-d', dest='db_filename', help='Name of SQLite database file', required=True )
    args = parser.parse_args()

    # Report argument list
    print( '' )
    print( 'Arguments:',  )
    print( '  Tile: {}'.format( args.tile ) )
    print( '  Option: {}'.format( args.option ) )
    print( '  Database: {}'.format( args.db_filename ) )

    # Report start time
    print( '' )
    print( 'Starting at', time.strftime( '%H:%M:%S', time.localtime( START_TIME ) ) )

    # Get the Chrome driver
    driver = get_driver()

    # Initialize the dashboard
    init_dashboard( args.tile, args.option )

    # Load the list of dockets
    df = pd.DataFrame()
    df = get_docket_list( df )

    # Prepare dataframe for database
    df = prepare_for_database( df )

    # Save docket list in database
    save_docket_list( args.db_filename, args.tile, args.option, df )

    # Close the browser
    driver.quit()

    # Report elapsed time
    report_elapsed_time()
