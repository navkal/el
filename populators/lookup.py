# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import time
import re
import usaddress
import copy
import collections

import sys
sys.path.append( '../util' )
import util

WATER_CUSTOMER = 'water_customer'
SOLAR_ID = 'solar_id'
ADDR = util.NORMALIZED_ADDRESS
STREET_NUMBER = util.NORMALIZED_STREET_NUMBER
STREET_NAME = util.NORMALIZED_STREET_NAME
APT_NUM = 'apt_num'
NOTHING = ''


# Search for match between fragments of two names
def partial_match( name1, name2 ):

    matched = False

    if name1 and name2:

        # Split names into fragments
        ls_name1 = re.findall( r"[\w']+", name1 )
        ls_name2 = re.findall( r"[\w']+", name2 )

        # Search for exact match of any fragment pair
        for s1 in ls_name1:
            if matched:
                break
            for s2 in ls_name2:
                if matched:
                    break
                matched = s1 == s2

    return matched


# Determine whether current row represents owner of parcel
def is_homeowner( row ):

    b_is = \
        ( row[util.OWNER_OCCUPIED] == 'Y' ) \
        and \
        (
            (
                partial_match( row[util.OWNER_1_NAME], row[util.LAST_NAME] )
                and
                (
                    partial_match( row[util.OWNER_1_NAME], row[util.FIRST_NAME] )
                    or
                    partial_match( row[util.OWNER_1_NAME], 'TRUST' )
                    or
                    partial_match( row[util.OWNER_1_NAME], 'TR' )
                    or
                    partial_match( row[util.OWNER_1_NAME], 'RT' )
                    or
                    partial_match( row[util.OWNER_1_NAME], 'IRT' )
                )
            )
            or
            (
                partial_match( row[util.OWNER_2_NAME], row[util.LAST_NAME] )
                and
                (
                    partial_match( row[util.OWNER_2_NAME], row[util.FIRST_NAME] )
                    or
                    partial_match( row[util.OWNER_2_NAME], 'TRUST' )
                    or
                    partial_match( row[util.OWNER_2_NAME], 'TR' )
                    or
                    partial_match( row[util.OWNER_2_NAME], 'RT' )
                    or
                    partial_match( row[util.OWNER_2_NAME], 'IRT' )
                )
            )
            or
            (
                partial_match( row[util.OWNER_3_NAME], row[util.LAST_NAME] )
                and
                (
                    partial_match( row[util.OWNER_3_NAME], row[util.FIRST_NAME] )
                    or
                    partial_match( row[util.OWNER_3_NAME], 'TRUST' )
                    or
                    partial_match( row[util.OWNER_3_NAME], 'TR' )
                    or
                    partial_match( row[util.OWNER_3_NAME], 'RT' )
                    or
                    partial_match( row[util.OWNER_3_NAME], 'IRT' )
                )
            )
        )

    return b_is

# Based on USPS guidelines: https://pe.usps.com/text/pub28/28apc_002.htm
STREET_TYPES = \
{
    'AV': 'AVE',
    'AVENUE': 'AVE',
    'BLUFF': 'BLF',
    'BRIDGE': 'BRG',
    'BROOK': 'BRK',
    'BROOKS': 'BRKS',
    'BYPASS': 'BYP',
    'CENTER': 'CTR',
    'CG': 'XING',
    'CI': 'CIR',
    'CIRCLE': 'CIR',
    'CLUB': 'CLB',
    'COMMON': 'CMN',
    'COMMONS': 'CMNS',
    'CORNER': 'COR',
    'COURT': 'CT',
    'COVE': 'CV',
    'CREEK': 'CRK',
    'CRESCENT': 'CRES',
    'CREST': 'CRST',
    'CROSSING': 'XING',
    'DRIVE': 'DR',
    'ESTATE': 'EST',
    'ESTATES': 'ESTS',
    'FIELD': 'FLD',
    'GLEN': 'GLN',
    'HOLLOW': 'HOLW',
    'LANE': 'LN',
    'LODGE': 'LDG',
    'LOOP': 'LOOP',
    'LP': 'LOOP',
    'OWAY': 'OWAY',
    'PK': 'PARK',
    'PATH': 'PATH',
    'PH': 'PATH',
    'PIKE': 'PIKE',
    'PINES': 'PNES',
    'PLACE': 'PL',
    'RIDGE': 'RDG',
    'ROAD': 'RD',
    'RD.': 'RD',
    'RUN': 'RUN',
    'RW': 'ROADWAY',
    'SQUARE': 'SQ',
    'ST.': 'ST',
    'STREET': 'ST',
    'TERRACE': 'TER',
    'TERR': 'TER',
    'TR': 'TER',
    'WAY': 'WAY',
    'WAYSIDE': 'WAYSIDE',
    'WY': 'WAY',

    # Not real street types, added to accommodate unusual addresses in Water data
    'BA': 'BA',
    'INFIRM': 'INFIRM',
    'HS': 'HS',
    'CAMPU': 'CAMPU',
    'H.': 'H.',
}

# Based on USPS guidelines: https://pe.usps.com/text/pub28/28c2_014.htm
DIRS = \
{
    'NORTH': 'N',
    'SOUTH': 'S',
    'EAST': 'E',
    'WEST': 'W',
}

CITY_STATE_ZIP = ' Andover MA 01810'

EXPECTED_KEYS = \
{
    'AddressNumber',
    'StreetName',
    'StreetNamePostType',
    'PlaceName',
    'StateName',
    'ZipCode',
    'StreetNamePreDirectional',
    'StreetNamePostDirectional',
    'AddressNumberSuffix',
    'OccupancyIdentifier',
    'IntersectionSeparator',
    'SecondStreetName',
    'SecondStreetNamePostType',
    'SubaddressType',
    'SubaddressIdentifier',
    'SecondStreetNamePreDirectional',
    'StreetNamePostModifier',
    'BuildingName',
    'Recipient',
    'USPSBoxType',
    'USPSBoxID',
}

# Normalize street address
def normalize_address( row, col_name, verbose=False ):

    # Create original copy of the address
    original = row[col_name]

    # Initialize return value
    address = original

    # Help usaddress parsing algorithm with these troublesome cases
    address = re.sub( r' CI$', ' CIR', address )
    address = address.replace( ' CI ', ' CIR ' )
    address = address.replace( '-', ' ' )

    if verbose:
        print( '' )
        print( 'Normalizing address in column "{0}": "{1}"'.format( col_name, address ) )

    try:
        norm = usaddress.tag( address + CITY_STATE_ZIP )

        if len( norm ) and isinstance( norm[0], dict ):

            parts = copy.deepcopy( norm[0] )

            # Correct parsing mistakes that occur, for example, with 'GRANDVIEW TR'
            if ( 'Recipient' in parts ) and ( 'StreetName' not in parts ) and ( 'StreetNamePostType' not in parts ):
                if verbose:
                    print( 'Bf replacing Recipient', parts )
                od = collections.OrderedDict()
                for key in parts.keys():
                    if key == 'Recipient':
                        split = parts['Recipient'].split()
                        od['StreetName'] = ' '.join( split[:-1] )
                        od['StreetNamePostType'] = split[-1]
                    else:
                        od[key] = parts[key]
                parts = od
                if verbose:
                    print( 'Af replacing Recipient', parts )


            keys = parts.keys()

            for key in keys:
                if verbose:
                    print( '- {0} "{1}"'.format( key, parts[key] ) )
                if key not in EXPECTED_KEYS:
                    print( address )
                    print( 'KEY NOT RECOGNIZED', key, parts[key] )
                    exit()

            if 'StreetNamePostType' in keys:
                street_type = parts['StreetNamePostType']
                if street_type in STREET_TYPES.values():
                    parts['StreetNamePostType'] = street_type
                elif street_type in STREET_TYPES:
                    parts['StreetNamePostType'] = STREET_TYPES[street_type]
                else:
                    print( address )
                    print( 'STREET TYPE NOT FOUND', street_type )
                    exit()

            if ( 'StreetNamePreDirectional' in keys ) and ( 'StreetName' in keys ):
                pre_dir = parts['StreetNamePreDirectional']
                if pre_dir in DIRS.values():
                    parts['StreetNamePreDirectional'] = pre_dir
                elif pre_dir in DIRS:
                    parts['StreetNamePreDirectional'] = DIRS[pre_dir]
                else:
                    print( address )
                    print( parts )
                    print( 'PRE DIRECTIONAL NOT FOUND', pre_dir )
                    exit()

            if ( 'StreetNamePostDirectional' in keys ) and ( 'StreetName' in keys ):
                post_dir = parts['StreetNamePostDirectional']
                if post_dir in DIRS.values():
                    parts['StreetNamePostDirectional'] = post_dir
                elif post_dir in DIRS:
                    parts['StreetNamePostDirectional'] = DIRS[post_dir]
                else:
                    print( 'POST DIRECTIONAL NOT FOUND', post_dir )
                    exit()

        a_org = []
        for key in norm[0].keys():
            a_org.append( norm[0][key] )
        s_org = ' '.join( a_org )
        a_new = []
        for key in parts.keys():
            a_new.append( parts[key] )
        s_new = ' '.join( a_new )

        if verbose:
            print( '' )
            print( 'Reconstituted address:' )
            print( '- unmapped: "{0}"'.format( s_org ) )
            print( '-   mapped: "{0}"'.format( s_new ) )
            print( '' )

        if s_new.endswith( CITY_STATE_ZIP ):
            address = s_new[ :-len( CITY_STATE_ZIP ) ]
        else:
            print( 'BAD ENDING!!!' )
            exit()

    except usaddress.RepeatedLabelError:

        if verbose:
            print( 'Exception: usaddress.RepeatedLabelError' )


    if verbose:
        if address != original:
            print( 'Address mapped from "{0}" to "{1}"'.format( original, address ) )

    return address


# Extract number from normalized address
def extract_street_number( address ):
    address = address.strip()

    if address == '':
        number = ''
    else:
        a_parts = address.split( ' ', 1 )
        prefix = a_parts[0]
        number = prefix if ( prefix.isnumeric() or prefix[0].isdigit() or prefix[-1].isdigit() ) else ''

    return number

# Extract number from normalized address
def get_street_number( row, col_name ):
    return extract_street_number( row[col_name] )


# Extract street from normalized address
def get_street_name( row, col_name ):

    number = extract_street_number( row[col_name] )

    if number == '':
        street = row[col_name]
    else:
        street = row[col_name].split( ' ', 1 )[-1]

    return street


# Determine whether current row represents family member of parcel's owner
def is_family( row, df ):

    if row[util.IS_HOMEOWNER] or ( row[util.OWNER_OCCUPIED] == 'N' ):
        b_is = False

    elif row[APT_NUM]:

        df_owner = df.loc \
        [
            ( df[util.IS_HOMEOWNER] == True )
            &
            ( df[util.OWNER_1_NAME] == row[util.OWNER_1_NAME] )
            &
            ( df[util.OWNER_2_NAME] == row[util.OWNER_2_NAME] )
            &
            ( df[util.OWNER_3_NAME] == row[util.OWNER_3_NAME] )
            &
            ( df[APT_NUM] == row[APT_NUM] )
        ]

        b_is = len( df_owner ) > 0

    else:

        # Decide based on owner names

        b_is = \
        (
            partial_match( row[util.OWNER_1_NAME], row[util.LAST_NAME] )
            or
            partial_match( row[util.OWNER_2_NAME], row[util.LAST_NAME] )
            or
            partial_match( row[util.OWNER_3_NAME], row[util.LAST_NAME] )
        )

    return b_is


# Drop excess rows following merge of Census and Assessment tables
def drop_assessment_noise( df_merge ):

    t = time.time()
    print( '' )
    print( 'drop_assessment_noise() starting at', time.strftime( '%H:%M:%S', time.localtime( t ) ) )

    # Fill null cells
    df_merge[util.PARCEL_ID] = df_merge[util.PARCEL_ID].fillna('').astype(str)

    bf_len = len( df_merge )

    # Get list of unique Resident IDs from merged table
    res_ids = df_merge[util.RESIDENT_ID].unique()

    # Iterate over Resident IDs
    for res_id in res_ids:

        # Extract all rows containing this Resident ID
        df_res_id = df_merge[ df_merge[util.RESIDENT_ID] == res_id ]

        # If there are multiple rows pertaining to current resident...
        if len( df_res_id ) > 1:

            # Extract rows that represent parcels owned by resident
            df_is_homeowner = df_res_id[ df_res_id[util.IS_HOMEOWNER] == True ]

            # If resident owns any parcels...
            if len( df_is_homeowner ) > 0:

                # Extract rows that represent parcels not owned by resident
                df_is_not_homeowner = df_res_id[ df_res_id[util.IS_HOMEOWNER] == False ]

                # If any rows represent parcels not owned...
                if len( df_is_not_homeowner ) > 0:

                    # Drop them
                    df_merge = df_merge.drop( labels=df_is_not_homeowner.index )
            else:
                # Resident owns no parcels

                # Extract rows that represent parcels where resident is family
                df_is_family = df_res_id[ df_res_id[util.IS_FAMILY] == True ]

                # If resident is family...
                if len( df_is_family ) > 0:

                    # Extract rows that represent parcels where resident is not family
                    df_is_not_family = df_res_id[ df_res_id[util.IS_FAMILY] == False ]

                    # If any rows represent parcels where resident is not family...
                    if len( df_is_not_family ) > 0:

                        # Drop them
                        df_merge = df_merge.drop( labels=df_is_not_family.index )

                else:
                    #
                    # The Census+Assessment merge had correlated this resident with multiple parcels.
                    # However, we don't have enough information to determine which is the correct one.
                    # Rather than retain excess and misleading information in the database, we clear it out.
                    #

                    # Drop excess rows
                    drop_index = df_res_id.index.drop( [ df_res_id.index[0] ] )
                    df_merge = df_merge.drop( labels=drop_index )

                    # Clear unreliable values
                    for col_name in df_assessment_res.columns:
                        if col_name not in df_census.columns:
                            df_merge.loc[ df_merge[util.RESIDENT_ID] == res_id, col_name] = ''

    # Report outcome
    print( 'drop_assessment_noise() removed {0} rows'.format( bf_len - len( df_merge ) ) )
    util.report_elapsed_time( prefix='drop_assessment_noise() done: ', start_time=t )

    return df_merge


# Insert water data that does not correlate with Resident IDs
def insert_nonresident_water( df_merge ):

    bf_len = len( df_merge )

    # Extract water table Meter Numbers that are not already in merge
    df_w = df_water.copy()
    missing = set( df_w[util.METER_NUMBER] ) - set( df_merge[util.METER_NUMBER] )
    df_w = df_w[ df_w[util.METER_NUMBER].isin( missing ) ]

    # Synthesize column representing water customer
    df_w[WATER_CUSTOMER] = ( df_w[util.FIRST_NAME] + ' ' + df_w[util.LAST_NAME] ).str.strip()

    # Find out frequency of each customer in the dataframe
    vc = df_w[WATER_CUSTOMER].value_counts()

    # Iterate over customers to build list of non-resident Meter Numbers
    nonres_meter_numbers = []

    for customer in vc.index:
        df_customer = df_w[ df_w[WATER_CUSTOMER] == customer ]

        if ( vc[customer] > 2 ) or ( set( df_customer[util.SERVICE_TYPE].unique() ) - set( ['Residential', 'Irrigation'] ) != set( [] ) ):
            for index, row in df_customer.iterrows():
                nonres_meter_numbers.append( row[util.METER_NUMBER] )

    # Restrict water dataframe to non-resident Meter Numbers
    df_w = df_w[ df_w[util.METER_NUMBER].isin( nonres_meter_numbers ) ]


    # Extract Assessment table Parcel IDs that are not in merge
    df_a = df_assessment_res.copy()
    missing = set( df_a[util.PARCEL_ID] ) - set( df_merge[util.PARCEL_ID] )
    df_a = df_a[ df_a[util.PARCEL_ID].isin( missing ) ]

    # Merge Water and Assessment
    df_m = pd.merge( df_w, df_a, how='left', on=ADDR )

    # Find Meter Numbers that occur more than once
    vc = df_m[util.METER_NUMBER].value_counts()
    dups = vc[ vc > 1 ]

    # Remove excess rows from the merge
    for meter_num, count in dups.items():

        # Drop excess rows
        df_meter_num = df_m[ df_m[util.METER_NUMBER] == meter_num ]
        drop_index = df_meter_num.index.drop( [ df_meter_num.index[0] ] )
        df_m = df_m.drop( labels=drop_index )

        # Clear unreliable values
        for col_name in df_a.columns:
            if col_name not in df_w.columns:
                df_m.loc[ df_m[util.METER_NUMBER] == meter_num, col_name] = ''


    # Extract Solar table Solar IDs that are not in merge
    df_s = df_solar.copy()
    missing = set( df_s[SOLAR_ID] ) - set( df_merge[SOLAR_ID] )
    df_s = df_s[ df_s[SOLAR_ID].isin( missing ) ]

    # Add Solar to merge
    df_m = pd.merge( df_w, df_s, how='left', on=ADDR )
    df_m[SOLAR_ID] = df_m[SOLAR_ID].fillna('').astype(str)

    # Append local merge results to global merge
    df_merge = df_merge.append( df_m, ignore_index=True, sort=True )

    # Report
    print( 'insert_nonresident_water() added {0} rows'.format( len( df_merge ) - bf_len ) )

    return df_merge


# Drop excess rows following addition of Water table to merge
def drop_water_noise( df_merge ):

    t = time.time()
    print( '' )
    print( 'drop_water_noise() starting at', time.strftime( '%H:%M:%S', time.localtime( t ) ) )

    # Fill null cells
    df_merge[util.METER_NUMBER] = df_merge[util.METER_NUMBER].fillna('').astype(str)

    bf_len = len( df_merge )

    # Get list of unique Resident/Parcel ID pairs from merged table
    df_merge['pair'] = df_merge[util.RESIDENT_ID].str.cat( ',' + df_merge[util.PARCEL_ID] )
    unique_pairs = df_merge['pair'].unique()

    # Establish columns to be preserved
    preserve_columns = df_census.columns.union( df_assessment_res.columns )

    # Create set of service types for comparison
    res_irr = set( ['Residential','Irrigation'] )


    # Iterate over Resident IDs
    for pair in unique_pairs:

        # Extract all rows containing this Resident ID
        df_pair = df_merge[ df_merge['pair'] == pair ]

        # If there are multiple rows pertaining to current resident...
        if len( df_pair ) > 1:

            # The +Water merge has correlated this resident/parcel pair with multiple Meter Numbers.

            # Get the Parcel ID
            parcel_id = df_pair.iloc[0][util.PARCEL_ID]

            # Determine whether any service types are duplicated among the rows
            vc = df_pair[util.SERVICE_TYPE].value_counts()
            dup_service_types = vc[vc > 1]

            # Determine how listed services compare to expected { Residential, Irrigation }
            vc_idx = set( vc.index )
            service_type_diff = vc_idx.symmetric_difference( res_irr )

            # If we don't have a Parcel ID, or we have a duplicated service type, or service types are not as expected:
            if ( parcel_id == '' ) or ( len( dup_service_types ) > 0 ) or ( len( service_type_diff ) > 0 ):
                #
                # We don't have enough information to determine which Meter Number is the correct one.
                # Rather than retain excess and misleading information in the database, we clear it out.
                #

                # Drop excess rows
                drop_index = df_pair.index.drop( [ df_pair.index[0] ] )
                df_merge = df_merge.drop( labels=drop_index )

                # Clear unreliable values
                for col_name in df_water.columns:
                    if col_name not in preserve_columns:
                        df_merge.at[ df_pair.index[0], col_name ] = ''


    # Report outcome
    print( 'drop_water_noise() removed {0} rows'.format( bf_len - len( df_merge ) ) )
    util.report_elapsed_time( prefix='drop_water_noise() done: ', start_time=t )

    return df_merge


# Add columns to identify property owners and family members
def flag_ownership( df ):

    df[util.LAST_NAME] = df[util.LAST_NAME].fillna( '' )
    df[util.FIRST_NAME] = df[util.FIRST_NAME].fillna( '' )
    df[util.MIDDLE_NAME] = df[util.MIDDLE_NAME].fillna( '' )
    df[util.OWNER_1_NAME] = df[util.OWNER_1_NAME].fillna( '' )
    df[util.OWNER_2_NAME] = df[util.OWNER_2_NAME].fillna( '' )
    df[util.OWNER_3_NAME] = df[util.OWNER_3_NAME].fillna( '' )

    t = time.time()
    print( '' )
    print( 'util.IS_HOMEOWNER starting at', time.strftime( '%H:%M:%S', time.localtime( t ) ) )

    df[util.IS_HOMEOWNER] = df.apply( lambda row: is_homeowner( row ), axis=1 )

    print( 'Found {0} homeowners'.format( df[util.IS_HOMEOWNER].value_counts()[True] ) )
    util.report_elapsed_time( prefix='util.IS_HOMEOWNER done: ', start_time=t )


    t = time.time()
    print( '' )
    print( 'util.IS_FAMILY starting at', time.strftime( '%H:%M:%S', time.localtime( t ) ) )

    df[util.IS_FAMILY] = df.apply( lambda row: is_family( row, df ), axis=1 )

    print( 'Found {0} family members'.format( df[util.IS_FAMILY].value_counts()[True] ) )
    util.report_elapsed_time( prefix='util.IS_FAMILY done: ', start_time=t )
    print( '' )

    return df


def report_duplicate_res_ids( df, verbose=False ):

    vc = df[util.RESIDENT_ID].value_counts()
    dups = vc[ vc > 1 ]
    dups = sorted( dups.index.tolist() )

    print( '' )
    print( 'Found {0} duplicated Resident IDs'.format( len( dups ) ) )
    if verbose:
        print( dups )

        for res_id in sorted( dups ):
            print( '' )
            print( res_id + ':' )
            print( '' )
            df_res = df[ df[util.RESIDENT_ID] == res_id ]
            df_res = df_res[ df_res.columns.intersection( [ util.RESIDENT_ID, util.PARCEL_ID, util.METER_NUMBER, SOLAR_ID ] ) ]
            print( df_res )

    print( '' )

    return dups


def write_merge_to_database( df, table_name ):

    # Sort on Resident ID and create id column
    df = df.sort_values( by=[util.RESIDENT_ID, WATER_CUSTOMER] )
    df = df.reset_index( drop=True )
    df[util.ID] = pd.Series( range( 1, len( df ) + 1 ) ).astype( int )

    # Fabricate missing columns
    for col_name in db_col_names:
        if col_name not in df.columns:
            print( 'Adding column {0} to table {1}'.format( col_name, table_name ) )
            df[col_name] = ''

    df = df[ db_col_names ]

    # Save result to database
    df.to_sql( table_name, conn, index=False, if_exists='replace' )
    df.to_csv( '../test/' + table_name + '.csv', index=False )

    return df


# Create address-to-zone lookup table
def make_zone_lookup_table():

    # Start with full Assessment table
    df_zones = df_assessment.copy()

    # Remove missing zones and duplicate addresses
    df_zones = df_zones.dropna( subset=[util.ZONING_CODE_1] )
    df_zones = df_zones.drop_duplicates( subset=[ADDR, util.LADDR_ALT_STREET_NUMBER] )

    # Split normalized address into street number and name
    df_zones[STREET_NUMBER] = df_zones.apply( lambda row: get_street_number( row, ADDR ), axis=1 )
    df_zones[STREET_NAME] = df_zones.apply( lambda row: get_street_name( row, ADDR ), axis=1 )

    # Isolate columns to be saved
    df_zones = df_zones[ [STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER, STREET_NAME, util.ZONING_CODE_1] ]

    # Sort on street name
    df_zones = df_zones.sort_values( by=[STREET_NAME, STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER] )

    # Create table in database
    util.create_table( 'ZoneLookup', conn, cur, df=df_zones )

    return df_zones


#############

# Main program
if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Correlate data in different tables' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-d', dest='debug', action='store_true', help='Include debug columns in lookup table?' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read tables from database
    df_census = pd.read_sql_table( 'Census', engine, index_col=util.ID, parse_dates=True )
    df_assessment = pd.read_sql_table( 'Assessment', engine, index_col=util.ID, parse_dates=True ).append( pd.read_sql_table( 'AssessmentAddendum', engine, index_col=util.ID, parse_dates=True ), ignore_index=True, sort=True  )
    df_water = pd.read_sql_table( 'Water', engine, index_col=util.ID, parse_dates=True )
    df_solar = pd.read_sql_table( 'Solar', engine, parse_dates=True )


    ##########################
    # Prepare tables for merge
    ##########################

    #
    # Census table
    #

    # Set up columns for merge
    df_census[util.RADDR_STREET_NUMBER] = df_census[util.RADDR_STREET_NUMBER].fillna('').astype(str)
    df_census[util.RADDR_STREET_NAME] = df_census[util.RADDR_STREET_NAME].fillna('').astype(str)
    df_census[ADDR] = df_census[util.RADDR_STREET_NUMBER] + ' ' + df_census[util.RADDR_STREET_NAME]
    df_census[ADDR] = df_census.apply( lambda row: normalize_address( row, ADDR ), axis=1 )
    df_census[APT_NUM] = df_census[util.RADDR_APARTMENT_NUMBER].fillna('').astype(str)

    # Select columns for database
    df_census = df_census[ [ ADDR, APT_NUM, util.RESIDENT_ID, util.LAST_NAME, util.FIRST_NAME, util.MIDDLE_NAME, util.RADDR_STREET_NUMBER, util.RADDR_STREET_NAME, util.RADDR_STREET_NUMBER_SUFFIX ] ]


    #
    # Assessment table
    #

    # Set up columns for merge
    df_assessment[util.LADDR_STREET_NUMBER] = df_assessment[util.LADDR_STREET_NUMBER].fillna('').astype(str)
    df_assessment[util.LADDR_STREET_NAME] = df_assessment[util.LADDR_STREET_NAME].fillna('').astype(str)
    df_assessment[ADDR] = df_assessment[util.LADDR_STREET_NUMBER] + ' ' + df_assessment[util.LADDR_STREET_NAME]
    df_assessment[ADDR] = df_assessment.apply( lambda row: normalize_address( row, ADDR ), axis=1 )
    df_assessment[APT_NUM] = df_assessment[util.LADDR_CONDO_UNIT].fillna('').astype(str)

    # Select columns for database
    df_assessment = df_assessment[ [ ADDR, APT_NUM, util.PARCEL_ID, util.OWNER_1_NAME, util.OWNER_2_NAME, util.OWNER_3_NAME, util.OWNER_OCCUPIED, util.LADDR_STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER, util.LADDR_STREET_NAME, util.ZONING_CODE_1 ] ]

    # Remove business properties
    df_assessment_res = df_assessment.loc[ ( df_assessment[util.OWNER_OCCUPIED] == util.YES ) | ( df_assessment[util.OWNER_OCCUPIED] == util.NO ) ]


    #
    # Water table
    #

    # Extract most recent entries
    df_water = df_water.sort_values( by=[util.CURRENT_DATE] )
    df_water = df_water.drop_duplicates( subset=[util.METER_NUMBER], keep='last' )

    # Set up columns for merge
    df_water[util.ADDR_STREET_NUMBER] = df_water[util.ADDR_STREET_NUMBER].fillna('').astype(str).str.strip()
    df_water[util.ADDR_STREET_NAME] = df_water[util.ADDR_STREET_NAME].fillna('').astype(str).str.strip()
    df_water[ADDR] = df_water[util.ADDR_STREET_NUMBER] + ' ' + df_water[util.ADDR_STREET_NAME]
    df_water[ADDR] = df_water.apply( lambda row: normalize_address( row, ADDR ), axis=1 )
    df_water[util.FIRST_NAME] = df_water[util.FIRST_NAME].fillna('').astype(str).str.strip()
    df_water[util.LAST_NAME] = df_water[util.LAST_NAME].fillna('').astype(str).str.strip()

    # Select columns for database
    df_water = df_water[ [ ADDR, util.METER_NUMBER, util.SERVICE_TYPE, util.FIRST_NAME, util.LAST_NAME ] ]


    #
    # Solar table
    #

    # Set up columns for merge
    df_solar = df_solar.rename( columns={ util.SITE_ADDRESS: ADDR, util.ID: SOLAR_ID } )
    df_solar[SOLAR_ID] = df_solar[SOLAR_ID].fillna('').astype(str)
    df_solar[ADDR] = df_solar[ADDR].fillna('').astype(str)
    df_solar[ADDR] = df_solar.apply( lambda row: normalize_address( row, ADDR ), axis=1 )

    # Select columns for database
    df_solar = df_solar[ [ ADDR, SOLAR_ID ] ]


    #
    # Build list of columns to be saved in database
    #

    db_col_names = [ util.ID, util.RESIDENT_ID, util.PARCEL_ID, util.METER_NUMBER, SOLAR_ID, STREET_NUMBER, util.LADDR_ALT_STREET_NUMBER, STREET_NAME, WATER_CUSTOMER, util.IS_HOMEOWNER, util.IS_FAMILY ]
    if args.debug:
        db_col_names += [ util.SERVICE_TYPE, util.RADDR_STREET_NUMBER_SUFFIX, APT_NUM, util.LAST_NAME, util.FIRST_NAME, util.MIDDLE_NAME, util.OWNER_1_NAME, util.OWNER_2_NAME, util.OWNER_3_NAME, util.OWNER_OCCUPIED ]

    print( '' )
    print( 'Census table has {0} rows'.format( len( df_census ) ) )
    print( 'Assessment table has {0} rows'.format( len( df_assessment_res ) ) )
    print( 'Water table has {0} rows'.format( len( df_water ) ) )
    print( 'Solar table has {0} rows'.format( len( df_solar ) ) )


    ##################
    # Merge dataframes
    ##################

    # Merge Census and Assessment
    df_merge = pd.merge( df_census, df_assessment_res, how='left' )
    df_merge = flag_ownership( df_merge )
    df_merge = drop_assessment_noise( df_merge )
    print( '' )
    print( 'Census + Assessment:', len( df_merge ), 'rows' )
    print( 'Resident ID unique values in merge:', len( df_merge[util.RESIDENT_ID].unique() ) )
    print( 'Parcel ID unique values in merge:', len( df_merge[util.PARCEL_ID].unique() ) )
    report_duplicate_res_ids( df_merge )

    # Add Water to merge
    df_merge = pd.merge( df_merge, df_water, how='left' )
    df_merge = drop_water_noise( df_merge )
    print( '' )
    print( 'Census + Assessment + Water:', len( df_merge ), 'rows' )
    print( 'Resident ID unique values in merge:', len( df_merge[util.RESIDENT_ID].unique() ) )
    print( 'Parcel ID unique values in merge:', len( df_merge[util.PARCEL_ID].unique() ) )
    print( 'Meter Number unique values in merge:', len( df_merge[util.METER_NUMBER].unique() ) )
    report_duplicate_res_ids( df_merge )

    # Add Solar to merge
    df_merge = pd.merge( df_merge, df_solar, how='left' )
    df_merge[SOLAR_ID] = df_merge[SOLAR_ID].fillna('').astype(str)
    print( '' )
    print( 'Census + Assessment + Water + Solar:', len( df_merge ), 'rows' )
    print( 'Resident ID unique values in merge:', len( df_merge[util.RESIDENT_ID].unique() ) )
    print( 'Parcel ID unique values in merge:', len( df_merge[util.PARCEL_ID].unique() ) )
    print( 'Meter Number unique values in merge:', len( df_merge[util.METER_NUMBER].unique() ) )
    print( 'Solar ID unique values in merge:', len( df_merge[SOLAR_ID].unique() ) )
    report_duplicate_res_ids( df_merge )

    # Insert non-resident water Meter Numbers
    df_merge = insert_nonresident_water( df_merge )
    print( '' )
    print( 'Census + Assessment + Water + Solar + Non-Resident Water:', len( df_merge ), 'rows' )
    print( 'Resident ID unique values in merge:', len( df_merge[util.RESIDENT_ID].unique() ) )
    print( 'Parcel ID unique values in merge:', len( df_merge[util.PARCEL_ID].unique() ) )
    print( 'Meter Number unique values in merge:', len( df_merge[util.METER_NUMBER].unique() ) )
    print( 'Solar ID unique values in merge:', len( df_merge[SOLAR_ID].unique() ) )
    report_duplicate_res_ids( df_merge )

    # Split normalized address into number and street
    df_merge[STREET_NUMBER] = df_merge.apply( lambda row: get_street_number( row, ADDR ), axis=1 )
    df_merge[STREET_NAME] = df_merge.apply( lambda row: get_street_name( row, ADDR ), axis=1 )

    # Fill empty alt-street-number fields
    df_merge[util.LADDR_ALT_STREET_NUMBER] = df_merge[util.LADDR_ALT_STREET_NUMBER].fillna('').astype(str)

    # Save in database
    df_merge = write_merge_to_database( df_merge, 'Lookup' )

    # Save building zone lookup table
    df_zones = make_zone_lookup_table()

    # Report elapsed time
    util.report_elapsed_time()
