# Copyright 2020 Energize Andover.  All rights reserved.

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re
import usaddress
import copy
import collections

import util


ADDITIONAL_ADDRESS_INFO = 'additional_address_info'

# Prepare address column for normalization
def prepare_to_normalize( df, original_address_column_name, prepared_address_column_name, additional_info_column_name=ADDITIONAL_ADDRESS_INFO ):

    paren_regex = '\(.*\)?'

    # Optionally preserve text that will be excluded from the address data prepared for normalization
    if additional_info_column_name:

        # Attempt to extract text
        df_extracted = df[original_address_column_name].str.extractall( r'(' + paren_regex + ')' )

        # If text was extracted, save results in the specified column
        if df_extracted.first_valid_index() is not None:

            # Initialize column in which to save extracted text
            df[additional_info_column_name] = None

            # Iterate over extracted text
            idx_row = 0
            for index, row in df_extracted.iterrows():
                if idx_row != index[0]:
                    # Initialize string of extracted text
                    idx_row = index[0]
                    df.at[idx_row, additional_info_column_name] = df_extracted.loc[index][0]
                else:
                    # Append to string of extracted text
                    df.at[idx_row, additional_info_column_name] += ' ' + df_extracted.loc[index][0]

    # Generate clean address data that can be supplied to the normalize utility
    # - Strip parenthesized text
    # - Strip leading and trailing whitespace
    # - Convert to uppercase
    df[prepared_address_column_name] = df[original_address_column_name].str.replace( r'' + paren_regex + '', '', regex=True ).str.strip().str.upper()

    return df

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
    'HILL': 'HL',
    'HOLLOW': 'HOLW',
    'KNOLL': 'KNL',
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
    'VIEW': 'VW',
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
    '(NORTH)': 'N',
    'SO': 'S',
    'SOUTH': 'S',
    '(SOUTH)': 'S',
    'EAST': 'E',
    'WEST': 'W',
    'SOUTHWEST': 'SW',
}

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
    'OccupancyType',
    'StreetNamePreType',
}

# Normalize street address
def normalize_address( row, col_name, city='ANDOVER', return_parts=False, verbose=False ):

    # Create original copy of the address
    original = row[col_name]

    # Initialize return value
    address = original.strip()

    # Help usaddress parsing algorithm with these troublesome cases
    address = re.sub( r' CI$', ' CIR', address )
    address = address.replace( ' CI ', ' CIR ' )
    address = address.replace( '-', ' ' )

    if verbose:
        print( '' )
        print( 'Normalizing address in column "{0}": "{1}"'.format( col_name, address ) )

    parts = {}

    if address != '':

        try:
            trailing_address_parts = ' ' + city + ' XX 00000'
            norm = usaddress.tag( address + trailing_address_parts )

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
                    elif re.match( r'^\d+[a-zA-Z]$', pre_dir ):
                        parts['AddressNumber'] += '-' + pre_dir
                        parts['StreetNamePreDirectional'] = ''
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

            if s_new.endswith( trailing_address_parts ):
                address = s_new[ :-len( trailing_address_parts ) ]
            else:
                print( 'BAD ENDING!!!' )
                exit()

        except usaddress.RepeatedLabelError:

            if verbose:
                print( 'Exception: usaddress.RepeatedLabelError' )

    if verbose:
        if address != original:
            print( 'Address mapped from "{0}" to "{1}"'.format( original, address ) )

    if return_parts:

        if 'StreetName' in parts and re.match( r'^\d+[a-zA-Z]? ', parts['StreetName'] ) :
            # Fix two-number address, where second number is attached to street name instead of number
            street_split = parts['StreetName'].split()

            # Append second number where it belongs
            if not 'AddressNumber' in parts:
                parts['AddressNumber'] = street_split[0]
            else:
                parts['AddressNumber'] += ' ' + street_split[0]

            # Remove second number from where it doesn't belong
            parts['StreetName'] = ' '.join( street_split[1:] )

        number = parts['AddressNumber'] if 'AddressNumber' in parts else ''

        street_parts = []
        if 'StreetNamePreDirectional' in parts:
            street_parts.append( parts['StreetNamePreDirectional'] )
        if 'StreetName' in parts:
            street_parts.append( parts['StreetName'] )
        if 'StreetNamePostType' in parts:
            street_parts.append( parts['StreetNamePostType'] )
        street = ' '.join( street_parts )

        occupancy_parts = []
        if 'OccupancyType' in parts:
            occupancy_parts.append( parts['OccupancyType'] )
        if 'OccupancyIdentifier' in parts:
            occupancy_parts.append( parts['OccupancyIdentifier'] )
        occupancy = ' '.join( occupancy_parts )

        return_value = \
        {
            util.NORMALIZED_ADDRESS: address,
            util.NORMALIZED_STREET_NUMBER: number,
            util.NORMALIZED_STREET_NAME: street,
            util.NORMALIZED_OCCUPANCY: occupancy
        }

    else:
        return_value = address

    return return_value
