# Copyright 2020 Energize Andover.  All rights reserved.

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re
import usaddress
import copy
import collections



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
def normalize_address( row, col_name, city=' XXXXXXXXX', verbose=False ):

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


    if address != '':

        try:
            trailing_address_parts = city + ' XX 00000'
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

    return address
