# Copyright 2020 Energize Andover.  All rights reserved.

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import re
import usaddress
import copy
import collections

import util



# Based on USPS guidelines: https://pe.usps.com/text/pub28/28apc_002.htm
STREET_TYPES = \
{
    'AV': 'AVE',
    'AVE.': 'AVE',
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
    'PATH': 'PATH',
    'PD': 'PD',
    'PH': 'PATH',
    'PIKE': 'PIKE',
    'PINES': 'PNES',
    'PARKWAY': 'PKWY',
    'PK': 'PARK',
    'PKWY': 'PKWY',
    'PLACE': 'PL',
    'RIDGE': 'RDG',
    'RIVER': 'RIV',
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
    'TURNPIKE': 'TPKE',
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
    'S.': 'S',
    'SO': 'S',
    'SO.': 'S',
    'SOUTH': 'S',
    '(SOUTH)': 'S',
    'E.': 'E',
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
    'StreetNamePreModifier',
    'LandmarkName',
}


# Help usaddress parsing algorithm with troublesome address inputs
def fix_inputs_we_dont_like( address, return_parts, verbose ):

    # Miscellaneous typos
    address = re.sub( r' CI$', ' CIR', address )
    address = address.replace( ' CI ', ' CIR ' )
    address = re.sub( r' UNION$', ' UNION ST', address )
    address = re.sub( r' ST ST ', ' ST ', address )
    address = re.sub( r' T$', ' ST', address )
    address = re.sub( r' APT FRT ', ' ', address )
    address = re.sub( r' BROADWAY ST[A-Z]*$', ' BROADWAY ', address )
    address = re.sub( r' AB FARNHAM ', ' A-B FARNHAM ', address )
    address = re.sub( r'18 FRANKLIN-45 BROADWAY', '18 FRANKLIN ST (45 BROADWAY)', address )
    address = re.sub( r' \d+([A-Z][A-Z])? FL(OOR)? ', ' ', address )
    address = address.replace( ' BERNNINGTON ', ' BENNINGTON ' )
    address = re.sub( r'22 ?- ?24 PLEASANT$', '22-24 PLEASANT TER', address )
    address = re.sub( r'^5-7- ARLINGTON TERR$', '5-7 ARLINGTON TER', address )
    address = re.sub( r'(\d+) W ST', r'\1 WEST ST', address )

    # Remove spaces around hyphens
    address = re.sub( r' ?- ?', '-', address )

    # Move nonconforming informational text to trailing, parenthesized string
    if ' AKA ' in address:
        address_parts = address.split( ' AKA ', 1 )
        address = address_parts[0].strip()
        address = address + ' (AKA ' + address_parts[1].strip() + ')'
    if address.startswith( 'REAR ' ):
        address = re.sub( r'^REAR ', '', address )
        address = address.strip() + ' (REAR)'

    # Optionally extract and remove parenthesized text
    if return_parts:
        address_parts = address.split( '(', 1 )
        address = address_parts[0]
        additional_info = '(' + address_parts[1] if len( address_parts ) == 2 else None
    else:
        additional_info = None

    # Handle hyphens
    bf_address = address
    if re.search( '^\d+[A-Z]?\d*(-\d+[A-Z]?\d*)+ ', address ):
        # Replace dashes following hyphenated address number
        address_parts = address.split( ' ', 1 )
        address_parts[1] = address_parts[1].replace( '-', ' ' )
        address = ' '.join( address_parts )
    elif re.search( '^\d+ [A-Z]-[A-Z] ', address ):
        # Replace dashes following hyphenated unit letters
        address_parts = address.split( ' ', 2 )
        address_parts[2] = address_parts[2].replace( '-', ' ' )
        address = ' '.join( address_parts )
    elif re.search( '^\d+-[A-Z] ', address ):
        # Remove dash between initial number and single letter
        address_parts = address.split( '-', 1 )
        address = ''.join( address_parts )
    elif re.search( '^\d+-[A-Z]', address ):
        # Replace dash following initial number
        address_parts = address.split( '-', 1 )
        address = ' '.join( address_parts )
    elif re.search( '^\d+[A-Z]-[A-Z] ', address ):
        # Add space to unit letter range and replace subsequent dashes
        address = re.sub( r'(\d+)([A-Z]{1,2})', r'\1 \2', address )
        address_parts = address.split( ' ', 2 )
        address_parts[2] = address_parts[2].replace( '-', ' ' )
        address = ' '.join( address_parts )
    elif not re.search( '\d+-\d+', address ):
        # Replace dashes in entire address if it does not contain <number>-<number>
        address = address.replace( '-', ' ' )

    if verbose:
        if bf_address != address:
            print( 'Hyphen handling, before and after: <{}>, <{}>'.format( bf_address, address ) )

    address = address.strip()

    return address, additional_info


# Modify parsing results according to our liking
def fix_outputs_we_dont_like( parts, city, return_parts, verbose ):

    keys = parts.keys()

    # Correct parsing mistakes that occur, for example, with 'GRANDVIEW TR'
    if ( 'Recipient' in parts ) and ( 'StreetName' not in parts ) and ( 'StreetNamePostType' not in parts ):
        if verbose:
            print( 'Bf replacing Recipient', parts )
        od = collections.OrderedDict()
        for key in keys:
            if key == 'Recipient':
                words = parts['Recipient'].split()
                od['StreetName'] = ' '.join( words[:-1] )
                od['StreetNamePostType'] = words[-1]
            else:
                od[key] = parts[key]
        parts = od
        if verbose:
            print( 'Af replacing Recipient', parts )

    # Handle highway location, such as 'I 495', that has been designated as PO Box
    elif ( 'USPSBoxType' in keys ) and ( 'StreetName' not in keys ) and not re.match( r'^P\.?O\.? ', parts['USPSBoxType'] ) and ( 'USPSBoxID' in keys ):
        if verbose:
            print( 'Bf moving USPSBox* fields to StreetName', parts )
        parts['StreetName'] = ' '.join( [ parts['USPSBoxType'], parts['USPSBoxID'] ] )
        del parts['USPSBoxType']
        del parts['USPSBoxID']
        parts.move_to_end( 'StreetName', last=False )
        if verbose:
            print( 'Af moving USPSBox* fields to StreetName', parts )

    # Handle post-directional-like name, such as 'SOUTH ST', that has been designated as post-directional
    elif ( 'StreetNamePostDirectional' in keys ) and ( 'PlaceName' in keys ) and ( 'StateName' in keys ) and ( 'ZipCode' in keys ):
        if verbose:
            print( 'Bf moving StreetNamePostDirectional to StreetName', parts )
        if ( len( keys ) == 4 ) or ( ( len( keys ) == 5 ) and ( 'AddressNumber' in keys ) ):
            parts['StreetName'] = parts['StreetNamePostDirectional']
            del parts['StreetNamePostDirectional']
            parts.move_to_end( 'StreetName', last=False )
            if re.match( r'^[A-Z]+ +{}$'.format( city ), parts['PlaceName'] ):
                parts['StreetName'] = ' '.join( [parts['StreetName'], parts['PlaceName'].split()[0] ] )
                parts['PlaceName'] = city
            if 'AddressNumber' in keys:
                parts.move_to_end( 'AddressNumber', last=False )
        if verbose:
            print( 'Af moving StreetNamePostDirectional to StreetName', parts )

    # Handle street name, such as 'BROADWAY', that has been prepended to PlaceName field
    elif ( 'PlaceName' in keys ) and re.match( r'^[A-Z]+ +{}$'.format( city ), parts['PlaceName'] ) and ( 'StateName' in keys ) and ( 'ZipCode' in keys ) and ( len( keys ) == 3 ):
        if verbose:
            print( 'Bf moving PlaceName fragment to StreetName', parts )
        parts['StreetName'] = parts['PlaceName'].split()[0]
        parts['PlaceName'] = city
        parts.move_to_end( 'StreetName', last=False )
        if verbose:
            print( 'Af moving PlaceName fragment to StreetName', parts )

    # Handle street name and type, such as 'MELROSE TERR', that has been designated as landmark
    elif ( 'LandmarkName' in keys ) and ( 'PlaceName' in keys ) and ( 'StateName' in keys ) and ( 'ZipCode' in keys ) and ( len( keys ) == 4 ) and ( len( parts['LandmarkName'].split() ) > 1 ):
        if verbose:
            print( 'Bf moving LandmarkName fragments to StreetName and StreetNamePostType', parts )
        words = parts['LandmarkName'].split()
        parts['StreetNamePostType'] = words.pop()
        parts['StreetName'] = ' '.join( words )
        del parts['LandmarkName']
        parts.move_to_end( 'StreetNamePostType', last=False )
        parts.move_to_end( 'StreetName', last=False )
        if verbose:
            print( 'Af moving LandmarkName fragments to StreetName and StreetNamePostType', parts )

    # Handle hyphenated letter range following address number, such as '206 A-B PARK ST'
    elif ( 'AddressNumber' in keys ) and ( 'StreetName' in keys ) and re.search( '^\d+$', parts['AddressNumber'] ) and re.search( '^[A-Z]-[A-Z] ', parts['StreetName'] ):
        if verbose:
            print( 'Bf moving occupancy letter range from StreetName to AddressNumber', parts )
        words = parts['StreetName'].split()
        parts['AddressNumber'] = ' '.join( [parts['AddressNumber'], words.pop(0)] )
        parts['StreetName'] = ' '.join( words )
        if verbose:
            print( 'Af moving occupancy letter range from StreetName to AddressNumber', parts )

    # Handle single letter following address number, such as '2 D WOODLAND ST'
    elif ( 'AddressNumber' in keys ) and ( 'StreetName' in keys ) and re.search( '^\d+$', parts['AddressNumber'] ) and re.search( '^[A-Z] ', parts['StreetName'] ):
        words = parts['StreetName'].split()
        # Make sure it's not a misplaced pre-directional
        if words[0] not in DIRS.values():
            if verbose:
                print( 'Bf moving occupancy letter from StateName to AddressNumber', parts )
            parts['AddressNumber'] = ''.join( [parts['AddressNumber'], words.pop(0)] )
            parts['StreetName'] = ' '.join( words )
            if verbose:
                print( 'Af moving occupancy letter from StateName to AddressNumber', parts )

    # Handle single letter, identified as a suffix, following purely numeric address number, such as '251 C FARNHAM ST'
    elif return_parts and ( 'AddressNumber' in keys ) and ( 'AddressNumberSuffix' in keys ) and parts['AddressNumber'].isdigit() and re.search( '^[A-Z]$', parts['AddressNumberSuffix'] ):
        if verbose:
            print( 'Bf moving AddressNumberSuffix to AddressNumber', parts )
        parts['AddressNumber'] = ''.join( [ parts['AddressNumber'], parts['AddressNumberSuffix'] ] )
        del parts['AddressNumberSuffix']
        if verbose:
            print( 'Af moving AddressNumberSuffix to AddressNumber', parts )

    # Optionally remove OccupancyType
    if return_parts and ( 'OccupancyType' in keys ):
        del parts['OccupancyType']

    # Optionally clean up OccupancyIdentifier
    if return_parts and ( 'OccupancyIdentifier' in keys ):
        if verbose:
            print( 'Bf normalizing OccupancyIdentifier', parts )
        if parts['OccupancyIdentifier'] in ['1ST', '2ND', '3RD', '4TH']:
            parts['OccupancyIdentifier'] = parts['OccupancyIdentifier'][0]
        elif parts['OccupancyIdentifier'].startswith( 'UNIT' ):
            parts['OccupancyIdentifier'] = re.sub( r'^UNIT', '', parts['OccupancyIdentifier'] ).strip()
        elif parts['OccupancyIdentifier'].endswith( 'FLR' ):
            parts['OccupancyIdentifier'] = re.sub( r'FLR$', '', parts['OccupancyIdentifier'] ).strip()
        elif parts['OccupancyIdentifier'].endswith( 'FL' ):
            parts['OccupancyIdentifier'] = re.sub( r'FL$', '', parts['OccupancyIdentifier'] ).strip()
        elif parts['OccupancyIdentifier'].find( '#' ) >= 0:
            parts['OccupancyIdentifier'] = parts['OccupancyIdentifier'].split( '#' )[-1].strip()
        elif ( re.search( r'^\d+ [A-Z]$', parts['OccupancyIdentifier'] ) or re.search( '^[A-Z] \d+$', parts['OccupancyIdentifier'] ) ):
            parts['OccupancyIdentifier'] = ''.join( parts['OccupancyIdentifier'].split() )
        if verbose:
            print( 'Af normalizing OccupancyIdentifier', parts )
            if not ( re.search( r'^\d+[A-Z]*( [NSEW])?$', parts['OccupancyIdentifier'] ) or re.search( '^[A-Z]+\d*( [NSEW])?$', parts['OccupancyIdentifier'] ) or re.search( '^\d+-\d+$', parts['OccupancyIdentifier'] ) ):
                print( 'OccupancyIdentifier not fixed <{}>'.format( parts['OccupancyIdentifier'] ) )

    return parts


# Normalize street address
def normalize_address( row, col_name, city='ANDOVER', return_parts=False, verbose=False ):

    # Create original copy of the address
    original = row[col_name]

    # Initialize return value
    address = original.strip().upper()

    if verbose:
        print( '' )
        print( 'Normalizing address in column "{0}": "{1}"'.format( col_name, address ) )

    # Help usaddress parsing algorithm with troublesome address inputs
    address, additional_info = fix_inputs_we_dont_like( address, return_parts, verbose )

    parts = {}

    if address != '':

        try:
            trailing_address_parts = ( ( ' ' + city ) if city else '' )+ ' XX 00000'
            norm = usaddress.tag( address + trailing_address_parts )

            if len( norm ) and isinstance( norm[0], dict ):

                parts = copy.deepcopy( norm[0] )

                # Fix parsing results we don't like
                parts = fix_outputs_we_dont_like( parts, city, return_parts, verbose )

                keys = parts.keys()

                if verbose:
                    print( 'Parser output:' )
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
                    elif re.match( r'^\d+[A-Z]$', pre_dir ):
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

                # Package final results
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
                    print( '<{}> expected to end with <{}>'.format( s_new, trailing_address_parts ) )
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
            util.NORMALIZED_OCCUPANCY: occupancy,
            util.NORMALIZED_ADDITIONAL_INFO: additional_info
        }

    else:
        return_value = address

    return return_value

