# Copyright 2025 Energize Andover.  All rights reserved.

B_DEBUG = False
# B_DEBUG = True

import argparse
import os

import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import simplekml
import xml.etree.ElementTree as ET

import sys
sys.path.append( '../util' )
import util


# Nicknames
ADDR = util.NORMALIZED_ADDRESS
STREET_NAME = util.NORMALIZED_STREET_NAME
IS_RES = util.IS_RESIDENTIAL
FUEL = util.HEATING_FUEL_DESC
WARD = util.WARD_NUMBER
LINK = util.VISION_LINK
LAT = util.LATITUDE
LONG = util.LONGITUDE
ELEC = util.ELECTRIC
OIL = util.OIL
GAS = util.GAS
YES = util.YES
LEAN_ELIG = util.LEAN_ELIGIBILITY
LEAN = util.LEAN
LMF = util.LEAN_MULTI_FAMILY
WX = util.WX
NWX = 'n' + WX
COLOR = util.COLOR
ICON = util.ICON
OCC = util.TOTAL_OCCUPANCY
WX_PERMIT = util.WX_PERMIT
IS_RENTAL = 'is_rental'
WARDS=util.LAWRENCE_WARDS
STYLEMAP = 'stylemap'

RES = 'res'
RENT = 'rent'

HOUSE_TYPES = [LEAN, LMF, RES, RENT]
FUELS = [ELEC, OIL, GAS]

FILTER = 'filter'
DOCS = 'docs'

KML_NAMESPACE = util.KML_NAMESPACE

KML = 'kml'
KML_LIST = 'kml_list'
XML = 'xml'
XML_LIST = 'xml_list'
PARCEL_COUNT = 'parcel_count'
UNIT_COUNT = 'unit_count'

# Field values to be used in generating folders
FOLDER_HOUSE_TYPES = [RES, RENT]
FOLDER_WX_TYPES = [WX, NWX]

# Dictionary of folders
DC_FOLDERS = {}

# Dictionary of trees
DC_TREES = {}
TREE_DEBUG = 'tree_debug'


#
# ==> Dictionary of documents ==>
#

# Each entry corresponds to three KML output files
DC_DOCUMENTS = \
{
    f'{LEAN}_{ELEC}':
    {
        FILTER:
        {
            IS_RES: [YES],
            LEAN_ELIG: [LEAN],
            FUEL: [ELEC],
        },
    },
    f'{LEAN}_{OIL}':
    {
        FILTER:
        {
            IS_RES: [YES],
            LEAN_ELIG: [LEAN],
            FUEL: [OIL],
        },
    },
    f'{LEAN}_{GAS}':
    {
        FILTER:
        {
            IS_RES: [YES],
            LEAN_ELIG: [LEAN],
            FUEL: [GAS],
        },
    },
    f'{LMF}_{ELEC}':
    {
        FILTER:
        {
            IS_RES: [YES],
            LEAN_ELIG: [LMF],
            FUEL: [ELEC],
        },
    },
    f'{LMF}_{OIL}':
    {
        FILTER:
        {
            IS_RES: [YES],
            LEAN_ELIG: [LMF],
            FUEL: [OIL],
        },
    },
    f'{LMF}_{GAS}':
    {
        FILTER:
        {
            IS_RES: [YES],
            LEAN_ELIG: [LMF],
            FUEL: [GAS],
        },
    },
    f'{RES}_{ELEC}':
    {
        FILTER:
        {
            IS_RES: [YES],
            FUEL: [ELEC],
        },
    },
    f'{RES}_{OIL}':
    {
        FILTER:
        {
            IS_RES: [YES],
            FUEL: [OIL],
        },
    },
    f'{RES}_{GAS}':
    {
        FILTER:
        {
            IS_RES: [YES],
            FUEL: [GAS],
        },
    },
    f'{RENT}_{ELEC}':
    {
        FILTER:
        {
            IS_RES: [YES],
            IS_RENTAL: [True],
            FUEL: [ELEC],
        },
    },
    f'{RENT}_{OIL}':
    {
        FILTER:
        {
            IS_RES: [YES],
            IS_RENTAL: [True],
            FUEL: [OIL],
        },
    },
    f'{RENT}_{GAS}':
    {
        FILTER:
        {
            IS_RES: [YES],
            IS_RENTAL: [True],
            FUEL: [GAS],
        },
    },
}

# Add ward-specific filters for all house types

# --> Scaled-down scope for debugging -->
if B_DEBUG:
    DC_DOCUMENTS = \
    {
    }

    WARDS = WARDS[:2]
# <-- Scaled-down scope for debugging <--

for ward in WARDS:
    for fuel in FUELS:
        DC_DOCUMENTS[f'{ward}_{LEAN}_{fuel}'.lower()] = \
        {
            FILTER:
            {
                WARD: [ward],
                IS_RES: [YES],
                LEAN_ELIG: [LEAN],
                FUEL: [fuel],
            },
        }

for ward in WARDS:
    for fuel in FUELS:
        DC_DOCUMENTS[f'{ward}_{LMF}_{fuel}'.lower()] = \
        {
            FILTER:
            {
                WARD: [ward],
                IS_RES: [YES],
                LEAN_ELIG: [LMF],
                FUEL: [fuel],
            },
        }

for ward in WARDS:
    for fuel in FUELS:
        DC_DOCUMENTS[f'{ward}_{RES}_{fuel}'.lower()] = \
        {
            FILTER:
            {
                WARD: [ward],
                IS_RES: [YES],
                FUEL: [fuel],
            },
        }

for ward in WARDS:
    for fuel in FUELS:
        DC_DOCUMENTS[f'{ward}_{RENT}_{fuel}'.lower()] = \
        {
            FILTER:
            {
                WARD: [ward],
                IS_RES: [YES],
                IS_RENTAL: [True],
                FUEL: [fuel],
            },
        }


# Secondary filtering based on weatherization status
WX_FILTERS = \
[
    '',     # No filter
    WX,     # Weatherized
    NWX,    # Not weatherized
]

# Initialize structure to save information as documents are generated
for s_key in DC_DOCUMENTS:
    DC_DOCUMENTS[s_key][DOCS] = {}
    for s_wx in WX_FILTERS:
        s_label = s_key
        if s_wx:
            s_label += '_' + s_wx
        DC_DOCUMENTS[s_key][DOCS][s_label.lower()] = \
        {
            KML: None,
            PARCEL_COUNT: 0,
            UNIT_COUNT: 0,
        }

#
# <== Dictionary of documents <==
#

# KML rendering attributes
KML_MAP = util.KML_MAP

######################


def make_styles():

    df_styles = pd.DataFrame()

    for s_ward in KML_MAP[COLOR]:
        for s_fuel in KML_MAP[ICON]:

            # Extract mapped placemark attributes
            s_color = KML_MAP[COLOR][s_ward]
            icon_colors = s_color.split('|')
            if len( icon_colors ) == 1:
                icon_colors.append( icon_colors[0] )

            s_icon = KML_MAP[ICON][s_fuel]
            shape_scale = s_icon.split('|')

            # Generate normal style
            normal_style = simplekml.Style()
            normal_style.labelstyle.scale = 0
            normal_style.iconstyle.color = icon_colors[0]
            normal_style.iconstyle.icon.href = 'https://maps.google.com/mapfiles/kml/shapes/' + shape_scale[0] + '.png'
            normal_style.iconstyle.scale = shape_scale[1]

            # Generate highlight style for mouseover
            highlight_style = simplekml.Style()
            highlight_style.labelstyle.scale = 1.1
            highlight_style.iconstyle.color = icon_colors[1]

            # Combine normal and highlight styles in a stylemap
            style_map = simplekml.StyleMap()
            style_map.normalstyle = normal_style
            style_map.highlightstyle = highlight_style

            # Save the stylemap in the styles dataframe
            dc_row = \
            {
                WARD: s_ward,
                FUEL: s_fuel,
                STYLEMAP: style_map
            }
            df_styles = df_styles.append( dc_row, ignore_index=True )

    return df_styles


# Generate a KML file containing POIs
def make_kml_file( s_label, df, df_styles, n_parcels, n_units, output_directory ):

    # Create empty KML
    kml = simplekml.Kml()

    # Set the document name
    s_docname = make_doc_name( s_label, n_parcels, n_units )
    kml.document.name = s_docname
    doc = kml.newdocument( name=s_docname )

    for index, row in df.iterrows():

        # Create a point for this parcel
        point = doc.newpoint( name=f'{row[WARD]}: {row[ADDR]}', coords=[ ( row[LONG], row[LAT] ) ] )

        # Set link
        point.description = f'<a href="{row[LINK]}">{row[ADDR]}</a><br/>{row[FUEL]} heat'

        # Set the style map to switch between normal and highlight styles
        s_ward = row[WARD]
        s_fuel = row[FUEL]
        style_row = df_styles[ ( df_styles[WARD] == s_ward ) & ( df_styles[FUEL] == s_fuel )]

        # Assign the stylemap to the point
        style_map = simplekml.StyleMap()
        point.stylemap = style_row[STYLEMAP].values[0]

    s_doc_key = s_label.lower()
    s_filepath = os.path.join( output_directory, 'maps', f'{s_doc_key}.kml' )
    kml.save( s_filepath )

    return kml, s_doc_key


# Format delimited words in a string
def format_words( s_words, s_delimiter=' ' ):

    ls_in = s_words.split( s_delimiter )
    ls_out = []

    for s_in in ls_in:
        s_out = s_in.upper() if ( s_in.lower() in [LEAN.lower(), LMF.lower()] ) else s_in.capitalize()
        ls_out.append( s_out )

    s_out = ' '.join( ls_out )

    return s_out

# Format trailing counts of parcels and units
def format_counts( n_parcels, n_units ):
    return f' - P:{n_parcels:,} - H:{n_units:,}'

# Generate document name
def make_doc_name( s_label, n_parcels, n_units ):
    s_out = format_words( s_label, s_delimiter='_' )
    s_out += format_counts( n_parcels, n_units )
    return s_out


# Generate KML documents
def make_kml_documents( df_parcels, df_styles, output_directory ):

    print( '' )
    print( f'Generating {len( DC_DOCUMENTS ) * len( WX_FILTERS )} KML files' )
    print( '' )

    # Add rental flag column
    df_parcels[IS_RENTAL] = df_parcels[OCC] > 1

    n_files = 0

    # Generate three KMLs for each filter in the DC_DOCUMENTS table
    for s_key in DC_DOCUMENTS:

        # Generate three KMLs for current filter
        for s_wx in WX_FILTERS:

            # Start with a copy of the full parcels table
            df = df_parcels.copy()

            # Initialize the label for current KML
            s_label = s_key

            # Select rows based on current filter
            dc_filters = DC_DOCUMENTS[s_label][FILTER]
            for col in dc_filters:
                df = df[ df[col].isin( dc_filters[col] ) ]

            # Select further, based on weatherization status
            if s_wx:
                s_label += '_' + s_wx
                df = df[ df[WX_PERMIT].isnull() ] if s_wx == NWX else df[ ~df[WX_PERMIT].isnull() ]

            # Count parcels and housing units that will be represented by this KML
            n_parcels = len( df )
            n_units = df[OCC].sum()

            # Edit Vision hyperlinks encoded for Excel
            pattern = r'=HYPERLINK\("(http.*pid=\d+)".*'
            df = df.replace( to_replace=pattern, value=r'\1', regex=True )

            # Reorder columns and rows
            ls_columns = [WARD, STREET_NAME, ADDR, FUEL, LAT, LONG, LINK]
            df = df[ls_columns]
            df = df.sort_values( by=ls_columns )
            df = df.reset_index( drop=True )

            # Convert dataframe to KML
            kml, s_doc_key = make_kml_file( s_label, df, df_styles, n_parcels, n_units, output_directory )

            # Save the KML file and associated counts
            DC_DOCUMENTS[s_key][DOCS][s_doc_key][KML] = kml
            DC_DOCUMENTS[s_key][DOCS][s_doc_key][PARCEL_COUNT] = n_parcels
            DC_DOCUMENTS[s_key][DOCS][s_doc_key][UNIT_COUNT] = n_units

            # Report progress
            n_files += 1
            print( '{: >3d}: {}, "{}"'.format( n_files, s_doc_key, kml.document.name ) )

    print( '' )

    return


# Generate data structure to group documents into folders
def make_dc_folders():

    WD = 'ward'
    HS = 'house_type'
    FU = 'fuel'
    WX = 'wx'

    # Convert lists to lowercase
    lc_wd = [item.lower() for item in WARDS]
    lc_hs = [item.lower() for item in HOUSE_TYPES]
    lc_fu = [item.lower() for item in FUELS]
    lc_wx = [item.lower() for item in WX_FILTERS]

    # Select attributes required for folders
    dc_required_attrs = \
    {
        WD: lc_wd,
        HS: [item.lower() for item in FOLDER_HOUSE_TYPES],
        FU: lc_fu,
        WX: [item.lower() for item in FOLDER_WX_TYPES],
    }

    # Initialize dictionary of folders we want to populate with ward KMLs
    for hs in dc_required_attrs[HS]:
        for fu in dc_required_attrs[FU]:
            for wx in dc_required_attrs[WX]:
                s_name = f'{hs} {fu} {wx}'  # This decides the ordering of name fragments
                DC_FOLDERS[s_name] = \
                {
                    KML_LIST: [],
                    PARCEL_COUNT: 0,
                    UNIT_COUNT: 0,
                    XML: None,
                }

    # Iterate over dictionary, 3 documents per element
    for s_doc_label in DC_DOCUMENTS:

        # Iterate over 3 documents in current dictionary element
        for s_kml_label in DC_DOCUMENTS[s_doc_label][DOCS]:

            # Extract document attributes from label parts
            dc_kml_attrs = \
            {
                WD: '',
                HS: '',
                FU: '',
                WX: '',
            }

            for s_part in s_kml_label.split( '_' ):
                if s_part in lc_wd:
                    dc_kml_attrs[WD] = s_part
                elif s_part in lc_hs:
                    dc_kml_attrs[HS] = s_part
                elif s_part in lc_fu:
                    dc_kml_attrs[FU] = s_part
                elif s_part in lc_wx:
                    dc_kml_attrs[WX] = s_part

            # Determine whether current document meets folder requirements
            b_meets_req = True
            for s_key, ls_values in dc_required_attrs.items():
                b_meets_req = b_meets_req and dc_kml_attrs[s_key] in ls_values

            # If this document satisfies folder requirements, save it with the corresponding folder
            if b_meets_req:

                # Save document with parent folder
                hs = dc_kml_attrs[HS]
                fu = dc_kml_attrs[FU]
                wx = dc_kml_attrs[WX]
                s_folder_name = f'{hs} {fu} {wx}'
                kml = DC_DOCUMENTS[s_doc_label][DOCS][s_kml_label][KML]
                DC_FOLDERS[s_folder_name][KML_LIST].append( kml )
                DC_FOLDERS[s_folder_name][PARCEL_COUNT] += DC_DOCUMENTS[s_doc_label][DOCS][s_kml_label][PARCEL_COUNT]
                DC_FOLDERS[s_folder_name][UNIT_COUNT] += DC_DOCUMENTS[s_doc_label][DOCS][s_kml_label][UNIT_COUNT]

                print( f'Label "{s_doc_label}": Saving doc "{s_kml_label}" in folder "{s_folder_name}"' )

    return


# Group KML documents into KML folders
def insert_maps_in_folders( output_directory=None ):

    namespace = KML_NAMESPACE
    schema = f'{{{namespace}}}'
    ET.register_namespace( '', namespace )

    print( '' )

    # Iterate over dictionary of folders
    for s_folder in DC_FOLDERS:

        # Create new KML root
        root = ET.Element( f'{schema}kml' )

        # Convert lowercase folder string to formatted name
        s_folder_name = format_words( s_folder )

        # Append counts
        n_parcels = DC_FOLDERS[s_folder][PARCEL_COUNT]
        n_units = DC_FOLDERS[s_folder][UNIT_COUNT]
        s_folder_name += format_counts( n_parcels, n_units )

        # Initialize document to contain everything
        parent_document = ET.SubElement( root, f'{schema}Document' )
        ET.SubElement( parent_document, f'{schema}name' ).text = s_folder_name

        # Track unique style IDs
        dc_unique_styles = {}

        # Create the top-level folder
        content_folder = ET.SubElement( parent_document, f'{schema}Folder' )
        ET.SubElement( content_folder, f'{schema}name' ).text = s_folder_name

        # Initialize list to collect features ( Placemarks, Documents, etc.)
        ls_features = []

        # Iterate over source KML documents
        for kml in DC_FOLDERS[s_folder][KML_LIST]:
            xml_root = ET.fromstring( kml.kml() )
            src_doc = xml_root.find( f'{schema}Document' )

            # Iterate over children of the source document
            for child in src_doc:

                # Replicate child element
                replicated_child = util.replicate_kml_element( child )

                tag_name = replicated_child.tag.replace( schema, '' )

                # Collect Style and StyleMap elements
                if tag_name in ['Style', 'StyleMap']:
                    style_id = replicated_child.get( 'id' )
                    if style_id not in dc_unique_styles:
                        dc_unique_styles[style_id] = replicated_child

                # Collect feature elements
                elif tag_name in ['Placemark', 'Folder', 'Document', 'NetworkLink']:
                    ls_features.append( replicated_child )

        # Insert collected styles first into the Parent Document
        for style_id, style_element in dc_unique_styles.items():
            parent_document.insert( 0, style_element ) # Insert at the beginning of the Document

        # Append features to content folder
        for feature in ls_features:
            content_folder.append( feature )

        # Save the combined KML
        xml = ET.ElementTree( root )

        # Save for later, to be inserted into a tree
        DC_FOLDERS[s_folder][XML] = xml


        if output_directory:
            s_filename = f'{"_".join( s_folder.split() )}.kml'
            print( f'Saving folder "{s_folder_name}" to {s_filename}' )
            output_path = os.path.join( output_directory, 'folders', s_filename )
            xml.write( output_path, encoding='utf-8', xml_declaration=True )

    return


# Generate data structure to group folders into trees
def make_dc_trees():

    # Iterate over house and weatherization types that we want in folders
    for hs in FOLDER_HOUSE_TYPES:
        for wx in FOLDER_WX_TYPES:

            # Generate a tree label
            hs = hs.lower()
            wx = wx.lower()
            s_label = '_'.join( [hs, wx] )

            # Initialize an empty dictionary for this tree
            DC_TREES[s_label] = \
            {
                TREE_DEBUG: [],
                XML_LIST: [],
                PARCEL_COUNT: 0,
                UNIT_COUNT: 0,
                XML: None,
            }

            # Populate the dictionary for this tree
            for s_folder, dc_folder in DC_FOLDERS.items():

                # Split the label into its parts
                ls_folder_parts = s_folder.split()

                # If the current house and weatherization types are in the split folder label...
                if hs in ls_folder_parts and wx in ls_folder_parts:
                    #... Add current folder to this tree
                    DC_TREES[s_label][TREE_DEBUG].append( s_folder )
                    DC_TREES[s_label][XML_LIST].append( dc_folder[XML] )
                    DC_TREES[s_label][PARCEL_COUNT] += ( dc_folder[PARCEL_COUNT] )
                    DC_TREES[s_label][UNIT_COUNT] += ( dc_folder[UNIT_COUNT] )

    return


# Insert KML folders into trees
def insert_folders_in_trees( output_directory ):

    for s_tree, dc_tree in DC_TREES.items():

        # Convert lowercase folder string to formatted name
        s_tree_name = format_words( s_tree, '_' )

        # Append counts
        n_parcels = dc_tree[PARCEL_COUNT]
        n_units = dc_tree[UNIT_COUNT]
        s_tree_name += format_counts( n_parcels, n_units )

        output_path = os.path.join( output_directory, 'trees', f'{s_tree}.kml' )
        dc_tree[XML] = util.insert_in_parent_kml_folder( dc_tree[XML_LIST], s_tree_name, output_path )

    return


# Group trees into folders based on house type
def make_treetop( s_house_type, output_directory ):

    # Select trees by house type
    ls_trees = []
    for s_tree in DC_TREES:
        if s_house_type in s_tree:
            ls_trees.append( DC_TREES[s_tree] )

    # Collect XMLs and counts from selected trees
    ls_xml = []
    n_parcels = 0
    n_units = 0
    for dc_tree in ls_trees:
        ls_xml.append( dc_tree[XML] )
        n_parcels += dc_tree[PARCEL_COUNT]
        n_units += dc_tree[UNIT_COUNT]

    # Make treetop for this house type
    s_name = make_doc_name( s_house_type, n_parcels, n_units )
    output_path = os.path.join( output_directory, 'treetops', f'{s_house_type}.kml' )
    xml = util.insert_in_parent_kml_folder( ls_xml, s_name, output_path )

    return xml


# Build treetops and full parcels tree
def make_parcels_tree( output_directory ):

    # Initialize values for parcels tree root
    ls_xml = []

    # Generate top-level subtrees based on house types
    for s_house_type in FOLDER_HOUSE_TYPES:

        # Generate the subtree
        xml = make_treetop( s_house_type, output_directory )

        # Accumulate information for parcels tree root
        ls_xml.append( xml )

    # Generate full parcels tree
    output_path = os.path.join( output_directory, '', 'parcels.kml' )
    util.insert_in_parent_kml_folder( ls_xml, 'Parcels', output_path )

    return



######################

# Main program
if __name__ == '__main__':

    # Read arguments
    parser = argparse.ArgumentParser( description='Generate KML files showing Lawrence parcels partitioned in various ways' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename', required=True )
    parser.add_argument( '-o', dest='output_directory', help='Target directory output files', required=True )
    parser.add_argument( '-c', dest='clear_directory', action='store_true', help='Clear target directory first?' )

    args = parser.parse_args()

    # Report arguments
    print( '' )
    print( 'Arguments' )
    print( ' Input database:', args.master_filename )
    print( ' Output directory:', args.output_directory )

    # Optionally clear target directory
    if B_DEBUG:
        print( '' )
        print( '===> Running in debug mode <===' )
        print( '' )
    else:
        if args.clear_directory:
            print( ' Clearing output directories' )
            util.clear_directory( args.output_directory, files='*.kml' )
            util.clear_directory( os.path.join( args.output_directory, 'maps' ) )
            util.clear_directory( os.path.join( args.output_directory, 'folders' ) )
            util.clear_directory( os.path.join( args.output_directory, 'trees' ) )
            util.clear_directory( os.path.join( args.output_directory, 'treetops' ) )

    # Read the parcels table
    conn, cur, engine = util.open_database( args.master_filename, False )
    print( '' )
    s_table = 'Assessment_L_Parcels'
    print( f'Reading {s_table}' )
    df_parcels = pd.read_sql_table( s_table, engine )

    # Generate styles of placemarks based on ward and fuel
    df_styles = make_styles()

    # Generate KML documents
    make_kml_documents( df_parcels, df_styles, args.output_directory )

    # Generate data structure to group documents into folders
    make_dc_folders()

    # Generate KML files representing single-tier groupings of documents into foldersl
    insert_maps_in_folders( output_directory=args.output_directory )

    # Build data structure of trees
    make_dc_trees()

    # Insert KML folders into trees
    insert_folders_in_trees( args.output_directory )

    # Build treetops and full parcels tree
    make_parcels_tree( args.output_directory )


    if B_DEBUG:
        print( '' )
        print( '===> Running in debug mode <===' )
        print( '' )

    util.report_elapsed_time()
