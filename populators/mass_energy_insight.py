# Copyright 2023 Energize Lawrence.  All rights reserved.

import argparse
import os
import pandas as pd
import chardet

import sys
sys.path.append( '../util' )
import util


#################################################
# When downloading fresh data from MEI website:
# . Select desired years, must be consecutive
# . Uncheck 'null' to exclude empty column names
# . Save in CSV format
#################################################


# Peek into CSV input file to find the start year
def find_start_year( csv_filename ):

    with open( csv_filename, 'rb' ) as rawdata:

        # Read first line of CSV file
        encoding_info = chardet.detect( rawdata.read( 10000 ) )
        sep = bytes( '\t', 'utf-8' ).decode( 'unicode_escape' )
        df = pd.read_csv( csv_filename, encoding=encoding_info['encoding'], sep=sep, dtype=object, nrows=0 )

        # Extract starting year from first named column
        df = df.drop( columns=[col for col in df.columns if 'Unnamed' in col] )
        start_year = int( df.columns[0].split()[1] ) - 1

        return start_year


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate Mass Energy Insight master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    args = parser.parse_args()

    # Read Mass Energy Insight data - Andover
    print( '\n=======> Mass Energy Insight input - Andover' )
    csv_filename = '../xl/mass_energy_insight/mass_energy_insight_a.csv'
    table_name = 'RawMassEnergyInsight_A'
    drop_columns = 'unit,mmbtu'
    os.system( 'python xl_to_db.py -i {} -t {} -v -p {} -o {} -c'.format( csv_filename, table_name, drop_columns, args.master_filename ) )

    # Read Mass Energy Insight data - Lawrence
    print( '\n=======> Mass Energy Insight input - Lawrence' )
    csv_filename = '../xl/mass_energy_insight/mass_energy_insight_l.csv'
    table_name = 'RawMassEnergyInsight_L'
    drop_columns = 'unit,mmbtu'
    os.system( 'python xl_to_db.py -i {} -t {} -v -p {} -o {}'.format( csv_filename, table_name, drop_columns, args.master_filename ) )

    # Preprocess Mass Energy Insight data - Andover
    print( '\n=======> Mass Energy Insight preprocess - Andover' )
    os.system( 'python mass_energy_insight_preprocess.py -i RawMassEnergyInsight_A -o RawMassEnergyInsight_A_OldFormat -d {0}'.format( args.master_filename ) )

    # Preprocess Mass Energy Insight data - Lawrence
    print( '\n=======> Mass Energy Insight preprocess - Lawrence' )
    os.system( 'python mass_energy_insight_preprocess.py -i RawMassEnergyInsight_L -o RawMassEnergyInsight_L_OldFormat -d {0}'.format( args.master_filename ) )


    # Read external suppliers data
    print( '\n=======> External suppliers input' )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/external_suppliers_electric_l.xlsx -t RawExternalSuppliersElectric_L -o {0}'.format( args.master_filename ) )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/external_suppliers_gas_l.xlsx -t RawExternalSuppliersGas_L -r 1 -m -o {0}'.format( args.master_filename ) )

    # Read ISO zones data
    print( '\n=======> ISO zones input' )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/iso_zones_l.xlsx -s account_number -t RawIsoZones_L -o {0}'.format( args.master_filename ) )

    # Generate clean Mass Energy Insight tables with optional addition of external suppliers data
    print( '\n=======> Mass Energy Insight tables' )
    os.system( 'python mass_energy_insight_clean.py -i RawMassEnergyInsight_A_OldFormat -o Mei_A -d {0}'.format( args.master_filename ) )
    os.system( 'python mass_energy_insight_clean.py -i RawMassEnergyInsight_L_OldFormat -z RawIsoZones_L -e RawExternalSuppliersElectric_L -g RawExternalSuppliersGas_L -o Mei_L -p ExternalSuppliersElectric_L -q ExternalSuppliersGas_L -d {0}'.format( args.master_filename ) )


    # Generate Mass Energy Insight month tables
    print( '\n=======> Mass Energy Insight months' )
    os.system( 'python mass_energy_insight_months.py -i Mei_A -o Mei_A -d {0}'.format( args.master_filename ) )
    os.system( 'python mass_energy_insight_months.py -i Mei_L -o Mei_L -d {0}'.format( args.master_filename ) )

    # Generate Mass Energy Insight totals
    print( '\n=======> Mass Energy Insight totals' )
    os.system( 'python mass_energy_insight_totals.py -i Mei_A -o Mei_A_Totals -d {0}'.format( args.master_filename ) )
    os.system( 'python mass_energy_insight_totals.py -i Mei_L -o Mei_L_Totals -d {0}'.format( args.master_filename ) )



    # Read National Grid electric meter data - Andover
    print( '\n=======> National Grid electric meters input - Andover' )
    csv_directory = '../xl/mass_energy_insight/electric_meters_a'
    raw_table_name = 'RawElectricMeters_A'
    os.system( 'python xl_to_db.py -d {} -v -l account_number -s account_number,readDate -t {} -o {}'.format( csv_directory, raw_table_name, args.master_filename ) )

    # Summarize National Grid electric meter data - Andover
    print( '\n=======> National Grid electric meters summary - Andover' )
    facility_table_name = 'Mei_A_Totals'
    table_name = 'ElectricMeters_A'
    os.system( 'python mass_energy_insight_meters.py -i {} -f {} -o {} -d {}'.format( raw_table_name, facility_table_name, table_name, args.master_filename ) )

    # Read National Grid electric meter data - Lawrence
    print( '\n=======> National Grid electric meters input - Lawrence' )
    csv_directory = '../xl/mass_energy_insight/electric_meters_l'
    raw_table_name = 'RawElectricMeters_L'
    os.system( 'python xl_to_db.py -d {} -v -l account_number -s account_number,readDate -t {} -o {}'.format( csv_directory, raw_table_name, args.master_filename ) )

    # Summarize National Grid electric meter data - Lawrence
    print( '\n=======> National Grid electric meters summary - Lawrence' )
    facility_table_name = 'Mei_L_Totals'
    table_name = 'ElectricMeters_L'
    os.system( 'python mass_energy_insight_meters.py -i {} -f {} -o {} -d {}'.format( raw_table_name, facility_table_name, table_name, args.master_filename ) )

    # Generate copyright notice
    print( '\n=======> Copyright' )
    util.create_about_table( 'MassEnergyInsight', util.make_df_about_energize_lawrence(), args.master_filename )

    # Publish research copy of database
    input_db = util.read_database( args.master_filename )
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'RawElectricMeters_A',
            'RawElectricMeters_L',
            'RawExternalSuppliersElectric_L',
            'RawExternalSuppliersGas_L',
            'RawIsoZones_L',
            'RawMassEnergyInsight_A',
            'RawMassEnergyInsight_L',
            'RawMassEnergyInsight_A_OldFormat',
            'RawMassEnergyInsight_L_OldFormat',
         ],
        'encipher_column_names':
        [
        ],
        'drop_column_names':
        [
        ]
    }
    util.publish_database( input_db, args.research_filename, publish_info )

    util.report_elapsed_time()
