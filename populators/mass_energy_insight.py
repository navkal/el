# Copyright 2022 Energize Lawrence.  All rights reserved.

import argparse
import os
import pandas as pd

import sys
sys.path.append( '../util' )
import util


# Main program
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description='Generate Mass Energy Insight master database' )
    parser.add_argument( '-m', dest='master_filename',  help='Output filename - Name of master database file', required=True )
    parser.add_argument( '-r', dest='research_filename',  help='Output filename - Name of research database file', required=True )
    args = parser.parse_args()

    # Read Mass Energy Insight data
    print( '\n=======> Mass Energy Insight input' )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/mass_energy_insight_a.csv -t RawMassEnergyInsight_A -v -f "\t" -r 1 -e -p "Unnamed: 9" -o {0} -c'.format( args.master_filename ) )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/mass_energy_insight_l.csv -t RawMassEnergyInsight_L -v -f "\t" -r 1 -e -p "Unnamed: 9" -o {0}'.format( args.master_filename ) )

    # Read external suppliers data
    print( '\n=======> External suppliers input' )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/external_suppliers_electric.xlsx -t RawExternalSuppliersElectric -o {0}'.format( args.master_filename ) )
    os.system( 'python xl_to_db.py -i ../xl/mass_energy_insight/external_suppliers_gas.xlsx -t RawExternalSuppliersGas -r 1 -o {0}'.format( args.master_filename ) )

    # Generate clean Mass Energy Insight tables with optional addition of external suppliers data
    print( '\n=======> Mass Energy Insight tables' )
    os.system( 'python mass_energy_insight_clean.py -i RawMassEnergyInsight_A -o Mei_A -d {0}'.format( args.master_filename ) )
    os.system( 'python mass_energy_insight_clean.py -i RawMassEnergyInsight_L -e RawExternalSuppliersElectric -g RawExternalSuppliersGas -o Mei_L -d {0}'.format( args.master_filename ) )

    # Generate Mass Energy Insight month tables
    print( '\n=======> Mass Energy Insight months' )
    os.system( 'python mass_energy_insight_months.py -i Mei_A -o Mei_A -d {0}'.format( args.master_filename ) )
    os.system( 'python mass_energy_insight_months.py -i Mei_L -o Mei_L -d {0}'.format( args.master_filename ) )

    # Generate Mass Energy Insight totals
    print( '\n=======> Mass Energy Insight totals' )
    os.system( 'python mass_energy_insight_totals.py -i Mei_A -o Mei_A_Totals -d {0}'.format( args.master_filename ) )
    os.system( 'python mass_energy_insight_totals.py -i Mei_L -o Mei_L_Totals -d {0}'.format( args.master_filename ) )

    # Generate copyright notice
    print( '\n=======> Copyright' )
    df_about = pd.DataFrame( columns=['copyright'], data=['© 2022 Energize Lawrence.  All rights reserved.'] )
    util.create_about_table( 'MassEnergyInsight', df_about, args.master_filename )

    # Publish research copy of database
    input_db = util.read_database( args.master_filename )
    publish_info = \
    {
        'number_columns': True,
        'drop_table_names':
        [
            'RawMassEnergyInsight_A',
            'RawMassEnergyInsight_L',
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
