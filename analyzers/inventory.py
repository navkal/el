# Copyright 2019 Energize Andover.  All rights reserved.

import pandas as pd

import sys
sys.path.append( '../util' )
import util

COLUMN_NAME = 'column_name'


# Determine whether column is present in table
def is_column_in_table( column_name, table_columns ):
    return util.YES if ( column_name in table_columns ) else util.NO


# Main program
if __name__ == '__main__':

    # Build list of master and published database names
    db_names = ['master'] + list( util.PUBLISH_INFO.keys() )

    # Build inventory of listed databases
    inventory = {}

    for db_name in db_names:

        db = util.read_database( '../db/' + db_name + '.sqlite' )

        for table_name in db.keys():

            if table_name in inventory:
                # Insert database into dataframe for current table
                inventory[table_name][db_name] = inventory[table_name].apply( lambda row: is_column_in_table( row[COLUMN_NAME], set( db[table_name].columns ) ), axis=1 )
            else:
                # Create and initialize dataframe for current table
                inventory[table_name] = pd.DataFrame( data={ COLUMN_NAME: db[table_name].columns } )
                inventory[table_name][db_name] = util.YES


    # Save inventory
    with pd.ExcelWriter( '../analysis/inventory.xlsx' ) as writer:

        # Get sorted list of table names
        table_names = list( inventory.keys() )
        table_names.sort()

        # Save dataframes to Excel sheets
        for table_name in table_names:
            inventory[table_name].to_excel( writer, sheet_name=table_name, index=False )


    # Report elapsed time
    util.report_elapsed_time()

