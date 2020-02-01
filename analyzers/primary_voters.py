# Copyright 2019 Energize Andover.  All rights reserved.

import sqlalchemy
import pandas as pd
from collections import OrderedDict
import datetime

import sys
sys.path.append( '../util' )
import util


#------------------------------------------------------

####################
# Global constants #
####################

LABEL = 'label'
VALUE = 'value'

RELEASE_ID = util.ID


#------------------------------------------------------

###############
# Main script #
###############


# Open database
db_path = '../db/master.sqlite'
engine = sqlalchemy.create_engine( 'sqlite:///' + db_path )

# Extract list of primary voters
df_voters = pd.read_sql_table( 'Residents', engine, parse_dates=True )
n_attended_tm_all = len( df_voters[ df_voters[util.TOWN_MEETINGS_ATTENDED] != 0 ] )
df_voters = df_voters[ df_voters[util.PRIMARY_ELECTIONS_VOTED] > 0 ]
n_attended_tm_pv = len( df_voters[ df_voters[util.TOWN_MEETINGS_ATTENDED] != 0 ] )


# Generate summary
n_d_total = len( df_voters[df_voters[util.PARTY_AFFILIATION] == util.D] )
n_r_total = len( df_voters[df_voters[util.PARTY_AFFILIATION] == util.R] )
n_u_total = len( df_voters[df_voters[util.PARTY_AFFILIATION] == util.U] )

a_summary = []
a_summary.append( { LABEL: 'Primary Voters', VALUE: len( df_voters ) } )
a_summary.append( { LABEL: 'Democratic Primary Voters', VALUE: n_d_total } )
a_summary.append( { LABEL: 'Republican Primary Voters', VALUE: n_r_total } )
a_summary.append( { LABEL: 'Unaffiliated Primary Voters', VALUE: n_u_total } )
a_summary.append( { LABEL: 'Primary Voters who Attended Town Meeting', VALUE: n_attended_tm_pv } )
a_summary.append( { LABEL: 'Total Voters who Attended Town Meeting', VALUE: n_attended_tm_all } )
df_summary = pd.DataFrame( a_summary, columns=[ LABEL, VALUE ] )

# Save results to spreadsheets

# Create ID column for release version
df_voters[RELEASE_ID] = 1 + df_voters.reset_index().index
df_voters = df_voters.set_index( RELEASE_ID ).reset_index()

# Save debug version of spreadsheet
with pd.ExcelWriter( '../analysis/primary_voters_debug.xlsx' ) as writer:
    df_summary.to_excel( writer, sheet_name='Summary', index=False, header=False )
    df_voters.to_excel( writer, sheet_name='Primary Voters', index=False )

# Remove debug columns
debug_columns = df_voters.columns
df_voters = df_voters.drop( columns=[util.RESIDENT_ID,util.FIRST_NAME,util.LAST_NAME] )
print( '' )
print( 'Columns removed for release version' )
print( list( debug_columns.difference( df_voters.columns ) ) )

# Save release version of spreadsheet
with pd.ExcelWriter( '../analysis/primary_voters.xlsx' ) as writer:
    df_summary.to_excel( writer, sheet_name='Summary', index=False, header=False )
    df_voters.to_excel( writer, sheet_name='Primary Voters', index=False )

util.report_elapsed_time()
