# Copyright 2019 Energize Andover.  All rights reserved.

import sqlalchemy
import pandas as pd

import sys
sys.path.append( '../util' )
import util


#------------------------------------------------------

###############
# Main script #
###############

# Open database
db_path = '../db/master.sqlite'
engine = sqlalchemy.create_engine( 'sqlite:///' + db_path )

# Read election tables from database
election_columns = [util.RESIDENT_ID, util.ELECTION_DATE, util.LAST_NAME, util.FIRST_NAME]
df_e1 = pd.read_sql_table( 'ElectionModel_01', engine, columns=election_columns )
df_e2 = pd.read_sql_table( 'ElectionModel_02', engine, columns=election_columns )
df_e3 = pd.read_sql_table( 'ElectionModel_03', engine, columns=election_columns )
df_elections = df_e1.append( df_e2 ).append( df_e3 )

# Get date of most recent election
df_elections = df_elections.sort_values( by=[util.ELECTION_DATE], ascending=[False] )
last_election_date = df_elections.iloc[0][util.ELECTION_DATE ]

# Isolate data pertaining to last election
df_last_election = df_elections[ df_elections[util.ELECTION_DATE] == last_election_date ]
print( 'Total voters participating in last election:', len( df_last_election ) )

# Read list of residents from database
last_election_column_name = 'primary_ballot_' + last_election_date.split()[0]
residents_columns = [util.RESIDENT_ID, last_election_column_name]
df_residents = pd.read_sql_table( 'Residents', engine, parse_dates=True, columns=residents_columns )

# Find known voters who participated in the last election
df_known_voters = df_residents[ df_residents[last_election_column_name].notnull() & ( df_residents[last_election_column_name] != util.ABSENT ) ]
print( 'Known voters participating in last election:', len( df_known_voters ) )

# Find unknown voters
set_unknown_voters = set( df_last_election[util.RESIDENT_ID] ) - set( df_known_voters[util.RESIDENT_ID] )
df_unknown_voters = df_last_election[ df_last_election[util.RESIDENT_ID].isin( set_unknown_voters ) ]
print( 'Unknown voters participating in last election:', len( set_unknown_voters ) )

# Generate spreadsheet listing unknown voters
df_unknown_voters = df_unknown_voters.drop( columns=[util.ELECTION_DATE] )
df_unknown_voters.to_excel( '../analysis/new_voters.xlsx', index=False )

util.report_elapsed_time()
