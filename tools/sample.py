# Copyright 2019 Energize Andover.  All rights reserved.

import sqlalchemy
import pandas as pd
import sqlite3

db_path = '../db/student.sqlite'
engine = sqlalchemy.create_engine( 'sqlite:///' + db_path )

# Read the Census table into a dataframe
df_census = pd.read_sql_table( 'Census', engine, parse_dates=True )
print( '\n\n--- Census table ---' )
print( '\nColumn names:' )
print( df_census.columns )
print( '\nA few rows:' )
print( df_census.head() )

# Read the Solar table into a dataframe
df_solar = pd.read_sql_table( 'Solar', engine, parse_dates=True )
print( '\n\n--- Solar table ---' )
print( '\nColumn names:' )
print( df_solar.columns )
print( '\nA few rows:' )
print( df_solar.head() )

# Write both dataframes to new database
conn = sqlite3.connect( './out/sample.sqlite' )
df_census.to_sql( 'Census', conn, if_exists='replace', index=False )
df_solar.to_sql( 'Solar', conn, if_exists='replace', index=False )

# Write Census dataframe to Excel and CSV files
df = df_census.drop( columns='01-id' )
df.to_excel( './out/census.xlsx', index=False )
df.to_csv( './out/census.csv', index=False, header=True )

# Write Solar dataframe to Excel and CSV files
df = df_solar.drop( columns='1-id' )
df.to_excel( './out/solar.xlsx', index=False )
df.to_csv( './out/solar.csv', index=False, header=True )

# Write both dataframes to multiple sheets in single Excel file
with pd.ExcelWriter( './out/sample.xlsx' ) as writer:
    df_census.to_excel( writer, sheet_name='Census' )
    df_solar.to_excel( writer, sheet_name='Solar' )
