# Copyright 2019 Energize Andover.  All rights reserved.

import argparse
import pandas as pd
pd.set_option( 'display.max_columns', 500 )
pd.set_option( 'display.width', 1000 )

import sys
sys.path.append( '../util' )
import util


#------------------------------------------------------


def calculate_mean_dem_score( row, partition_column ):
    df_partition_voters = df_voters[ df_voters[partition_column] == row[partition_column] ]
    partition_score = round( df_partition_voters[util.LIKELY_DEM_SCORE].mean() ) if ( len( df_partition_voters ) > 0 ) else 0
    return partition_score


def calculate_mean_engagement_score( row, partition_column, voter_affiliation=None ):

    # Start with all voters in the partition
    df_partition_voters = df_voters[ df_voters[partition_column] == row[partition_column] ]

    # Optionally narrow set of voters to likely Democrats or Republicans
    if voter_affiliation == util.D:
        df_partition_voters = df_partition_voters[ df_partition_voters[util.LIKELY_DEM_SCORE] > 0 ]
    elif voter_affiliation == util.R:
        df_partition_voters = df_partition_voters[ df_partition_voters[util.LIKELY_DEM_SCORE] < 0 ]

    # Calculate mean score for the partition
    partition_score = round( df_partition_voters[util.VOTER_ENGAGEMENT_SCORE].mean(), 2 ) if ( len( df_partition_voters ) > 0 ) else 0

    return partition_score


def calculate_mean_assessed_value( row, partition_column ):
    df_res = df_residents[ df_residents[partition_column] == row[partition_column] ]
    df_res = df_res[ df_res[util.TOTAL_ASSESSED_VALUE] > 0 ]
    df_res = df_res.drop_duplicates( subset=[util.NORMALIZED_STREET_NUMBER, util.NORMALIZED_STREET_NAME] )
    mean = int( df_res[util.TOTAL_ASSESSED_VALUE].mean() ) if ( len( df_res ) > 0 ) else 0
    return mean

#------------------------------------------------------

###############
# Main script #
###############


if __name__ == '__main__':

    # Retrieve and validate arguments
    parser = argparse.ArgumentParser( description='Characterize partitions of residents by voting habits' )
    parser.add_argument( '-m', dest='master_filename',  help='Master database filename' )
    parser.add_argument( '-p', dest='partition_column',  help='Column on which to partition residents' )
    parser.add_argument( '-t', dest='table_name',  help='Name of partition table' )
    args = parser.parse_args()

    # Open the master database
    conn, cur, engine = util.open_database( args.master_filename, False )

    # Read Residents table and isolate voters
    df_residents = pd.read_sql_table( 'Residents', engine, parse_dates=True )
    df_residents[util.TOTAL_ASSESSED_VALUE] = df_residents[util.TOTAL_ASSESSED_VALUE].astype(float).fillna(0).astype(int)
    df_voters = df_residents[ df_residents[util.VOTED] == util.YES ]
    df_local_voters = df_voters[ df_voters[util.LOCAL_ELECTIONS_VOTED] > 0 ]
    df_local_voters = df_local_voters[ [args.partition_column, util.LOCAL_ELECTIONS_VOTED, util.PARTY_AFFILIATION] ]

    # Initialize dataframe of partitions with resident counts
    df_partitions = df_residents[args.partition_column].value_counts().rename( util.RESIDENT_COUNT ).to_frame()
    df_partitions[args.partition_column] = df_partitions.index
    df_partitions = df_partitions[ [ args.partition_column, util.RESIDENT_COUNT ] ]

    # Add column of per-partition voter counts
    df_count = df_voters[args.partition_column].value_counts().rename( util.VOTER_COUNT ).to_frame()
    df_count[args.partition_column] = df_count.index
    df_count = df_count.reset_index( drop=True )
    df_partitions = pd.merge( df_partitions, df_count, how='left', on=[args.partition_column] )

    # Add column of per-partition likely Democrat counts
    df_dems = df_voters[ df_voters[util.LIKELY_DEM_SCORE] > 0 ]
    df_count = df_dems[args.partition_column].value_counts().rename( util.LIKELY_DEM_COUNT ).to_frame()
    df_count[args.partition_column] = df_count.index
    df_count = df_count.reset_index( drop=True )
    df_partitions = pd.merge( df_partitions, df_count, how='left', on=[args.partition_column] )

    # Add column of per-partition likely Republican counts
    df_reps = df_voters[ df_voters[util.LIKELY_DEM_SCORE] < 0 ]
    df_count = df_reps[args.partition_column].value_counts().rename( util.LIKELY_REPUB_COUNT ).to_frame()
    df_count[args.partition_column] = df_count.index
    df_count = df_count.reset_index( drop=True )
    df_partitions = pd.merge( df_partitions, df_count, how='left', on=[args.partition_column] )

    # Add column of per-partition local Democratic voters
    df_local_d_voters = df_local_voters[ df_local_voters[util.PARTY_AFFILIATION] == util.D ]
    df_count = df_local_d_voters[args.partition_column].value_counts().rename( util.LOCAL_DEM_VOTER_COUNT ).to_frame()
    df_count[args.partition_column] = df_count.index
    df_count = df_count.reset_index( drop=True )
    df_partitions = pd.merge( df_partitions, df_count, how='left', on=[args.partition_column] )

    # Add column of per-partition local Republican voters
    df_local_r_voters = df_local_voters[ df_local_voters[util.PARTY_AFFILIATION] == util.R ]
    df_count = df_local_r_voters[args.partition_column].value_counts().rename( util.LOCAL_REPUB_VOTER_COUNT ).to_frame()
    df_count[args.partition_column] = df_count.index
    df_count = df_count.reset_index( drop=True )
    df_partitions = pd.merge( df_partitions, df_count, how='left', on=[args.partition_column] )

    # Set empty counts as 0 and convert to int
    count_columns = [util.VOTER_COUNT, util.LIKELY_DEM_COUNT, util.LIKELY_REPUB_COUNT, util.LOCAL_DEM_VOTER_COUNT, util.LOCAL_REPUB_VOTER_COUNT]
    df_partitions[count_columns] = df_partitions[count_columns].fillna(0).astype(int)

    # Calculate partition scores as average of resident scores
    df_partitions[util.MEAN_LIKELY_DEM_SCORE] = df_partitions.apply( lambda row: calculate_mean_dem_score( row, args.partition_column ), axis=1 )
    df_partitions[util.MEAN_PARTY_PREFERENCE_SCORE] = df_partitions.apply( lambda row: util.likely_dem_to_party_preference_score( row[util.MEAN_LIKELY_DEM_SCORE] ), axis=1 )
    df_partitions[util.MEAN_VOTER_ENGAGEMENT_SCORE] = df_partitions.apply( lambda row: calculate_mean_engagement_score( row, args.partition_column ), axis=1 )
    df_partitions[util.MEAN_LIKELY_DEM_VOTER_ENGAGEMENT_SCORE] = df_partitions.apply( lambda row: calculate_mean_engagement_score( row, args.partition_column, util.D ), axis=1 )
    df_partitions[util.MEAN_LIKELY_REPUB_VOTER_ENGAGEMENT_SCORE] = df_partitions.apply( lambda row: calculate_mean_engagement_score( row, args.partition_column, util.R ), axis=1 )
    df_partitions[util.MEAN_TOTAL_ASSESSED_VALUE] = df_partitions.apply( lambda row: calculate_mean_assessed_value( row, args.partition_column ), axis=1 )

    # Sort on partition
    df_partitions[args.partition_column] = df_partitions[args.partition_column].astype(str)
    df_partitions = df_partitions.sort_values( by=[args.partition_column] )
    df_partitions = df_partitions.reset_index( drop=True )

    # Save result to database
    util.create_table( args.table_name, conn, cur, df=df_partitions )

    # Report elapsed time
    util.report_elapsed_time()
