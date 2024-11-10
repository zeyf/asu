#!/usr/bin/python3


import psycopg2
import os
import sys


DATABASE_NAME='dds_assignment'
RATINGS_TABLE_NAME='ratings'
RANGE_TABLE_PREFIX='range_part'
RROBIN_TABLE_PREFIX='rrobin_part'
RANGE_QUERY_OUTPUT_FILE='RangeQueryOut.txt'
PONT_QUERY_OUTPUT_FILE='PointQueryOut.txt'
RANGE_RATINGS_METADATA_TABLE ='rangeratingsmetadata'
RROBIN_RATINGS_METADATA_TABLE='roundrobinratingsmetadata'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    with openconnection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_name
            LIKE
                '{RANGE_TABLE_PREFIX}%'
            OR
                table_name
            LIKE
                '{RROBIN_TABLE_PREFIX}%'
            """
        )

        partition_table_names = cursor.fetchall()
        with open(RANGE_QUERY_OUTPUT_FILE, "w+") as f:
            for (table_name, ) in partition_table_names:
                cursor.execute(
                    f"""
                    SELECT
                        userid,
                        movieid,
                        rating
                    FROM
                        {table_name}
                    WHERE
                        rating >= {ratingMinValue}
                        AND rating <= {ratingMaxValue}
                    """
                )

                matching_rows = cursor.fetchall()
                for user_id, movie_id, rating in matching_rows:
                    f.write(f"{table_name},{user_id},{movie_id},{rating}\n")



def PointQuery(ratingsTableName, ratingValue, openconnection):
    with openconnection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_name
            LIKE
                '{RANGE_TABLE_PREFIX}%'
            OR
                table_name
            LIKE
                '{RROBIN_TABLE_PREFIX}%'
            """
        )

        partition_table_names = cursor.fetchall()
        with open(PONT_QUERY_OUTPUT_FILE, "w+") as f:
            for (table_name,) in partition_table_names:
                cursor.execute(
                    f"""
                    SELECT
                        userid,
                        movieid,
                        rating
                    FROM
                        {table_name}
                    WHERE
                        rating = {ratingValue}
                    """
                )

                matching_rows = cursor.fetchall()
                for user_id, movie_id, rating in matching_rows:
                    f.write(f"{table_name},{user_id},{movie_id},{rating}\n")
                


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()