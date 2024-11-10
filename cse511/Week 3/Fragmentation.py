#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    with openconnection.cursor() as cursor:
        cursor.execute(
            f"""
                CREATE TABLE {ratingstablename} (
                    userid INT NOT NULL,
                    movieid INT NOT NULL,
                    rating FLOAT NOT NULL,
                    timestamp BIGINT NOT NULL
                )
            """
        )

        with open(ratingsfilepath, "r") as f:
            comma_delimited_lines = [
                l.strip().split("::")
                for l in f.readlines()
            ]
            for user_id, movie_id, rating, timestamp in comma_delimited_lines:
                cursor.execute(
                    f"""
                        INSERT INTO {ratingstablename} (
                            userid,
                            movieid,
                            rating,
                            timestamp
                        ) VALUES (
                            {user_id},
                            {movie_id},
                            {rating},
                            {timestamp}
                        )
                    """
                )


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    range_table_prefix = 'range_part'
    partition_range_size = 5 / numberofpartitions

    with openconnection.cursor() as cursor:
        for partition_index in range(numberofpartitions):
            (
                rating_partition_table_lower_bound,
                rating_partition_table_upper_bound,
                is_inclusive_of_lower_bound
            ) = (
                partition_range_size * partition_index,
                partition_range_size * (partition_index + 1),
                partition_index == 0
            )

            cursor.execute(
                f"""
                    CREATE TABLE {range_table_prefix}{partition_index} AS (
                        SELECT
                            *
                        FROM
                            {ratingstablename}
                        WHERE
                            rating >{"=" if is_inclusive_of_lower_bound else ""} {rating_partition_table_lower_bound}
                            AND rating <= {rating_partition_table_upper_bound}
                    )
                """
            )


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    round_robin_table_prefix = 'rrobin_part'

    with openconnection.cursor() as cursor:
        for partition_index in range(numberofpartitions):
            cursor.execute(
                f"""
                    CREATE TABLE {round_robin_table_prefix}{partition_index} AS (
                        SELECT
                            userid,
                            movieid,
                            rating
                        FROM (
                            SELECT
                                *,
                                ROW_NUMBER() OVER() AS rn FROM {ratingstablename}
                            ) AS temp
                        WHERE mod(
                            temp.rn - 1,
                            {numberofpartitions}
                        ) = {partition_index}
                    )
                """
            )

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    round_robin_table_prefix = 'rrobin_part'

    with openconnection.cursor() as cursor:
        cursor.execute(
            f"""
                INSERT INTO
                    {ratingstablename}
                VALUES (
                    {userid},
                    {itemid},
                    {rating}
                )
            """
        )
        
        cursor.execute(
            f"""
                SELECT
                    *
                FROM
                    {ratingstablename}
            """
        )
        primary_ratings_table_records = cursor.fetchall()
        
        # Using the database metadata tables, find out the number of partitions tables existent
        cursor.execute(
            f"""
                SELECT
                    *
                FROM
                    information_schema.tables
                WHERE
                    table_name
                LIKE
                    '{round_robin_table_prefix}%'
            """
        )
        rating_partition_table_records = cursor.fetchall()

        partition_table_insertion_index = (
            len(primary_ratings_table_records) - 1
        ) % len(rating_partition_table_records)
        cursor.execute(
            f"""
                INSERT INTO {round_robin_table_prefix}{partition_table_insertion_index}
                VALUES (
                    {userid},
                    {itemid},
                    {rating}
                )
            """
        )


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    range_table_prefix = 'range_part'

    # Reasoning:
    # Since values can only go from (0, 5] and we are zero indexing partition table names,
    # e.g. at rating of 5, the partition table index would be 4
    # So, we want to move it from 1 index to zero index (substract 1)
    # In order to handle floating point errors since ratings are at 0.5 increments,
    # We do integer division (// 1) in order to remove this
    # In the case of a rating like 0.5 --> (0.5 - 1) // 1 --> (-0.5) // 1 = 0 anyways
    # This covers all edge cases!
    range_table_partition_index = (rating - 1) // 1
    with openconnection.cursor() as cursor:
        cursor.execute(
            f"""
                INSERT INTO {range_table_prefix}{range_table_partition_index} (
                    userid,
                    movieid,
                    rating
                ) VALUES (
                    {userid},
                    {itemid},
                    {rating}
                )
            """
        )

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()