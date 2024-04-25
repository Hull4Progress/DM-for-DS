#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 21:37:45 2024

@author: rick
"""

import sys
import json
import csv
import yaml

import pandas as pd
import numpy as np

import matplotlib as mpl

import time
from datetime import datetime
# see https://stackoverflow.com/questions/415511/how-do-i-get-the-current-time-in-python
#   for some basics about datetime

import pprint

# sqlalchemy 2.0 documentation: https://www.sqlalchemy.org/
import psycopg2
from sqlalchemy import create_engine, text as sql_text



#============================================
#
#  Basic utilities
#
#============================================

def hello_world():
    print('hello world')
    
def time_diff(time1, time2):
    return (time2-time1).total_seconds()

#============================================
#
#  Query building utilities
#
#============================================



def build_query_listings_join_reviews(date1, date2):
    q31 = """
SELECT DISTINCT l.id, l.name
FROM listings l, reviews r 
WHERE l.id = r.listing_id
  AND r.date >= '"""
    q32 = """'
  AND r.date <= '"""
    q33 = """'
ORDER BY l.id;
"""
    return q31 + date1 + q32 + date2 + q33

# test: 
# print(build_query_listings_join_reviews('2015-01-01', '2015-12-31'))

    
# I am loading the query results into a dataframe to make sure that the full
#    value of the query is retrieved by PostgreSQL.  If I retrieve into a
#    cursor, then the system may use "lazy" evaluation and not actually retrieve
#    all the records until I access them later with fetchone() or fetchmany()


#============================================
#
#  Run Test utility
#
#============================================


def run_test(db_eng, q, count):
    time_list = []
    for i in range(0,count):
        time_start = datetime.now()
        # Open new db connection for each execution of the query to avoid multithreading
        with db_eng.connect() as conn:
            # conn.execute(sql_text(q3))
            df = pd.read_sql(q, con=conn)
        time_end = datetime.now()
        diff = time_diff(time_start, time_end)
        time_list.append(diff)
    return time_list, \
        round(sum(time_list)/len(time_list), 4), \
        round(min(time_list), 4), \
        round(max(time_list), 4), \
        round(np.std(time_list), 4)
        
#============================================
#
#  Add/Drop index utility
#
#============================================

        

def add_drop_index(db_eng, table_name, add_drop, index_name, index_column):
    if add_drop == 'add':
        q1 = """BEGIN TRANSACTION;
CREATE INDEX IF NOT EXISTS """
        q2 = """
ON """
        q3 = """("""
        q4 = """);
END TRANSACTION;
"""
        modify_index = q1 + index_name + q2 + table_name + q3 + index_column + q4
    elif add_drop == 'drop':
        q6 = """BEGIN TRANSACTION;
DROP INDEX IF EXISTS """
        q7 = """;
END TRANSACTION;
"""
        modify_index = q6 + index_name + q7
    else:
        print('ERROR: call to function add_drop_index has invalid add_drop value:', add_drop)
        return

    # print('Inside the function add_drop_index the SQL queries produced are: \n')
    # print(modify_index)
    # print()

    q8 = """SELECT *
FROM pg_indexes
WHERE tablename = '"""
    q9 = """';
"""
    show_indexes = q8 + table_name + q9

    # print(show_indexes)
    # print()

    with db_eng.connect() as conn:
        conn.execute(sql_text(modify_index))
        result_indexes = conn.execute(sql_text(show_indexes))

    return result_indexes.fetchall()


# TESTING

# add_test = add_drop_index(db_eng, 'reviews', 'add', 'date_in_reviews', 'date')
# print(add_test)

# drop_test = add_drop_index(db_eng, 'reviews', 'drop', 'date_in_reviews', '')
# print(drop_test)

# bad_test = add_drop_index(db_eng, 'reviews', 'foo', 'date_in_reviews', '')
# print(bad_test)
