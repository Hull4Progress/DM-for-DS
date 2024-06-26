"""
Created on Wed May 8, 2024

@author: aunshx
"""

import sys
import json
import csv
import yaml

import copy

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


# ==============================


# Simple check
def hello_world():
    print("Hello World!")

# Query builder
def build_query_listings_reviews(date1, date2):
    q31 = """SELECT DISTINCT l.id, l.name
FROM listings l, reviews r 
WHERE l.id = r.listing_id
  AND r.date >= '"""
    q32 = """'
  AND r.date <= '"""
    q33 = """'
ORDER BY l.id;"""
    return q31 + date1 + q32 + date2 + q33    

# Calculate the difference between times 
def time_diff(time1, time2):
    return (time2-time1).total_seconds()

# Calculate and return a dict with metrics for each year query 
def calc_time_diff_per_year(db_eng, count, q_dict):
    perf_details = {}

    # Iterate through all the queries in q_dict
    for year, sql_query in q_dict.items():
        time_list = []        
        for i in range(count): 
            time_start = datetime.now()

            with db_eng.connect() as conn:
                df = pd.read_sql(sql_query, con=conn)

            time_end = datetime.now()
            # Calulate the time difference
            diff = time_diff(time_start, time_end)
            time_list.append(diff)
            
        # Splitting the string to get the year
        parts = year.split('_')
        curr_year = parts[-1]

        # Calulcate the metrics
        perf_profile = {
            'avg': round(sum(time_list) / len(time_list), 4),
            'min': round(min(time_list), 4),
            'max': round(max(time_list), 4),
            'std': round(np.std(time_list), 4)
        }
        # Add metrics according to the year
        perf_details[curr_year] = perf_profile
    return perf_details


# Fetches the json file
def fetch_perf_data(filename):
    f = open('perf_data/' + filename)
    return json.load(f)

# writes the dictionary in dict as a json file into filename
def write_perf_data(dict1, filename):
    dict2 = dict(sorted(dict1.items()))
    with open('perf_data/' + filename, 'w') as fp:
        json.dump(dict2, fp)
        
# Function to rename keys and extract the year
def rename_keys(data):
    new_data = {}
    for key, value in data.items():
        if key.startswith("listings_join_reviews_"):
            year = key.split("_")[-1]
            new_data[year] = value
        else:
            new_data[key] = value
    return new_data
