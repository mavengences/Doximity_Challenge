# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 18:27:10 2017

@author: Dorien Xia
"""

from bs4 import BeautifulSoup


import urllib.request
import requests
import json
import re
import ast
import pandas as pd
import pandas_datareader.data as web
import csv
from pandas.io import sql
import sqlite3
from sqlalchemy import create_engine
import psycopg2

def api_to_dataframe(api_endpoint):
    first_list=requests.get(api_endpoint).json()
    #print(first_list["users"])
    output_dataframe=pd.DataFrame(columns=['Firstname', 'ID', 'last_active_date', 'Lastname', 'practice_location', 'specialty', 'user_type_classification'])
    for i in range(len(first_list["users"])):
        list_to_append=[]
        list_to_append.append(first_list["users"][i]['firstname'])
        list_to_append.append(str(int(first_list["users"][i]['id'])))
        list_to_append.append(first_list["users"][i]['last_active_date'])
        list_to_append.append(first_list["users"][i]['lastname'])
        list_to_append.append(first_list["users"][i]['practice_location'])
        list_to_append.append(first_list["users"][i]['specialty'])
        list_to_append.append(first_list["users"][i]['user_type_classification'])
        output_dataframe.loc[i]=list_to_append
    return output_dataframe

def line_parse(line_of_data):
    list_of_data=[]
    list_of_soup= BeautifulSoup(line_of_data).find_all("td")
    for i in list_of_soup:
        #print(i.string)
        list_of_data.append(i.string)
    return list_of_data


def html_url_table_to_dataframe(url):
    f = urllib.request.urlopen(url)
    input_soup = BeautifulSoup(f.read())
    data=input_soup.find_all("table")
    header=str(data).split("<tr id=")[1]
    #print(header)
    header_soup= BeautifulSoup(header)
    #print(header_soup)
    header_names=header_soup.find_all("th")
    header_list=[]
    for i in header_names:
        column_name=i.string
        #print(i.string)
        header_list.append(column_name)
    df=pd.DataFrame(columns=header_list)
    list_of_raw_data=str(data).split("<tr id=")[2:]
    print("dataframe is named: \n")
    print(df)
    for i in range(len(list_of_raw_data)):
        list_to_append=line_parse(list_of_raw_data[i])
        #print(list_to_append)
        #print(list_of_raw_data[i])
        #print("\n")
        #print(df.loc[i])
        df.loc[i]=list_to_append
    return df



for i in range(1,4,1):
    url="http://de-tech-challenge-api.herokuapp.com/user_activity?page="+str(i)
    output_df=html_url_table_to_dataframe(url)
    print(output_df.head(10))
    print("\n")
    api_endpoint="http://de-tech-challenge-api.herokuapp.com/api/v1/users?page="+str(i)
    output_api_dataframe=api_to_dataframe(api_endpoint)
    print(output_api_dataframe.head(10))
    print("\n")
    if i ==1:
        final_output_df=output_df.merge(output_api_dataframe, left_on=['ID','Firstname','Lastname'], right_on=['ID','Firstname','Lastname'], how="left")
        print(final_output_df.head(10))
    else:
        merged_df=output_df.merge(output_api_dataframe, left_on=['ID','Firstname','Lastname'], right_on=['ID','Firstname','Lastname'], how="left")
        final_output_df=final_output_df.append(merged_df, ignore_index=True)
        print(merged_df.head(10))
        print("merged dataframe count is: \n")
        print(merged_df.count())
        print("total dataframe count is: \n")
        print(final_output_df.count())


