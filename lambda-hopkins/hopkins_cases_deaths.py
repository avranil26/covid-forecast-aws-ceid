#!/usr/bin/env python
# coding: utf-8

# In[1]:


import covidcast, os

from datetime import date
import pandas as pd
import boto3

from io import StringIO, BytesIO


# In[ ]:


def handler(event, context):
    
    bucket_name = os.environ.get('bucket_name')
    client = boto3.client('s3', region_name='us-east-1')
    client_athena = boto3.client('glue', region_name='us-east-1')

    # Incidence Cases
    incidence_cases_data = data_as_of_date(signal="confirmed_incidence_num", geo_type="state")
    incidence_cases_data["target_type"] = "day ahead inc case"

    # Incidence Deaths
    incidence_deaths_data = data_as_of_date(signal="deaths_incidence_num", geo_type="state")
    incidence_deaths_data["target_type"] = "day ahead inc death"
    
    #cumulative dealths
    cumulative_deaths_data = data_as_of_date(signal="deaths_cumulative_num", geo_type="state")
    cumulative_deaths_data["target_type"] = "day ahead cum death"

    final_data = pd.concat([incidence_cases_data, incidence_deaths_data, cumulative_deaths_data], axis=0)    
    
    final_data['geo_value'] = final_data["geo_value"].str.upper()
    final_data.rename(columns={'geo_value': 'location', 'time_value': 'target_end_date'}, inplace = True)

    locations = pd.read_csv("abb2fips.csv", dtype={
                     'state_code': str
                     })

    mapping = dict(zip(locations.state, locations.state_code))
    final_data["location"] = final_data['location'].map(mapping)

    final_data['value'] = final_data['value'].astype(str).astype(float)

    final_data = final_data[final_data['location'].notna()]
    
    
    dataframe_to_s3(client, final_data, bucket_name = bucket_name, 
                    filepath= "hopkins-reformatted/" + str(date.today()) + "/time-series-covid19-US.parquet", 
                    format = 'parquet')
    
    response = client_athena.start_crawler(Name='hopkins-csse')


# In[2]:


def data_as_of_date(signal, geo_type):
    data = covidcast.signal(data_source= "jhu-csse",signal= signal, as_of= date.today(),
    #start_day=date(2020, 1, 1), end_day=date.today(),
    geo_type=geo_type)

    #data['geo_value'] = data["geo_value"].str.upper()

    data = data[["geo_value", "time_value", "value"]]
    return data


# In[5]:


def dataframe_to_s3(s3_client, input_datafame, bucket_name, filepath, format):

        if format == 'parquet':
            out_buffer = BytesIO()
            input_datafame.to_parquet(out_buffer, index=False)

        elif format == 'csv':
            out_buffer = StringIO()
            input_datafame.to_parquet(out_buffer, index=False)

        s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())


# In[10]:


#incidence_cases_data = data_as_of_date(signal="confirmed_incidence_num", geo_type="state")


# In[30]:


#incidence_deaths_data = data_as_of_date(signal="deaths_incidence_num", geo_type="state")


# In[34]:


# Cumulative Cases

#cumulative_cases = covidcast.signal(data_source= "jhu-csse",signal= "confirmed_cumulative_num", as_of= date.today(), geo_type="state")
#cumulative_cases['geo_value'] = cumulative_cases["geo_value"].str.upper()
#cumulative_cases = cumulative_cases[["geo_value", "time_value", "value"]]

# Incidence Cases

#incidence_cases = covidcast.signal(data_source= "jhu-csse",signal= "confirmed_incidence_num", as_of= date.today(), geo_type="state")
#incidence_cases['geo_value'] = incidence_cases["geo_value"].str.upper()
#incidence_cases = incidence_cases[["geo_value", "time_value", "value"]]

# Cumulative Deaths


#cumulative_deaths = covidcast.signal(data_source= "jhu-csse",signal= "deaths_cumulative_num", as_of= date.today(), geo_type="state")
#cumulative_deaths['geo_value'] = cumulative_deaths["geo_value"].str.upper()
#cumulative_deaths = cumulative_deaths[["geo_value", "time_value", "value"]]

# Incidence Deaths

#incidence_deaths = covidcast.signal(data_source= "jhu-csse",signal= "deaths_incidence_num", as_of= date.today(), geo_type="state")
#incidence_deaths['geo_value'] = incidence_deaths["geo_value"].str.upper()
#incidence_deaths = incidence_deaths[["geo_value", "time_value", "value"]]

