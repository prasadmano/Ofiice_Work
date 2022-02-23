import pandas as pd
import ast
from sklearn.preprocessing import LabelEncoder
from datetime import datetime
import numpy as np
from importlib import resources
import io
import json
import os
import csv
import glob
import pgeocode
import re
from sklearn.preprocessing import LabelEncoder
from datetime import datetime
from tqdm import tqdm
tqdm.pandas()
import pytz
from numpy import nan
from hashlib import md5
    

def tansaction_code_hash(df, column_name = 'order_id'):
    """
        Hashing order_id to transaction_code with md5(x.encode()).hexdigest() method

        Input:
            df           : Dataframe on which amount should be cleaned
            column_name  : Column on which the function to be done

        Output:
            df           : Returns existing Dataframe with transaction_code column

    """
    df['transaction_code'] = (df[column_name].astype(str)+'hsr569**').progress_apply(lambda x: md5(x.encode()).hexdigest())
    print('*'*50 + '  Completed transaction_code creation  ' + '*'*50) 
    return df

def email_hash(df, column_name = 'email'):

    """
        Hashing email to user_code with md5(x.encode()).hexdigest() method

        Input:
            df           : Dataframe on which amount should be cleaned
            column_name  : Column on which the function to be done

        Output:
            df           : Returns existing Dataframe with transaction_code column

    """
    df['user_code']=(df[column_name].astype(str)+'hsr569**').progress_apply(lambda x: md5(x.encode()).hexdigest())
    print('*'*50 + '  Completed user_code creation  ' + '*'*50) 
    return df

def expand_items(df):

    """
            Raw data produced by parsers contain one order per row each order might contain 
            more than one product. This function converts it to a different data, which 
            contain one product per row. Input must be a pandas dataframe and output is a dataframe.

            Input:
                df           : Dataframe on which items should be expanded

            Output:
                df           : Returns existing Dataframe with expanded rows for each product 

            Note:
                The column should be items on dataframe
        """
    df.dropna(subset=['items'],inplace=True)
    try:
        df['items'] = df['items'].apply(lambda x: ast.literal_eval(x))
    except :
        pass
    mn_lst=[]
    for oid,its in zip(df['order_id'].values,df['items'].values):
        data=dict()
        for i in its:
            data=i
            data['order_id']=oid
            mn_lst.append(data)
    item_df=pd.DataFrame(mn_lst)
    print('*'*50 + '  Completed expand_items function  ' + '*'*50) 
    return pd.merge(df,item_df[item_df.columns],on='order_id',how='left')

def product_seq(df):
    
    """
           Maps serial numbers to the products in each order and this is stored in column 
           named product_seq. Input must be a pandas dataframe and output is a dataframe.

            Input:
                df           : Dataframe on which product sequence to be mapped

            Output:
                df           : Returns existing Dataframe with product sequence column

    """
    le = LabelEncoder()
    tc=le.fit_transform(df['order_id'].astype(str))
    pt = df['company'].apply(lambda x: x[:2].upper())
    df['test_column']=[p+'-'+"{:0>7}".format(t) for t,p in zip(tc,pt)]
    del tc,pt
    df.sort_values('test_column',inplace=True)
    gr1=df.groupby('test_column')
    sq=[]
    for tc,tc_df in tqdm(gr1):
        lt=list(range(len(tc_df['product_name'])))
        sq += lt
    df['product_seq']=sq
    df['product_seq'] += 1
    del df['test_column']
    print('*'*50 + '  Completed product_seq creation  ' + '*'*50) 

    return df

def primary_key(df):

    """
            combines transaction code and product sequence

            Input:
                df           : Dataframe on which primary_key should be calculated

            Output:
                df           : Returns existing Dataframe with primary_key column

            Note:
                The dataframe should have transaction_code and product_seq column on it
    """
    df['primary_key'] = df['transaction_code'] + "-" + df["product_seq"].astype(str)
    print('*'*50 + '  Completed primary_key creation  ' + '*'*50)       
    return df

def ext_pin(se):
    '''Extracts postal codes from address if it is available. To be applied to column containing address.'''
    pin=re.compile(r'\b\d{6}\b')
    try:
        match_obj=pin.findall(se)
        if not match_obj:
            match_obj=['']
    except TypeError:
        match_obj=['']
    return match_obj[-1]

def clean_ps(pn):
    '''Removes wrong postal codes. To be applied to column containing postal codes.'''
    if type(pn)==str:
        if (len(pn)==6 and pn.isdigit()):
            return pn
        else:
            return nan
    else:
        return nan

def postal_code_extraction(df, column_name):
    
    """
            cleaning postal code from address

            Input:
                df           : Dataframe on which pincode to be extracted from .
                column_name  : Name of the column the pincode to be extracted.

            Output:
                df           : Returns existing Dataframe postal_code
    """
    df['address'] = df[column_name].str.lower()
    df['address']=df['address'].astype(str).str.lower().str.strip().apply(lambda x: " ".join(x.split()))
    df['postal_code']=df['address'].apply(lambda x: ext_pin(x))
    df['postal_code']=df['postal_code'].apply(lambda x: clean_ps(x))
    print('*'*50 + '  Completed cleaning amount column  ' + '*'*50)
    return df

def cleaning_amount_columns(df, columns):
    """
        Removing unwanted characters from amount column and giving float output

        Input:
            df           : Dataframe on which amount should be cleaned
            columns      : Columns on which the function to be done

        Output:
            df           : Returns existing Dataframe with cleaned rs column

        Note:
            The input of the columns should be of list
    """

    # iterating columns 
    for i in columns:
        df[i] = df[i].astype(str).str.replace(',', '')
        df[i] = df[i].astype(str).str.extract('(\d+.\d+|\d+)', expand=False).astype(float)
        print('*'*50 + '  Completed cleaning amount column  ' + '*'*50)

    return df

def drop_duplicate_rows(df, column_name = 'order_id'):
    """
        droping duplicate rows from the dataframe

        Input:
            df           : Dataframe on which month, year, day column should be mapped
            column_name  : Column name on which duplicate rows should removed, by default column name is order_id

        Output:
            df           : Returns existing Dataframe with duplicates removed

        Note:
            Mention the column name from which the duplicates to be removed
    """

    df.drop_duplicates(subset=[column_name],inplace=True)
    df.reset_index(drop=True,inplace=False)
    print('*'*50 + '  Completed dropping duplicates  ' + '*'*50)

    return df

def month_year_day(df, column_name = 'order_timestamp'):
    """
        expanding month, year and day from email_time

        Input:
            df           : Dataframe on which month, year, day column should be mapped
            column_name  : Column name on which order_timestamp is present, by default column name is order_timestamp

        Output:
            df           : Returns existing Dataframe with mapped data

        Note:
            Input DataFrame should have order_timestamp column else column_name variable should
            be inputed with respective column
    """

    # converting string column to datatime format
    eml_tm = pd.to_datetime(df[column_name], infer_datetime_format=True)
    df['month'] = pd.DatetimeIndex(eml_tm).month
    df['year'] = pd.DatetimeIndex(eml_tm).year
    df['day'] = pd.DatetimeIndex(eml_tm).day
    print('*'*50 + '  Completed month_year_day creation  ' + '*'*50)

    return df

def email_time_clean(df, column_name = 'order_timestamp'):
    """
        cleaning the email time from .000Z to +00:00

        Input:
            df           : Dataframe on which order_timestamp to be cleaned
            column_name  : Column name on which order_timestamp is present, by default column name is order_timestamp

        Output:
            df           : Returns existing Dataframe with cleaned timestamp

        Note:
            Input DataFrame should have order_timestamp column else column_name variable should
            be inputed with respective column
        """
    df[column_name] = df[column_name].str.replace('.000Z','+00:00',regex=False)
    print('*'*50 + '  Completed email_time clean  ' + '*'*50)

    return df


def city_tier_mapping(df):
    """     
        Mapping Tier details to the respective city

        Input:
            df    : Dataframe on which tier to be mapped

        Output:
            df_1  : Returns existing df with tier details 

        Note:
            Dataframe should have city column
        """
        
    # Reading tier details csv
    with resources.path('vumonics_modules','tier.csv') as fd:
        tier  = pd.read_csv(fd,dtype=str)
    # checking for duplicates in city and dropping
    tier.drop_duplicates(subset = ['city'], inplace = True)
    # merging the tier details with dataframe
    df = df.merge(tier, on = 'city', how = 'left')
    print('*'*50 + '  Completed Tier mapping  ' + '*'*50)

    return df


def postal_code_tier_mapping(df):
    """     
        Mapping Tier details to the respective postal_code

        Input:
            df    : Dataframe on which tier to be mapped

        Output:
            df  : Returns existing df with tier details 

        Note:
            Dataframe should have postal_code column
    """

    # Reading tier details csv
    with resources.path('vumonics_modules','tier.csv') as fd:
        tier  = pd.read_csv(fd,dtype=str)
    # checking for duplicates in postal code and dropping
    tier.drop_duplicates(subset = ['postal_code'], inplace = True)
    # making postal code column to string and if '.0' is present it will be striped
    df.postal_code = df.postal_code.astype(str).str.strip('.0')
    tier.postal_code = tier.postal_code.astype(str).str.strip('.0')
    # merging the tier details with dataframe
    df = df.merge(tier, on = 'postal_code', how = 'left')
    print('*'*50 + '  Completed Tier mapping  ' + '*'*50)

    return df

def grabbing_html(df,  mid_value,  json_path):
    """
        Returns html file from json

        Input:
            df           : dataframe from which the value and column is given
            mid_value    : column value to look at .ie(mid of the email receipt)
            json_path    : path of the json from which the dataframe is created

        Output:
            Saves html file 

        Note:
            mid is mandatory in dataframe
    """

    # getting file name on folder
    files = os.listdir(json_path)
    st = df[df['mid'] == mid_value][0:1]['order_timestamp'].values[0].split('+')[0]
    count = 0
    # iterating all json files
    for file_name in tqdm(files):
        # checking for required json
        if 'parsed' not in file_name and 'year-'+st.split('-')[0]+'_month-' +st.split('-')[1] in file_name:
            with open(json_path+file_name, encoding="utf8") as f:
                data = json.load(f)
            # iterating individual data in json
            for j in data:
                site_json = json.loads(j)
                # checking mid on json
                if mid_value in site_json.get('mid'):
                    count+=1
                    test_da = site_json.get('html')
                    f = open(mid_value + '_' + str(count) +'.html', 'w')
                    print('html saved ==> ', mid_value + '_' + str(count) +'.html')
                    f.write(test_da)
                    f.close()
        else:
            pass

    print('*'*50 + '  Completed html saving process  ' + '*'*50)

def user_data_mapping(df, user_data, column_name = 'email'):
    """     
        Mapping User details to the email_id

        Input:
            df           : Dataframe on which email to be mapped
            column_name  : Column name on which email is present, by default column name is email
            user_data    : Path for the user data file

        Output:
            df         : Returns existing Dataframe with user details 

        Note:
            Input DataFrame should have email column else column_name variable should
            be inputed with respective column
    """

    # checking for column name email
    if 'email' not in set(df.columns):
        print('email column name is not present, add a resective column name on column_name variable \n ie ==> user_data_mapping(column_name = "user_email"')
    else:
        # checking for duplicates in email and dropping
        user_data.drop_duplicates(subset = ['user_email'], inplace = True)
        user_data.rename(columns = {'user_email' : column_name}, inplace = True)
        # merging the user details with dataframe
        df = df.merge(user_data, on = column_name, how = 'left')
        print('*'*50 + '  Completed user_data mapping  ' + '*'*50)

    return df