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




class data_prep:
    '''
    Available Functions:
        tansaction_code_hash()
        user_data_mapping()
        grabbing_html()
        postal_code_tier_mapping()
        city_tier_mapping()
        email_time_clean()
        month_year_day()
        drop_duplicate_rows()
        cleaning_amount_columns()
        postal_code_city_map()
        primary_key()
        product_seq()
        expand_items()
        email_hash()

    '''







    def __init__(self,df):
        self.df = df
#*************** City with Pincodes ***************************************#
        with resources.path('vumonics_modules','all_city_pin.csv') as fd:
            city = pd.read_csv(fd,dtype=str)
#*************** City Picodes and Tier ***************************************#
        with resources.path('vumonics_modules','tier.csv') as fd:
            tier  = pd.read_csv(fd,dtype=str)
        self.city = city
        self.tier = tier
        print("its working")
        print("avaliable functions")
        print("initialize a variable with data_prep and follow the process")
        return 

#************************************************************************************************************************************************#

    def tansaction_code_hash(self):
        """Hashing  transaction code"""
        self.df['transaction_code']=("#"+self.df['order_id'].astype(str)+'hsr569**').progress_apply(lambda x: md5(x.encode()).hexdigest())
        return self.df
#***********************************************************************************************************************************************#
    def email_hash(self):
        """Hashing Email """
        self.df['user_code']=(self.df['email'].astype(str)+'hsr569**').progress_apply(lambda x: md5(x.encode()).hexdigest())
        return self.df

#***********************************************************************************************************************************************#    
    def expand_items(self):
        '''Raw data produced by parsers contain one order per row each order might contain more than one product. This function converts it to a different data, which contain one product per row. Input must be a pandas dataframe and output is a dataframe.
        
        Note:- The dictionaries in items column must not contain “None”. All dictionaries must be converted from string to python format (by using ast.literal_eval).
        '''
        self.df.dropna(subset=['items'],inplace=True)
        try:
            self.df['items'] = self.df['items'].apply(lambda x: ast.literal_eval(x))
            mn_lst=[]
            for oid,its in zip(self.df['order_id'].values,self.df['items'].values):
                data={}
                for i in its:
                    data=i
                    data['order_id']=oid
                    mn_lst.append(data)
            item_df=pd.DataFrame(mn_lst)
        except:
            mn_lst=[]
            for oid,its in zip(self.df['order_id'].values,self.df['items'].values):
                data={}
                for i in its:
                    data=i
                    data['order_id']=oid
                    mn_lst.append(data)
            item_df=pd.DataFrame(mn_lst)
        self.df = pd.merge(self.df,item_df[item_df.columns],on='order_id',how='left')
        return self.df


#******************************************************************************************************************************************************************#




    def product_seq(self):
        '''Maps serial numbers to the products in each order and this is stored in column named product_seq. 
        Input must be a pandas dataframe and output is a dataframe.'''
        le = LabelEncoder()
        tc=le.fit_transform(self.df['order_id'].astype(str))
        pt = self.df['company'].apply(lambda x: x[:2].upper())
        self.df['transaction_code']=[p+'-'+"{:0>7}".format(t) for t,p in zip(tc,pt)]
        del tc,pt
        self.df.sort_values('transaction_code',inplace=True)
        gr1=self.df.groupby('transaction_code')
        sq=[]
        for tc,tc_df in tqdm(gr1):
            lt=list(range(len(tc_df['product_name'])))
            sq += lt
        self.df['product_seq']=sq
        self.df['product_seq'] += 1
        self.df['transaction_code']=(self.df['order_id'].astype(str)+'hsr569**').progress_apply(lambda x: md5(x.encode()).hexdigest())

        return self.df


#****************************************************************************************************************#



    def primary_key(self):
        '''combines sequence transaction code and product sequence'''
        self.df['primary_key'] = self.df['transaction_code']+"-"+self.df["product_seq"].astype(str)       
        return self.df

#******************************************************************************************************************#
    def clean_ps(self,pn):
        '''Removes wrong postal codes. 
        To be applied to column containing postal codes.'''
        if type(pn) != str:
            return None
        if (len(pn)==6 and pn.isdigit()):
            return pn
        else:
            return None
#*****************************************************************************************************************#
    def ext_pin(self,se):
        '''Extracts postal codes from address if it is available. 
        
        To be applied to column containing address.'''
        pin=re.compile(r'\b\d{6}\b')
        try:
            match_obj=pin.findall(se)
            if not match_obj:
                match_obj=['']
        except TypeError:
            match_obj=['']
        return match_obj[-1]
#*******************************************************************************************************************#
    def postal_code_city_map(self,column_name):
        """cleaning postal code from address"""
        self.df[column_name] = self.df[column_name].astype(str).str.lower()
        self.df[column_name]=self.df[column_name].astype(str).str.lower().str.strip().apply(lambda x: " ".join(x.split()))
        self.df[column_name.rstrip("address")+"postal_code"]=self.df[column_name].apply(lambda x: self.ext_pin(x))
        self.df[column_name.rstrip("address")+"postal_code"]=self.df[column_name.rstrip("address")+"postal_code"].apply(lambda x: self.clean_ps(x))
        self.df[column_name.rstrip("address")+"postal_code"]=self.df[column_name.rstrip("address")+"postal_code"].astype(str).str.rstrip(".0")
        self.df.rename(columns={column_name.rstrip("address")+"postal_code":'postal_code'},inplace = True)
        self.df = pd.merge(self.df,self.city,on='postal_code',how='left')
        '''Maps postal code from internet if it is not mapped from csv file.'''
        ps_l=self.df[(self.df['postal_code'].notnull()) & (self.df['city'].isnull())]['postal_code'].unique()
        nomi = pgeocode.Nominatim('IN')
        pg=nomi.query_postal_code(ps_l)[['postal_code','county_name']]
        pg=pg.dropna(subset=['county_name']).reset_index(drop=True)
        self.df = pd.merge(self.df,pg,on = 'postal_code',how = 'left')
        self.df['county_name']=self.df['county_name'].str.lower()
        self.df['city'].fillna(self.df['county_name'],inplace=True)
        del self.df['county_name']
        self.df.rename(columns={'postal_code':column_name.rstrip("address")+"postal_code"},inplace = True)
        self.df.rename(columns={'city':column_name.rstrip("address")+"city"},inplace = True)
        self.df= self.df.replace("None",np.nan)
        return self.df
#**********************************************************************************************************************#
    def cleaning_amount_columns(self, columns):
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
            self.df[i] = self.df[i].astype(str).str.replace(',', '')
            self.df[i] = self.df[i].astype(str).str.extract('(\d+.\d+|\d+)', expand=False).astype(float)
            print('*'*50 + '  Completed cleaning amount column  ' + '*'*50)

        return self.df
#**********************************************************************************************************************************#    
    def drop_duplicate_rows(self, column_name = 'order_id'):
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

        self.df.drop_duplicates(subset=[column_name],inplace=True)
        self.df.reset_index(drop=True,inplace=False)
        print('*'*50 + '  Completed dropping duplicates  ' + '*'*50)

        return self.df
#*************************************************************************************************************************************#
    def month_year_day(self, column_name = 'order_timestamp'):
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
        eml_tm = pd.to_datetime(self.df[column_name], infer_datetime_format=True)
        self.df['month'] = pd.DatetimeIndex(eml_tm).month
        self.df['year'] = pd.DatetimeIndex(eml_tm).year
        self.df['day'] = pd.DatetimeIndex(eml_tm).day
        print('*'*50 + '  Completed month_year_day creation  ' + '*'*50)

        return self.df
#*************************************************************************************************************************************#    
    def email_time_clean(self, column_name = 'order_timestamp'):
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
        print('hi')
        self.df[column_name] = self.df[column_name].str.replace('.000Z','+00:00',regex=False)
        print('*'*50 + '  Completed email_time clean  ' + '*'*50)

        return self.df
#**************************************************************************************************************************************#    
  
    def city_tier_mapping(self):
        """     
            Mapping Tier details to the respective city

            Input:
                df    : Dataframe on which tier to be mapped

            Output:
                df_1  : Returns existing df with tier details 

            Note:
                Dataframe should have city column
            """
        # checking for duplicates in city and dropping
        self.tier.drop_duplicates(subset = ['city'], inplace = True)
        # merging the tier details with dataframe
        self.df = self.df.merge(self.tier, on = 'city', how = 'left')
        print('*'*50 + '  Completed Tier mapping  ' + '*'*50)

        return self.df
#************************************************************************************************************************#    
    def postal_code_tier_mapping(self):
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
        self.tier
        # checking for duplicates in postal code and dropping
        self.tier.drop_duplicates(subset = ['postal_code'], inplace = True)
        # making postal code column to string and if '.0' is present it will be striped
        self.df.postal_code = self.df.postal_code.astype(str).str.strip('.0')
        self.tier.postal_code = self.tier.postal_code.astype(str).str.strip('.0')
        # merging the tier details with dataframe
        self.df = self.df.merge( self.tier, on = 'postal_code', how = 'left')
        print('*'*50 + '  Completed Tier mapping  ' + '*'*50)

        return self.df
    
#*****************************************************************************************************************#
    def grabbing_html(self,  mid_value,  json_path):
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
        st = self.df[self.df['mid'] == mid_value][0:1]['order_timestamp'].values[0].split('+')[0]
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
#**************************************************************************************************************************#

    def user_data_mapping(self, user_data, column_name = 'email'):
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
        if 'email' not in set(self.df.columns):
            print('email column name is not present, add a resective column name on column_name variable \n ie ==> user_data_mapping(column_name = "user_email"')
        else:
            # checking for duplicates in email and dropping
            user_data.drop_duplicates(subset = ['user_email'], inplace = True)
            user_data.rename(columns = {'user_email' : column_name}, inplace = True)
            # merging the user details with dataframe
            self.df = self.df.merge(user_data, on = column_name, how = 'left')
            print('*'*50 + '  Completed user_data mapping  ' + '*'*50)

        return self.df

