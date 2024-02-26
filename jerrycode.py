import os
import json
import pandas as pd
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import requests
import json
import numpy as np
import matplotlib.pyplot as plt

def agg_insurance():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/aggregated/insurance/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"transaction_name":[],"count":[],"amount":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['transactionData']:
                    trans_name=i['name']
                    count=i['paymentInstruments'][0]['count']
                    amount=i['paymentInstruments'][0]['amount']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column["transaction_name"].append(trans_name)
                    column["count"].append(count)
                    column["amount"].append(amount)
    agg_ins_data=pd.DataFrame(column)
    agg_ins_data["state"].unique()
    agg_ins_data["state"]=agg_ins_data["state"].str.replace("-"," ")
    agg_ins_data["state"]=agg_ins_data["state"].str.title()
    agg_ins_data["state"]=agg_ins_data["state"].str.replace("&","and")

    agg_ins_table='''CREATE TABLE if not exists aggregated_insurance(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    Transaction_Name varchar(50),
                                                    Count bigint,
                                                    Amount bigint
                                                    )'''
    cursor.execute(agg_ins_table)
    connection.commit() 

    for index, row in agg_ins_data.iterrows():
        insert_query = '''INSERT INTO aggregated_insurance(State,Year,Quarter,Transaction_Name,Count,Amount)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['transaction_name'],
            row['count'],
            row['amount']
        ))

    connection.commit()
    return agg_ins_data

def agg_trans():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/aggregated/transaction/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"transaction_name":[],"count":[],"amount":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['transactionData']:
                    name=i['name']
                    count=i['paymentInstruments'][0]['count']
                    amount=i['paymentInstruments'][0]['amount']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column["transaction_name"].append(name)
                    column["count"].append(count)
                    column["amount"].append(amount)
    agg_trans_data=pd.DataFrame(column)
    agg_trans_data["state"].unique()
    agg_trans_data["state"]=agg_trans_data["state"].str.replace("-"," ")
    agg_trans_data["state"]=agg_trans_data["state"].str.title()
    agg_trans_data["state"]=agg_trans_data["state"].str.replace("&","and")
   

    agg_trans_table='''CREATE TABLE if not exists aggregated_transaction(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    Transaction_Name varchar(150),
                                                    Count bigint,
                                                    Amount bigint
                                                    )'''
    cursor.execute(agg_trans_table)
    connection.commit()

    for index, row in agg_trans_data.iterrows():
        insert_query = '''INSERT INTO aggregated_transaction(State,Year,Quarter,
                                                            Transaction_Name,Count,Amount)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['transaction_name'],
            row['count'],
            row['amount']
        ))
    connection.commit()
    return agg_trans_data

def agg_user():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    
    path2="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/aggregated/user/country/india/state/"
    user_list=os.listdir(path2)
    columns={"state":[],"year":[],"quarter":[],"brand_name":[],"count":[],"percentage":[]}
    for state in user_list:
        state_link=path2+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                file=years_link+jsonfile
                data=open(file,"r")
                B=json.load(data)
                try:
                    for i in B['data']['usersByDevice']:
                        brand=i['brand']
                        count=i['count']
                        percentage=i['percentage']

                        columns["state"].append(state)
                        columns["year"].append(years)
                        columns["quarter"].append(int(jsonfile.strip(".json")))
                        columns["brand_name"].append(brand)
                        columns["count"].append(count)
                        columns["percentage"].append(percentage)
                except:
                    pass
    user_data=pd.DataFrame(columns)
    user_data["state"].unique()
    user_data["state"]=user_data["state"].str.replace("-"," ")
    user_data["state"]=user_data["state"].str.title()
    user_data["state"]=user_data["state"].str.replace("&","and")
    
    agg_user_table='''CREATE TABLE if not exists aggregated_user(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    Brand_Name varchar(150),
                                                    Count bigint,
                                                    Percentage FLOAT
                                                    )'''
    cursor.execute(agg_user_table)
    connection.commit()

    for index, row in user_data.iterrows():
        insert_query = '''INSERT INTO aggregated_user(State,Year,Quarter,
                                                            Brand_Name,Count,Percentage)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['brand_name'],
            row['count'],
            row['percentage']
        ))
    connection.commit()
    return user_data    

def map_insurance():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    
    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/map/insurance/hover/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"district_name":[],"count":[],"amount":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['hoverDataList']:
                    dis_name=i['name']
                    count=i['metric'][0]['count']
                    amount=i['metric'][0]['amount']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column["district_name"].append(dis_name)
                    column["count"].append(count)
                    column["amount"].append(amount)
    map_ins_data=pd.DataFrame(column)
    map_ins_data["state"].unique()
    map_ins_data["state"]=map_ins_data["state"].str.replace("-"," ")
    map_ins_data["state"]=map_ins_data["state"].str.title()
    map_ins_data["state"]=map_ins_data["state"].str.replace("&","and")

    map_ins_table='''CREATE TABLE if not exists map_insurance(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    District_Name varchar(150),
                                                    Count bigint,
                                                    Amount bigint
                                                    )'''
    cursor.execute(map_ins_table)
    connection.commit()

    for index, row in map_ins_data.iterrows():
        insert_query = '''INSERT INTO map_insurance(State,Year,Quarter,
                                                            District_Name,Count,Amount)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['district_name'],
            row['count'],
            row['amount']
        ))
    connection.commit()
    return map_ins_data

def map_trans():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    
    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/map/transaction/hover/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"district_name":[],"count":[],"amount":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['hoverDataList']:
                    name=i['name']
                    count=i['metric'][0]['count']
                    amount=i['metric'][0]['amount']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column["district_name"].append(name)
                    column["count"].append(count)
                    column["amount"].append(amount)
    map_trans_data=pd.DataFrame(column)
    map_trans_data["state"].unique()
    map_trans_data["state"]=map_trans_data["state"].str.replace("-"," ")
    map_trans_data["state"]=map_trans_data["state"].str.title()
    map_trans_data["state"]=map_trans_data["state"].str.replace("&","and")
    
    map_trans_table='''CREATE TABLE if not exists map_transaction(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    District_Name varchar(150),
                                                    Count bigint,
                                                    Amount decimal(65,10)
                                                    )'''
    cursor.execute(map_trans_table)
    connection.commit()

    for index, row in map_trans_data.iterrows():
        insert_query = '''INSERT INTO map_transaction(State,Year,Quarter,
                                                            District_Name,Count,Amount)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['district_name'],
            row['count'],
            row['amount']
        ))

    connection.commit()
    return map_trans_data

def map_users():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/map/user/hover/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"district_name":[],"registered_users":[],"app_opens":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['hoverData'].items():
                    dist_name=i[0]
                    reg_users=i[1]['registeredUsers']
                    app_opens=i[1]['appOpens']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column["district_name"].append(dist_name)
                    column["registered_users"].append(reg_users)
                    column["app_opens"].append(app_opens)

    map_users_data=pd.DataFrame(column)
    map_users_data["state"].unique()
    map_users_data["state"]=map_users_data["state"].str.replace("-"," ")
    map_users_data["state"]=map_users_data["state"].str.title()
    map_users_data["state"]=map_users_data["state"].str.replace("&","and")

    map_users_table='''CREATE TABLE if not exists map_users(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    District_Name varchar(150),
                                                    Registered_Users bigint,
                                                    App_Opens bigint
                                                    )'''
    cursor.execute(map_users_table)
    connection.commit()
    

    for index, row in map_users_data.iterrows():
        insert_query = '''INSERT INTO map_users(State,Year,Quarter,
                                                            District_Name,Registered_Users,App_Opens)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['district_name'],
            row['registered_users'],
            row['app_opens']
        ))

    connection.commit()
    return map_users_data

def top_insurance():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()

    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/top/insurance/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"district_name":[],"count":[],"amount":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['districts']:
                    entity_name=i['entityName']
                    count=i['metric']['count']
                    amount=i['metric']['amount']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column['district_name'].append(entity_name)
                    column['count'].append(count)
                    column['amount'].append(amount)

    top_ins_data=pd.DataFrame(column)
    top_ins_data["state"].unique()
    top_ins_data["state"]=top_ins_data["state"].str.replace("-"," ")
    top_ins_data["state"]=top_ins_data["state"].str.title()
    top_ins_data["state"]=top_ins_data["state"].str.replace("&","and")

    top_ins_table='''CREATE TABLE if not exists top_insurance(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    District_Name varchar(50),
                                                    Count bigint,
                                                    Amount bigint
                                                    )'''
    cursor.execute(top_ins_table)
    connection.commit() 

    for index, row in top_ins_data.iterrows():
        insert_query = '''INSERT INTO top_insurance(State,Year,Quarter,District_Name,Count,Amount)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['district_name'],
            row['count'],
            row['amount']
        ))
    connection.commit()
    return top_ins_data

def top_trans():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()

    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/top/transaction/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"district_name":[],"count":[],"amount":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)

                for i in A['data']['districts']:
                    entity_name=i['entityName']
                    count=i['metric']['count']
                    amount=i['metric']['amount']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column['district_name'].append(entity_name)
                    column['count'].append(count)
                    column['amount'].append(amount)

    top_trans_data=pd.DataFrame(column)
    top_trans_data["state"].unique()
    top_trans_data["state"]=top_trans_data["state"].str.replace("-"," ")
    top_trans_data["state"]=top_trans_data["state"].str.title()
    top_trans_data["state"]=top_trans_data["state"].str.replace("&","and")

    top_trans_table='''CREATE TABLE if not exists top_transaction(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    District_Name varchar(150),
                                                    Count bigint,
                                                    Amount decimal(65,10)
                                                    )'''
    cursor.execute(top_trans_table)
    connection.commit()

    for index, row in top_trans_data.iterrows():
        insert_query = '''INSERT INTO top_transaction(State,Year,Quarter,
                                                            District_Name,Count,Amount)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['district_name'],
            row['count'],
            row['amount']
        ))
    connection.commit()
    return top_trans_data

def top_users():
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()
    path="C:/Users/91822/OneDrive/Documents/Capstone-02/Capstone2/pulse/data/top/user/country/india/state/"
    path_list=os.listdir(path)
    column={"state":[],"year":[],"quarter":[],"pincodes":[],"registered_users":[]}
    for state in path_list:
        state_link=path+state+"/"
        state_list=os.listdir(state_link)
        for years in state_list:
            years_link=state_link+years+"/"
            years_list=os.listdir(years_link)
            for jsonfile in years_list:
                files=years_link+jsonfile
                data=open(files,"r")
                A=json.load(data)
                for i in A['data']['pincodes']:
                    name=i['name']
                    reg_users=i['registeredUsers']

                    column["state"].append(state)
                    column["year"].append(years)
                    column["quarter"].append(int(jsonfile.strip(".json")))
                    column["pincodes"].append(name)
                    column["registered_users"].append(reg_users)
    top_users_data=pd.DataFrame(column)
    top_users_data["state"].unique()
    top_users_data["state"]=top_users_data["state"].str.replace("-"," ")
    top_users_data["state"]=top_users_data["state"].str.title()
    top_users_data["state"]=top_users_data["state"].str.replace("&","and")

    top_users_table='''CREATE TABLE if not exists top_users(
                                                    State varchar(50),
                                                    Year int,
                                                    Quarter int,
                                                    Pincodes bigint,
                                                    Registered_Users bigint
                                                    )'''
    cursor.execute(top_users_table)
    connection.commit()
    

    for index, row in top_users_data.iterrows():
        insert_query = '''INSERT INTO top_users(State,Year,Quarter,
                                                            Pincodes,Registered_Users)
                          VALUES (%s, %s, %s, %s, %s)'''
        cursor.execute(insert_query, (
            row['state'],
            row['year'],
            row['quarter'],
            row['pincodes'],
            row['registered_users']
            
        ))
    connection.commit()   
    return top_users_data

connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
cursor=connection.cursor()
#agg_ins_df
cursor.execute("SELECT * FROM aggregated_insurance")
table1=cursor.fetchall()
agg_ins_df=pd.DataFrame(table1,columns=("State","Year","Quarter","Transaction_Name","Count","Amount"))
connection.commit()
#agg_trans_df
cursor.execute("SELECT * FROM aggregated_transaction")
table2=cursor.fetchall()
agg_trans_df=pd.DataFrame(table2,columns=("State","Year","Quarter","Transaction_Name",
                                          "Count","Amount"))
connection.commit()
#agg_user_df
cursor.execute("SELECT * FROM aggregated_user")
table3=cursor.fetchall()
agg_user_df=pd.DataFrame(table3,columns=("State","Year","Quarter","Brand_Name","Count","Percentage"))
connection.commit()


#map_ins_df
cursor.execute("SELECT * FROM map_insurance")
table4=cursor.fetchall()
map_ins_df=pd.DataFrame(table4,columns=("State","Year","Quarter","District_Name","Count","Amount"))
connection.commit()
#map_trans_df
cursor.execute("SELECT * FROM map_transaction")
table5=cursor.fetchall()
map_trans_df=pd.DataFrame(table5,columns=("State","Year","Quarter","District_Name","Count","Amount"))
connection.commit()
#map_users_df
cursor.execute("SELECT * FROM map_users")
table6=cursor.fetchall()
map_users_df=pd.DataFrame(table6,columns=("State","Year","Quarter","District_Name","Registered_Users","App_Opens"))
connection.commit()

#top_ins_df
cursor.execute("SELECT * FROM top_insurance")
table7=cursor.fetchall()
top_ins_df=pd.DataFrame(table7,columns=("State","Year","Quarter","District_Name","Count","Amount"))
connection.commit()
#top_trans_df
cursor.execute("SELECT * FROM top_transaction")
table8=cursor.fetchall()
top_trans_df=pd.DataFrame(table8,columns=("State","Year","Quarter","District_Name","Count","Amount"))
connection.commit()
#top_users_df
cursor.execute("SELECT * FROM top_users")
table9=cursor.fetchall()
top_users_df=pd.DataFrame(table9,columns=("State","Year","Quarter","Pincodes","Registered_Users"))
connection.commit()

def total_trans():
    sql_query="SELECT SUM(Transaction_Count) as total_transaction FROM Aggregated_transaction"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    res_in_float=float(res_in_value)
    total_count_in_c=np.round(res_in_float / 10_000_000,2)
    #st.header('Transactions')
    st.subheader('All Phonepe Transaction ' + str(total_count_in_c) + ' Crores') 
def total_payment_value():
    sql_query="SELECT SUM(Transaction_Amount) as total_amount from Aggregated_transaction"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    res_in_float=float(res_in_value)
    total_amount_in_c=np.round(res_in_float / 10_000_000,2)
    st.subheader('Total Payment Value ' + str(total_amount_in_c) + ' Crores')
def avg_trans_value():
    sql_query="Select AVG(Transaction_Amount) as avg_amount from Aggregated_transaction"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    res_in_float=float(res_in_value)
    total_amount_in_c=np.round(res_in_float / 10_000_000,2)
    st.subheader('Average Transaction Value: ' + str(total_amount_in_c) + ' Crores')

def categories():
    sql_query='''SELECT Transaction_Type, SUM(Transaction_Amount) AS total_amount 
    FROM aggregated_transaction 
    GROUP BY Transaction_Type'''
    cursor.execute(sql_query)
    results = cursor.fetchall()
    def convert_to_crores(amount):
        return amount / 10000000 
    for row in results:
        transaction_type = row[0]
        total_amount = row[1]
        total_amount_in_crores = convert_to_crores(total_amount)
        st.subheader(f"{transaction_type} : {total_amount_in_crores} Cr")         
  
def top_ten_states():
    sql_query=('''SELECT State, SUM(Transaction_amount) AS Total_Amount
                    FROM aggregated_transaction
                    GROUP BY State
                    ORDER BY Total_Amount DESC
                    LIMIT 10;''')
    cursor.execute(sql_query)
    res=cursor.fetchall()
    df = pd.DataFrame(res, columns=['State', 'Total_Amount'])
    df.index = df.index + 1
    st.write(df)

def top_ten_districts():
    sql_query=('''SELECT District_Name, SUM(Amount) AS Total_Amount
                    FROM top_transaction
                    GROUP BY District_Name
                    ORDER BY Total_Amount DESC
                    LIMIT 10;''')
    cursor.execute(sql_query)
    res=cursor.fetchall()
    df = pd.DataFrame(res, columns=['District', 'Total_Amount'])
    df.index = df.index + 1
    st.write(df)
def top_ten_pcode():
    sql_query=('''SELECT tu.Pincodes, SUM(tt.Amount) AS Total_Amount
                    FROM top_transaction AS tt
                    INNER JOIN top_users AS tu ON tt.state = tu.state
                    GROUP BY tt.District_Name, tu.Pincodes
                    ORDER BY Total_Amount DESC
                    LIMIT 10;''')
    cursor.execute(sql_query)
    res=cursor.fetchall()
    df = pd.DataFrame(res, columns=['Postal Codes', 'Total_Amount'])
    df['Postal Codes'] = df['Postal Codes'].astype(str).str.replace(',', '')
    df.index = df.index + 1
    st.write(df)
    

st.set_page_config(page_title="PHONEPE",page_icon=":iphone:",layout="wide")
selected=option_menu(menu_title="PHONEPE PULSE DATA VISUALIZATION AND EXPLORATION",
                    options=["Dash Board","Insurance","Transactions","Users","Data Analysis"],
                    icons=["window-dash","cash-coin","currency-exchange","people-fill","search"],
                    default_index=0,
                    orientation="horizontal"
                    )
if selected == "Dash Board":
    with st.sidebar:
        with st.container():
            st.title("India Phonepe Dashboard")
            video_file = open('Phonepe.mp4', 'rb')
            video_bytes = video_file.read()
            st.video(video_bytes)
            st.link_button("Install Now",
                        "https://play.google.com/store/apps/details?id=com.phonepe.app&hl=en_IN&gl=US")
    col1,col2,col3=st.columns(3)
    with col1:
        total_trans()
    with col2:
        total_payment_value()
    with col3:
        avg_trans_value()

    st.markdown("_____")
    col4,col5=st.columns([7,10])
    with col4:
        st.image("data:image/png;base64,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",
                    caption='Phonepe Categories',  
                    width=340,                     # Width of the image (in pixels)
                    use_column_width=False,        # Set it to True to adjust the width to the column width
                    clamp=False,                   # If set to True, the image will be resized to fit the specified width
                    channels='RGB',                # Color space of the image
                    output_format='auto')          # Output format: 'JPEG', 'PNG', 'WEBP', or 'auto')

    with col5:
        st.header("Categories")
        categories()
    st.markdown("____")

    col6, col7, col8 = st.columns((5, 5, 5))
    with col6:
        if st.button("Top 10 States"):
            top_ten_states()
    with col7:
        if st.button("Top 10 Districts"):
            top_ten_districts()
    with col8:
        if st.button("Top 10 Postal Codes"):
            top_ten_pcode()

if selected == "Insurance":
    



