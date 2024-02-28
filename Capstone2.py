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
import locale

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

#DASHBOARD-TOAL TRANSACTION(COUNT)
def total_trans():
    sql_query="SELECT SUM(Transaction_Count) as total_transaction FROM Aggregated_transaction"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    res_in_float=float(res_in_value)
    total_count_in_c=np.round(res_in_float / 10_000_000,2)
    st.markdown('<h1 style="color: Green; font-size: 20pt; font-weight: bold;">All Phonepe Transactions:</h1>', 
                unsafe_allow_html=True)
    st.subheader(str(total_count_in_c) + ' Crores')

#DASHBORD-TOTAL PAYMENT(AMOUNT)
def total_payment_value():
    sql_query="SELECT SUM(Transaction_Amount) as total_amount from Aggregated_transaction"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    res_in_float=float(res_in_value)
    total_amount_in_c=np.round(res_in_float / 10_000_000,2)
    st.markdown('<h1 style="color: green; font-size: 20pt; font-weight: bold;">Total Payment Value:</h1>', 
                unsafe_allow_html=True)
    st.subheader(str(total_amount_in_c) + ' Crores')

#DASHBORD-AVERAGE PAYMENT(AMOUNT)
def avg_trans_value():
    sql_query="Select AVG(Transaction_Amount) as avg_amount from Aggregated_transaction"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    res_in_float=float(res_in_value)
    total_amount_in_c=np.round(res_in_float / 10_000_000,2)
    st.markdown('<h1 style="color: green; font-size: 20pt; font-weight: bold;">Average Transaction Value</h1>',
                 unsafe_allow_html=True)
    st.subheader(str(total_amount_in_c) + ' Crores')

#DASHBORD-CATEGORIES VALUES
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
    
#DASHBOARD- TOP 10 STATES(TRAN_AMOUNT)
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
    
    fig=px.pie(df,values='Total_Amount',names='State',title='Top 10 States',
               color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Amount'],
                             labels={'Total_Amount':'Total_amount'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig,use_container_width=True)

#DASHBOARD- TOP 10 DISTRICTS(TRAN_AMOUNT)
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
    fig=px.pie(df,values='Total_Amount',names='District',title='Top 10 Districts',
               color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Amount'],
                             labels={'Total_Amount':'Total_amount'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig,use_container_width=True)

#DASHBOARD- TOP 10 POSTAL_CODES(TRAN_AMOUNT)
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
    fig=px.pie(df,values='Total_Amount',names='Postal Codes',title='Top 10 Postal Codes',
               color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Amount'],
                             labels={'Total_Amount':'Total_amount'})
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig,use_container_width=True)

#INSURANCE - QUARTER- PIE_CHART - AMOUNT
def aggregated_insurance_by_quarter_A(user_year):
    filtered_df = agg_ins_df[agg_ins_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Amount', names='Quarter', 
                                 title=f"Total Insurance Amount by Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors,hole=0.4,height=300,width=300)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )
    st.plotly_chart(fig_quarter_amount)

#INSURANCE - QUARTER- PIE_CHART - COUNT
def aggregated_insurance_by_quarter_C(user_year):
    filtered_df = agg_ins_df[agg_ins_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Count', names='Quarter', 
                                 title=f"Total Insurance Amount by Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors,hole=0.4,height=300,width=300)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )
    
    st.plotly_chart(fig_quarter_amount)

#INSURANCE - SIDEBAR - TOTAL INSURANCE PURCHASED
def ins_count():
    sql_query="select sum(Count) as total_count from aggregated_insurance"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    locale.setlocale(locale.LC_NUMERIC, 'en_IN')  # Set the locale to Indian English
    formatted_count = locale.format_string("%d", res_in_value, grouping=True)
    st.header("ALL INDIA")
    st.header("Insurance Policies Purchased(No's): " + formatted_count )

#INSURANCE - SIDEBAR -TOTAL PREMIUM VALUE
def ins_amount():
    sql_query="select sum(Amount) as total_amt from aggregated_insurance"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    total_amt_in_crore = res_in_value / 10000000  # 1 crore = 10,000,000
    formatted_total_amt = '{:,.2f}'.format(total_amt_in_crore) + 'cr'
    st.header("Total Premium Value: " + formatted_total_amt)

#INSURANCE - SIDEBAR -AVERAGE PREMIUM VALUE
def avg_amount():
    sql_query="select Avg(Amount) as total_amt from aggregated_insurance"
    cursor.execute(sql_query)
    res=cursor.fetchone()
    res_in_value=res[0]
    total_amt_in_crore = res_in_value / 10000000  # 1 crore = 10,000,000
    formatted_total_amt = '{:,.2f}'.format(total_amt_in_crore) + 'cr'
    st.header("Average Premium Value: " + formatted_total_amt)


#TRANSACTION - TOTAL TRANSACTION BY QUARTER - AMOUNT
def aggregated_transaction_by_quarter_A(user_year):
    filtered_df = agg_trans_df[agg_trans_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Amount', names='Quarter', 
                                 title=f"Aggregated Transaction Amount by Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )
    st.plotly_chart(fig_quarter_amount)

#TRANSACTION - TOTAL TRANSACTION BY QUARTER - COUNT
def aggregated_transaction_by_quarter_C(user_year):
    filtered_df = agg_trans_df[agg_trans_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Count', names='Quarter', 
                                 title=f"Aggregated Transaction Amount by Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )    
    st.plotly_chart(fig_quarter_amount)

#TRANSACTION - TOTAL TRANSACTION BY YEAR - COUNT
def map_quarter_to_numeric(quarter):
    quarter_mapping = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
    return quarter_mapping[quarter]
def aggregated_transaction_by_year_amountC():
    
    AT_grouped = agg_trans_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    AT_grouped["Quarter_numeric"] = AT_grouped["Quarter"].apply(map_quarter_to_numeric)
    fig_year_quarter_count = px.bar(AT_grouped, x="Year", y="Count",
                                    title="Total Transaction Count by Year and Quarter",
                                    height=550, width=550,
                                    color="Quarter_numeric",  # Use the numeric values for coloring
                                    barmode="stack")
    
    # Update x-axis ticks to show unique years
    fig_year_quarter_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()),  # Set tick values as unique years
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    
    st.plotly_chart(fig_year_quarter_count)

#TRANSACTION - TOTAL TRANSACTION BY YEAR - AMOUNT
def aggregated_transaction_by_year_amountA():
    # Group by both Year and Quarter
    AT_grouped = agg_trans_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    
    # Map Quarter to numeric values
    AT_grouped["Quarter_numeric"] = AT_grouped["Quarter"].apply(map_quarter_to_numeric)
    
    # Create stacked bar chart
    fig_year_quarter_count = px.bar(AT_grouped, x="Year", y="Amount",
                                    title="Total Transaction Amount by Year and Quarter",
                                    height=550, width=550,
                                    color="Quarter_numeric",  # Use the numeric values for coloring
                                    barmode="stack")
    
    # Update x-axis ticks to show unique years
    fig_year_quarter_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()),  # Set tick values as unique years
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    
    st.plotly_chart(fig_year_quarter_count)

#TRANSACTION-MAP-YEAR
def agg_transaction(user_year):
    MT=agg_trans_df[agg_trans_df["Year"]==user_year]
    MT.reset_index(drop=True,inplace=True)
    MTgroup=MT.groupby("State")[["Count","Amount"]].sum()
    MTgroup.reset_index(inplace=True)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    data=json.loads(response.content)
    state_names=[]
    for feauture in data["features"]:
        state_names.append(feauture["properties"]["ST_NM"])
        state_names.sort()

    fig_india=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Amount",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Amount"].min(),MTgroup["Amount"].max()),
                            hover_name="State",title=f"Total Amount for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india.update_geos(visible = False)
    fig_india.update_traces(hovertemplate='<b>%{hovertext}</b><br>Amount: %{z}')

    fig_india1=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Count",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Count"].min(),MTgroup["Count"].max()),
                            hover_name="State",title=f"Total Transaction Count for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india1.update_geos(visible = False)
    
    return fig_india, fig_india1

def ins_map(user_year):
    MT=agg_ins_df[agg_ins_df["Year"]==user_year]
    MT.reset_index(drop=True,inplace=True)
    MTgroup=MT.groupby("State")[["Count","Amount"]].sum()
    MTgroup.reset_index(inplace=True)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    data=json.loads(response.content)
    state_names=[]
    for feauture in data["features"]:
        state_names.append(feauture["properties"]["ST_NM"])
        state_names.sort()

    fig_india=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Amount",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Amount"].min(),MTgroup["Amount"].max()),
                            hover_name="State",title=f"Total Insurance Amount for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india.update_geos(visible = False)
    fig_india.update_traces(hovertemplate='<b>%{hovertext}</b><br>Amount: %{z}')

    fig_india1=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Count",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Count"].min(),MTgroup["Count"].max()),
                            hover_name="State",title=f"Total Insurance Transaction Count for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india1.update_geos(visible = False)
    
    return fig_india, fig_india1

def aggregated_insurance_by_year_Count():
    AT_grouped = agg_ins_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    AT_grouped["Quarter_numeric"] = AT_grouped["Quarter"].apply(map_quarter_to_numeric)
    fig_year_quarter_count = px.bar(AT_grouped, x="Year", y="Count",
                                    title="Total Insurance Transaction Count by Year and Quarter",
                                    height=550, width=550,
                                    color="Quarter_numeric",  
                                    barmode="stack")
    
    # Update x-axis ticks to show unique years
    fig_year_quarter_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()), 
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    st.plotly_chart(fig_year_quarter_count)

def aggregated_insurance_by_year_Amount():
    AT_grouped = agg_ins_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    AT_grouped["Quarter_numeric"] = AT_grouped["Quarter"].apply(map_quarter_to_numeric)
    fig_year_quarter_count = px.bar(AT_grouped, x="Year", y="Amount",
                                    title="Total Insurance Transaction Amount by Year and Quarter",
                                    height=550, width=550,
                                    color="Quarter_numeric",  
                                    barmode="stack")
    
    # Update x-axis ticks to show unique years
    fig_year_quarter_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()), 
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    st.plotly_chart(fig_year_quarter_count)

def ins_quarter_C(user_year):
    filtered_df = agg_ins_df[agg_ins_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Count', names='Quarter', 
                                 title=f"Total Insurance Policies Purchased in Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors,hole=0.4)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )    
    st.plotly_chart(fig_quarter_amount)

def ins_quarter_A(user_year):
    filtered_df = agg_ins_df[agg_ins_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Amount', names='Quarter', 
                                 title=f"Total Insurance Transaction Amount in Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors,hole=0.4)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )    
    st.plotly_chart(fig_quarter_amount)

def user_quarter_A(user_year):
    filtered_df = agg_user_df[agg_user_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Count"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#9467bd', '#d62728', '#17becf', '#e377c2']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Count', names='Quarter', 
                                 title=f"Total Phonepe Users in Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors,hole=0.4, 
                                 width=700, height=700)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )    
    st.plotly_chart(fig_quarter_amount)

def user_quarter_P(user_year):
    filtered_df = agg_user_df[agg_user_df["Year"] == user_year]
    AT_grouped_quarter = filtered_df.groupby(["Year", "Quarter"])[["Percentage"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    AT_grouped_quarter['Year'] = AT_grouped_quarter['Year'].astype(str)
    
    custom_colors = ['#9467bd', '#d62728', '#17becf', '#e377c2']
    
    # Create a pie chart
    fig_quarter_amount = px.pie(AT_grouped_quarter, values='Percentage', names='Quarter', 
                                 title=f"Total Phonepe Users Percentage in Quarter for {user_year}",
                                 color_discrete_sequence=custom_colors,hole=0.4, 
                                 width=600, height=600)
    
    fig_quarter_amount.update_traces(textposition='inside', textinfo='percent+label')
    
    fig_quarter_amount.update_layout(
        height=450,
        width=600
    )    
    st.plotly_chart(fig_quarter_amount)

def user_by_year_Count():
    AT_grouped = agg_user_df.groupby(["Year", "Quarter"])[["Count", "Percentage"]].sum()
    AT_grouped.reset_index(inplace=True)
    AT_grouped["Quarter_numeric"] = AT_grouped["Quarter"].apply(map_quarter_to_numeric)
    fig_year_quarter_count = px.bar(AT_grouped, x="Year", y="Count",
                                    title="Total Phonepe Users by Year and Quarter",
                                    height=550, width=550,
                                    color="Quarter_numeric",  
                                    barmode="stack")
    
    # Update x-axis ticks to show unique years
    fig_year_quarter_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()), 
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    st.plotly_chart(fig_year_quarter_count)
    
def user_by_year_Percentage():
    AT_grouped = agg_user_df.groupby(["Year", "Quarter"])[["Count", "Percentage"]].sum()
    AT_grouped.reset_index(inplace=True)
    AT_grouped["Quarter_numeric"] = AT_grouped["Quarter"].apply(map_quarter_to_numeric)
    fig_year_quarter_count = px.bar(AT_grouped, x="Year", y="Percentage",
                                    title="Percentage of Phonepe Users by Year and Quarter",
                                    height=550, width=550,
                                    color="Quarter_numeric",  
                                    barmode="stack")
    
    # Update x-axis ticks to show unique years
    fig_year_quarter_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()), 
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    st.plotly_chart(fig_year_quarter_count)

def users_map(user_year):
    MT=agg_user_df[agg_user_df["Year"]==user_year]
    MT.reset_index(drop=True,inplace=True)
    MTgroup=MT.groupby("State")[["Count","Amount"]].sum()
    MTgroup.reset_index(inplace=True)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    data=json.loads(response.content)
    state_names=[]
    for feauture in data["features"]:
        state_names.append(feauture["properties"]["ST_NM"])
        state_names.sort()

    fig_india=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Amount",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Amount"].min(),MTgroup["Amount"].max()),
                            hover_name="State",title=f"Total Insurance Amount for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india.update_geos(visible = False)
    fig_india.update_traces(hovertemplate='<b>%{hovertext}</b><br>Amount: %{z}')

    fig_india1=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Count",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Count"].min(),MTgroup["Count"].max()),
                            hover_name="State",title=f"Total Insurance Transaction Count for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india1.update_geos(visible = False)
    
    return fig_india, fig_india1


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
        st.image("https://cdn.zeebiz.com/sites/default/files/styles/zeebiz_850x478/public/2023/02/07/225973-phonepe-upi.png?itok=XRUCFnK4")
    with col5:
        st.markdown('<h1 style="color: green; font-size: 25pt; font-weight: bold;">Categories: </h1>',
                     unsafe_allow_html=True)
        categories()
    st.markdown("____")
    st.markdown('<h1 style="color: green; font-size: 25pt; font-weight: bold;">BASED ON TRANSACTION AMOUNT</h1>',
                 unsafe_allow_html=True)
    col6, col7, col8 = st.columns((5, 5, 5),gap='medium')
    
    with col6:
        top_ten_states()
    with col7:
        top_ten_districts()
    with col8:
        top_ten_pcode()

if selected == "Transactions":
    st.markdown('<h1 style="color: green; font-size: 25pt; font-weight: bold;">Exploring the Dynamics of PhonePe Transactions 💳📊</h1>',
                 unsafe_allow_html=True)
    col0,col01=st.columns(2)
    with col0:
        st.write("<style>div.row-widget.stRadio > div{flex-direction:row;}</style>", unsafe_allow_html=True)
        op1 = st.radio(
        "Choose Type",
        [":rainbow[Transaction Type]",":rainbow[Year Wise Data]",":rainbow[Quarter Wise Data]",
         ":rainbow[State Wise]"])
    if op1 == ":rainbow[Transaction Type]":
        col111,col112=st.columns(2)
        with col111:
            user_year1 = st.slider('Choose the Year',min_value=2018,max_value=2023)
        col1, col2 = st.columns(2)
        with col1:
            cursor.execute(f"""SELECT State, Transaction_Type, SUM(Transaction_Amount) AS Total_Amount                   
                            FROM aggregated_transaction 
                            WHERE Year = {user_year1}
                            GROUP BY State, Transaction_Type;""")
            df_amount = pd.DataFrame(cursor.fetchall(),
                                columns=["State", "Transaction_Type", "Total_Amount"])
            fig_amount = px.pie(df_amount, names="Transaction_Type", values="Total_Amount", hover_data=['State'],
                                labels={'Total_Amount': 'Total_Amount'}, hole=0.4,
                                title="Amount Spent on Various Categories", width=600, height=600)
            st.plotly_chart(fig_amount, use_container_width=True)
        
        with col2:
            cursor.execute(f"""SELECT State, Transaction_Type, SUM(Transaction_Count) AS Total_Count                   
                            FROM aggregated_transaction 
                            WHERE Year = {user_year1}
                            GROUP BY State, Transaction_Type;""")
            df_count = pd.DataFrame(cursor.fetchall(),
                                columns=["State", "Transaction_Type", "Total_Count"])
            fig_count = px.pie(df_count, names="Transaction_Type", values="Total_Count", hover_data=['State'],
                            labels={'Total_Count': 'Total_Count'}, hole=0.4,
                            title="Transaction made on Various Categories", width=600, height=600)
            st.plotly_chart(fig_count, use_container_width=True)

    elif op1==":rainbow[Quarter Wise Data]":
        col31,col32=st.columns(2)
        col3,col4=st.columns(2)
        with col31:
            user_year = st.slider("**Year**", min_value=2018, max_value=2022)
        with col3:
            aggregated_transaction_by_quarter_A(user_year)
        with col4:
            aggregated_transaction_by_quarter_C(user_year)
    elif op1 == ":rainbow[Year Wise Data]":
        col5,col6=st.columns(2)
        with col5:
            aggregated_transaction_by_year_amountC()
        with col6:
            aggregated_transaction_by_year_amountA()

    elif op1 == ":rainbow[State Wise]":
        col71,col81=st.columns(2)
        with col71:
            user_year = st.slider("**Year**", min_value=2018, max_value=2022)
        col7,col8=st.columns(2)
        fig_india, fig_india1 = agg_transaction(user_year)
        with col7:
            st.plotly_chart(fig_india)
        with col8:
            st.plotly_chart(fig_india1)

if selected == "Insurance":
    st.markdown('<h1 style="color: green; font-size: 25pt; font-weight: bold;">Analyzing Phonepe Insurance Data: Insights by State and Year📅🔍</h1>',
                 unsafe_allow_html=True)
    st.write("<style>div.row-widget.stRadio > div{flex-direction:row;}</style>", unsafe_allow_html=True)
    genre = st.radio(
    "Choose Type",
    [":rainbow[State Wise]",":rainbow[Year Wise]",":rainbow[Quarter Wise]"])
    if genre == ':rainbow[State Wise]':
        col91,col92=st.columns(2)
        with col91:
            user_year1 = st.slider('Choose the Year',min_value=2020,max_value=2023)
        ins_map1,ins_map2 = ins_map(user_year1)
        col9,col10=st.columns(2)
        col91,col92=st.columns(2)
        with col9:
            st.plotly_chart(ins_map1)
        with col10:
            st.plotly_chart(ins_map2)    
    if genre == ':rainbow[Year Wise]':
        col11,col12=st.columns(2)
        with col11:
            aggregated_insurance_by_year_Amount()
        with col12:
            aggregated_insurance_by_year_Count()
        
    if genre == ':rainbow[Quarter Wise]':
        col131,col132=st.columns(2)
        with col131:
            user_year1 = st.slider('Choose the Year',min_value=2020,max_value=2023)
        col13,col14=st.columns(2)
        with col13:
            ins_quarter_C(user_year1)
        with col14:
            ins_quarter_A(user_year1)

if selected == "Users":
    st.markdown('<h1 style="color: green; font-size: 25pt; font-weight: bold;">Exploring Total PhonePe Users 📱 Understanding the Growth and Reach📈</h1>',
                 unsafe_allow_html=True)
    st.write("<style>div.row-widget.stRadio > div{flex-direction:row;}</style>", unsafe_allow_html=True)
    option = st.radio(
    "Choose Type",
    [":rainbow[Brand Wise]",":rainbow[District Wise]",":rainbow[Quarter and Year Wise]"])   
    if option == ":rainbow[Brand Wise]":
        col1,col2=st.columns(2)
        with col1:
            user_year1 = st.slider('Choose the Year',min_value=2018,max_value=2022)
        col1, col2 = st.columns(2)
        with col1:
            cursor.execute(f"""SELECT State, Brand_Name, SUM(Count) AS Transaction_Count                   
                            FROM aggregated_user 
                            WHERE Year = {user_year1}
                            GROUP BY State, Brand_Name;""")
            df_amount = pd.DataFrame(cursor.fetchall(),
                                columns=["State", "Brand_Name", "Transaction_Count"])
            fig_amount = px.pie(df_amount, names="Brand_Name", values="Transaction_Count", hover_data=['State'],
                                labels={'Transaction_Count': 'Transaction_Count'}, hole=0.4,
                                title="Transaction Count Made on Differnt Brands",
                                width=600, height=600)
            st.plotly_chart(fig_amount, use_container_width=True)
        with col2:
                cursor.execute(f"""SELECT State, Brand_Name, Avg(Percentage) AS Avg_Percentage                   
                                FROM aggregated_user
                                WHERE Year = {user_year1}
                                GROUP BY State, Brand_Name;""")
                df_count = pd.DataFrame(cursor.fetchall(),
                                    columns=["State", "Brand_Name", "Avg_Percentage"])
                fig_count = px.pie(df_count, names="Brand_Name", values="Avg_Percentage", hover_data=['State'],
                                labels={'Avg_Percentage': 'Avg_Percentage',}, hole=0.4,
                                title="Average Transaction Percentage made by Different Brands", 
                                width=600, height=600)
                st.plotly_chart(fig_count, use_container_width=True)
    if option == ":rainbow[Quarter and Year Wise]":
        col0,col01=st.columns(2)
        with col0:
            user_year1 = st.slider('Choose the Year',min_value=2018,max_value=2022)
        col1,col2=st.columns(2)
        with col1:
            user_quarter_A(user_year1)
        with col2:
            user_by_year_Count()
    if option == ":rainbow[District Wise]":
        col0,col01=st.columns(2)
        with col0:
            cursor.execute("SELECT DISTINCT State FROM map_users")
            list_of_states = [row[0] for row in cursor.fetchall()]
            selected_state = st.selectbox("Choose One", list_of_states, index=0) 
        col1,col2=st.columns(2)
        with col1:
            cursor.execute(f"""SELECT District_Name, SUM(App_Opens) AS Total_App_Opens
                                FROM map_users
                                WHERE State = '{selected_state}'
                                GROUP BY District_Name;""")
            district_data = cursor.fetchall()
            district_df = pd.DataFrame(district_data, columns=["District", "App_Opens"])
            fig = px.bar(district_df, x='District', y='App_Opens', title='App Opens by District',
                         height=600)
            st.plotly_chart(fig, use_container_width=True)           
        with col2:
            cursor.execute(f"""SELECT District_Name, SUM(Registered_Users) AS Reg_users
                                FROM map_users
                                WHERE State = '{selected_state}'
                                GROUP BY District_Name;""")
            district_data = cursor.fetchall()
            district_df = pd.DataFrame(district_data, columns=["District", "Registered Users"])
            fig = px.bar(district_df, x='District', y='Registered Users', title='Registered Users by District',
                         height=600)
            st.plotly_chart(fig, use_container_width=True)
               
if selected == "Data Analysis":
    st.markdown('<h1 style="color: green; font-size: 25pt; font-weight: bold;">Unlocking Valuable Insights🔍: Answering Key Questions with Data Analysis📊</h1>',
                 unsafe_allow_html=True)
    st.subheader("Addressing Key Questions for Actionable Understanding")
    question = st.selectbox("Questions for Optimal Results", ("Choose one Question",
        "1. Top 10 States in Terms of Insurance Penetration",
        "2. Transaction Types with the Highest Amount Spent",
        "3. Trend Analysis of Yearly Phonepe Transaction",
        "4. Which year had a highest total number of insurance cases in the Tamil Nadu",
        "5. Which category of transactions had the lowest volume in Andaman and Nicobar Islands?",
        "6.Can you identify any notable trends or patterns in transaction volumes across different categories?",
        "7. What were the top three smartphone brands used phonepay in the year 2018 to 2023?",
        "8. What are the top 10 postal codes used spent highest amount using Phonepay?",
        "9. Which District has less Phonepe Registered Users?",
        "10. Which 10 state has the highest growth in insurance, transactions and what percentage?"
    ))
    connection = mysql.connector.connect(
        user='root',
        password='mysql@123',
        database='Phonepay',
        host='localhost'
    )
    cursor=connection.cursor()

    if question == "1. Top 10 States in Terms of Insurance Penetration":
        sql_query=('''SELECT State, SUM(Count) AS Total_count
                    FROM aggregated_insurance
                    GROUP BY State
                    ORDER BY Total_count DESC
                    LIMIT 10;''')
        cursor.execute(sql_query)
        res=cursor.fetchall()
        df = pd.DataFrame(res, columns=['State', 'Total Insurance'])
        df.index = df.index + 1
        st.write(df)
    elif question == "2. Transaction Types with the Highest Amount Spent":
        sql_query=("""SELECT Transaction_Type,SUM(Transaction_Amount) AS Total_Amount_Spent
                    FROM aggregated_transaction GROUP BY Transaction_Type ORDER BY Total_Amount_Spent DESC
                    LIMIT 1;""")
        cursor.execute(sql_query)
        res=cursor.fetchall()
        transaction_type, total_amount_spent = res[0]
        st.subheader(f"{transaction_type} - Total Amount Spent: {total_amount_spent}")
    elif question == "3. Trend Analysis of Yearly Phonepe Transaction":
        sql_query=('''SELECT Year,SUM(Transaction_Count) AS Total_Transaction_Count
                        FROM aggregated_transaction GROUP BY Year ORDER BY Year;''')
        cursor.execute(sql_query)
        res=cursor.fetchall()
        df = pd.DataFrame(res, columns=['Year', 'Total_Transaction_Count'])
        df.set_index('Year', inplace=True)
        st.line_chart(df)
        fig = px.line(df, x=df.index, y='Total_Transaction_Count', 
                      title='Trend Analysis of Yearly PhonePe Transaction')
        st.plotly_chart(fig)
    elif question == "4. Which year had a highest total number of insurance cases in the Tamil Nadu":
        sql_query = ('''SELECT Year, SUM(Count) AS Total_Insurance_Cases
                        FROM map_transaction 
                        WHERE State = 'Tamil Nadu'
                        GROUP BY Year 
                        ORDER BY Total_Insurance_Cases DESC 
                        LIMIT 1;''')
        cursor.execute(sql_query)
        res = cursor.fetchall()
        year, total_insurance_cases = res[0]
        st.subheader(f"{year} - Total Insurance Cases: {total_insurance_cases}")
    elif question == """5. Which category of transactions had the lowest volume in Andaman and Nicobar Islands?""":
        sql_query=('''SELECT Transaction_Type,SUM(Transaction_Count) AS Total_Volume
                        FROM aggregated_transaction
                        WHERE State = 'Andaman and Nicobar Islands'
                        GROUP BY Transaction_Type ORDER BY Total_Volume ASC LIMIT 1;''')
        cursor.execute(sql_query)
        res = cursor.fetchall()
        transaction_type, total_volume = res[0]
        st.subheader(f"{transaction_type} - Lowest Count {total_volume}")
    elif question == '6.Can you identify any notable trends or patterns in transaction volumes across different categories?':
        sql_query = '''SELECT Transaction_Type, SUM(Transaction_Count) AS Total_Count
               FROM aggregated_transaction
               GROUP BY Transaction_Type;'''
        cursor.execute(sql_query)
        res = cursor.fetchall()
        df = pd.DataFrame(res, columns=['Transaction_Type', 'Total_Count'])
        st.line_chart(df.set_index('Transaction_Type'))
        fig = px.line(df, x='Transaction_Type', y='Total_Count', 
                    title='Transaction Volumes Across Different Categories')
        st.plotly_chart(fig)
        st.write("""The visualization highlights that Merchant Payments exhibit the highest transaction volume, 
                 while there's a notable decline in transaction volumes for Financial Services and other categories.""")

    elif question ==  "7. What were the top three smartphone brands used phonepay in the year 2018 to 2023?":
        col0,col1=st.columns(2)
        with col0:
            year = st.slider('Choose the Year',min_value=2018,max_value=2022)
        sql_query=f'''
                    SELECT Brand_Name, SUM(Count) AS Total_Count
                    FROM aggregated_user
                    WHERE Year = {year}
                    GROUP BY Brand_Name
                    ORDER BY Total_Count DESC
                    LIMIT 3;'''
        cursor.execute(sql_query)
        res = cursor.fetchall()
        df = pd.DataFrame(res, columns=['Brand_Name', 'Total_Count'])
        df.index = df.index + 1
        st.write(df) 
    elif question == "8. What are the top 10 postal codes used spent highest amount using Phonepay?":
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
    elif question == "9. Which District has less Phonepe Registered Users?":
        cursor.execute('SELECT DISTINCT State FROM map_users')
        unique_states_data = cursor.fetchall()
        unique_states = [state[0] for state in unique_states_data]

        selected_state = st.selectbox('Select a State:', unique_states)
        sql_query = f'''
                    SELECT District_Name
                    FROM map_users
                    WHERE State = '{selected_state}'
                    GROUP BY State, District_Name
                    ORDER BY MIN(Registered_Users)
                    LIMIT 10;
                    '''
        cursor.execute(sql_query)
        res = cursor.fetchall()
        df = pd.DataFrame(res, columns=['District Name'])
        df.index = df.index + 1
        st.write(df)
    elif question == "10. Which 10 state has the highest growth in insurance, transactions and what percentage?":
        sql_query=('''SELECT State, 
                        SUM(Count) AS Total_Count, 
                        Avg(Amount) as Avg_Amount 
                        FROM aggregated_insurance
                        GROUP BY State
                        ORDER BY Avg_Amount DESC Limit 10;''')
        cursor.execute(sql_query)
        res=cursor.fetchall()
        df = pd.DataFrame(res, columns=['State', 'Total_Count','Avg_Amount'])
        df.index=df.index+1
        col0,col1=st.columns(2)
        fig = px.bar(df, x='State', y='Total_Count', title='State vs Transaction Count',color='Total_Count')
        with col0:
            st.plotly_chart(fig)
        fig1 = px.bar(df, x='State', y='Avg_Amount', title='State vs Average Amount of Insurance',color='Avg_Amount')
        with col1:
            st.plotly_chart(fig1)
    x=st.button("Done")
    if x:
        st.snow()