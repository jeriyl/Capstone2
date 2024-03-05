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

def agg_users(user_year):
    MT=agg_user_df[agg_user_df["Year"]==user_year]
    MT.reset_index(drop=True,inplace=True)
    MTgroup=MT.groupby("State")[["Count","Percentage"]].sum()
    MTgroup.reset_index(inplace=True)

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url)
    data=json.loads(response.content)
    state_names=[]
    for feauture in data["features"]:
        state_names.append(feauture["properties"]["ST_NM"])
        state_names.sort()

    fig_india=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Percentage",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Percentage"].min(),MTgroup["Percentage"].max()),
                            hover_name="State",title=f"Average Transaction Amount for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india.update_geos(visible = False)
    fig_india.update_traces(hovertemplate='<b>%{hovertext}</b><br>Amount: %{z}')

    fig_india1=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Count",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Count"].min(),MTgroup["Count"].max()),
                            hover_name="State",title=f"Total Phonepe Users Count for the year {user_year}",
                            fitbounds="locations",height=700,width=700)
    fig_india1.update_geos(visible = False)
    
    return fig_india, fig_india1

st.set_page_config(page_title="PHONEPE",page_icon=":iphone:",layout="wide")
selected=option_menu(menu_title="PHONEPE PULSE DATA VISUALIZATION AND EXPLORATION",
                    options=["Dash Board","Data Analysis","Insights"],
                    icons=["window-dash","people-fill","search"],
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

if selected == "Data Analysis":
    selected1=option_menu(menu_title="DATA ANALYSIS",
                    options=["INDIA","DISTRICTS","TOP CATEGORIES"],
                    #icons=["window-dash","people-fill","search"],
                    default_index=0,
                    orientation="horizontal"
                    )
    if selected1 == "INDIA":
        tab1,tab2,tab3=st.tabs(["Insurance","Transaction","Users"])
        with tab1:
            col1,col2=st.columns(2)
            with col1:
                user_year1 = st.slider('Choose the Year',min_value=2020,max_value=2023)
            ins_map1,ins_map2 = ins_map(user_year1)
            col3,col4=st.columns(2)
            col1,col2=st.columns(2)
            with col3:
                st.plotly_chart(ins_map1)
            with col4:
                st.plotly_chart(ins_map2)
        with tab2:
            col71,col81=st.columns(2)
            with col71:
                user_year = st.slider("**Year**", min_value=2018, max_value=2022)
            col7,col8=st.columns(2)
            fig_india, fig_india1 = agg_transaction(user_year)
            with col7:
                st.plotly_chart(fig_india)
            with col8:
                st.plotly_chart(fig_india1)
        with tab3:
            col71,col81=st.columns(2)
            with col71:
                user_year = st.slider("**Year**", min_value=2018, max_value=2022)
            col7,col8=st.columns(2)
            fig_india, fig_india1 = agg_users(user_year)
            with col7:
                st.plotly_chart(fig_india)
            with col8:
                st.plotly_chart(fig_india1)

    if selected1 == "DISTRICTS":
        tab1,tab2,tab3=st.tabs(["Insurance","Transaction","Users"])
        




