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
cumulative_df = pd.concat([agg_ins_df, agg_trans_df, agg_user_df, map_ins_df, 
                           map_trans_df, map_users_df, top_ins_df, top_trans_df, top_users_df], ignore_index=True)


def total_payment_value():
    sql_query = "SELECT SUM(Transaction_Amount) AS Total_Amount FROM aggregated_transaction"
    cursor.execute(sql_query)
    total_amount_row = cursor.fetchone()
    total_amount_at = total_amount_row[0]
    total_amount_float = float(total_amount_at)
    total_amount_crores = np.round(total_amount_float / 10_000_000,2)
    st.subheader('Total Payment Value: ' + str(total_amount_crores) + ' Crores') 
def total_transaction_count():
    sql_query = "SELECT SUM(Transaction_Count) AS Total_Count FROM aggregated_transaction"
    cursor.execute(sql_query)
    total_amount_row = cursor.fetchone()
    total_amount_at = total_amount_row[0]
    total_amount_float = float(total_amount_at)
    total_amount_crores = np.round(total_amount_float / 10_000_000,2)
    st.subheader('All Phonepe Transaction: ' + str(total_amount_crores) + ' Crores') 

def aggregated_insurance(user_year):
    AI=agg_ins_df[agg_ins_df["Year"]==user_year]
    AI.reset_index(drop=True,inplace=True)
    AIgroup=AI.groupby("State")[["Count","Amount"]].sum()
    AIgroup.reset_index(inplace=True)
    fig_ai1=px.bar(AIgroup,x="State",y="Amount",title=f"Aggregated-Insurance Amount for the year {user_year}",
                    color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=600)
    st.plotly_chart(fig_ai1)
    fig_ai=px.bar(AIgroup,x="State",y="Count",title=f"Aggregated-Insurance Count for the year {user_year}",
                color_discrete_sequence=px.colors.sequential.Agsunset,height=650,width=600)
    st.plotly_chart(fig_ai)

def aggregated_insurance_by_quarter_amount():
    AI_grouped_quarter = agg_ins_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AI_grouped_quarter.reset_index(inplace=True)
    fig_quarter_amount = px.pie(AI_grouped_quarter, values="Amount", names="Quarter", title="Aggregated Insurance Amount by Quarter")
    fig_quarter_amount.update_traces(hole=0.4, pull=[0.05] * len(AI_grouped_quarter["Quarter"]))
    st.plotly_chart(fig_quarter_amount)
def aggregated_insurance_by_quarter_count():
    AI_grouped_quarter = agg_ins_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AI_grouped_quarter.reset_index(inplace=True)
    fig_quarter_count = px.pie(AI_grouped_quarter, values="Count", names="Quarter", title="Aggregated Insurance Count by Quarter")
    fig_quarter_count.update_traces(hole=0.4, pull=[0.05] * len(AI_grouped_quarter["Quarter"]))
    st.plotly_chart(fig_quarter_count)

def aggregated_transaction_by_year_amount():
    AT_grouped = agg_trans_df.groupby("Year")[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    year=AT_grouped['Year']
    amount=AT_grouped['Amount']
    
    fig_year_amount = px.bar(AT_grouped, x="Year", y="Amount",
                            title="Transaction Amount by Year from 2018 to 2023",
                            color=year)
    fig_year_amount.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()),  # Set tick values as unique years
        ticktext=list(map(str, AT_grouped["Year"].unique()))  # Set tick labels as strings of unique years
    )
    
    st.plotly_chart(fig_year_amount)
def aggregated_transaction_by_year_count():
    AT_grouped = agg_trans_df.groupby("Year")[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    year=AT_grouped['Year']
    amount=AT_grouped['Amount']
    
    fig_year_count = px.bar(AT_grouped, x="Year", y="Count",color=year,
                            title="Transaction Count by Year from 2018 to 2023"
                            )
    fig_year_count.update_xaxes(
        tickvals=list(AT_grouped["Year"].unique()),  # Set tick values as unique years
        ticktext=list(map(str, AT_grouped["Year"].unique()))
    )
    st.plotly_chart(fig_year_count)


                                ############# STREAMLIT ##################

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
            
    with st.container():
        sql_query = "SELECT SUM(Transaction_Amount) AS Total_Amount FROM aggregated_transaction"
        cursor.execute(sql_query)
        total_amount_row = cursor.fetchone()
        total_amount = total_amount_row[0]
        total_amount_float = float(total_amount)
        total_amount_crores = np.round(total_amount_float / 10_000_000,2)
        st.subheader('Total Payment Value: ' + str(total_amount_crores) + ' Crores') 

    aggregated_insurance_by_quarter_amount()

          

      
   
  
    
