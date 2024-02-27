import os
import json
import pandas as pd
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import requests
import json

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

def aggregated_insurance_by_year():
    AI_grouped = agg_ins_df.groupby("Year")[["Count", "Amount"]].sum()
    AI_grouped.reset_index(inplace=True)
    col1,col2=st.columns(2)
    with col1:
        fig_year_amount = px.bar(AI_grouped, x="Year", y="Amount",
                                title="Aggregated-Insurance Amount by Year",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,
                                height=450, width=350)
        st.plotly_chart(fig_year_amount)
    with col2:
        fig_year_count = px.bar(AI_grouped, x="Year", y="Count",
                                title="Aggregated-Insurance Count by Year",
                                color_discrete_sequence=px.colors.sequential.Agsunset,
                                height=450, width=350)
        st.plotly_chart(fig_year_count)

def aggregated_insurance_by_quarter():
    AI_grouped_quarter = agg_ins_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AI_grouped_quarter.reset_index(inplace=True)
    fig_quarter_amount = px.bar(AI_grouped_quarter, x="Quarter", y="Amount", color="Year",
                                title="Aggregated Insurance Amount by Quarter",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,
                                height=650, width=600,barmode='stack')
    fig_quarter_amount.update_xaxes(tickmode='linear', dtick=1)
    st.plotly_chart(fig_quarter_amount)
    fig_quarter_count = px.bar(AI_grouped_quarter, x="Quarter", y="Count", color="Year",
                            title="Aggregated Insurance Count by Quarter",
                            color_discrete_sequence=px.colors.sequential.Agsunset,
                            height=650, width=600,barmode='stack')
    fig_quarter_count.update_xaxes(tickmode='linear', dtick=1)
    st.plotly_chart(fig_quarter_count)
  
def aggregated_transaction(user_year):
    AT=agg_trans_df[agg_trans_df["Year"]==user_year]
    AT.reset_index(drop=True,inplace=True)
    ATgroup=AT.groupby("State")[["Count","Amount"]].sum()
    ATgroup.reset_index(inplace=True)
    fig_at1=px.bar(ATgroup,x="State",y="Amount",title=f"Aggregated-Transaction Amount for the year {user_year}",
                color_discrete_sequence=px.colors.sequential.Aggrnyl,height=650,width=600)
    st.plotly_chart(fig_at1)
    fig_at=px.bar(ATgroup,x="State",y="Count",title=f"Aggregated-Transaction Count for the year {user_year}",
                color_discrete_sequence=px.colors.sequential.Agsunset,height=650,width=600)
    st.plotly_chart(fig_at)

def aggregated_transaction_by_year():
    AT_grouped = agg_trans_df.groupby("Year")[["Count", "Amount"]].sum()
    AT_grouped.reset_index(inplace=True)
    col3,col4=st.columns(2)
    with col3:
        fig_year_amount = px.bar(AT_grouped, x="Year", y="Amount",
                                title="Aggregated Transaction Amount by Year",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,
                                height=450, width=350)
        
        st.plotly_chart(fig_year_amount)
    with col4:
        fig_year_count = px.bar(AT_grouped, x="Year", y="Count",
                                title="Aggregated Transaction Count by Year",
                                color_discrete_sequence=px.colors.sequential.Agsunset,
                                height=450, width=350)
        
        st.plotly_chart(fig_year_count)

def aggregated_transaction_by_quarter():
    AT_grouped_quarter = agg_trans_df.groupby(["Year", "Quarter"])[["Count", "Amount"]].sum()
    AT_grouped_quarter.reset_index(inplace=True)
    fig_quarter_amount = px.bar(AT_grouped_quarter, x="Quarter", y="Amount", color="Year",
                                title="Aggregated Transaction Amount by Quarter",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,
                                height=650, width=800,barmode='stack')
    fig_quarter_amount.update_xaxes(tickmode='linear', dtick=1)
    st.plotly_chart(fig_quarter_amount)
    fig_quarter_count = px.bar(AT_grouped_quarter, x="Quarter", y="Count", color="Year",
                            title="Aggregated Transaction Count by Quarter",
                            color_discrete_sequence=px.colors.sequential.Agsunset,
                            height=650, width=800,barmode='stack')
    fig_quarter_count.update_xaxes(tickmode='linear', dtick=1)
    st.plotly_chart(fig_quarter_count)
    
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
                            hover_name="State",title=f"Insurance Amount for the year {user_year}",
                            fitbounds="locations",height=900,width=900)
    fig_india.update_geos(visible = False)
    fig_india.update_traces(hovertemplate='<b>%{hovertext}</b><br>Amount: %{z}')

    fig_india1=px.choropleth(MTgroup,geojson=data, locations="State", featureidkey="properties.ST_NM",
                            color="Count",color_continuous_scale="Rainbow",
                            range_color=(MTgroup["Count"].min(),MTgroup["Count"].max()),
                            hover_name="State",title=f"Insurance Count for the year {user_year}",
                            fitbounds="locations",height=900,width=900)
    fig_india1.update_geos(visible = False)
    
    return fig_india, fig_india1

                                ############# STREAMLIT ##################

st.set_page_config(page_title="PHONEPE",page_icon=":iphone:",layout="wide")
st.header(':violet[PHONEPE PULSE DATA VISUALIZATION AND EXPLORATION]')
with st.sidebar:
    st.title("India Phonepe Dashboard")
    video_file = open('Phonepe.mp4', 'rb')
    video_bytes = video_file.read()
    st.video(video_bytes)
    st.link_button("Install Now",
                   "https://play.google.com/store/apps/details?id=com.phonepe.app&hl=en_IN&gl=US")

selected=option_menu(menu_title="Choose the option for Data Exploration",
                    options=["AGGREGATION","MAP","TOP","DATA ANALYSIS"],
                    icons=["box-seam-fill","globe-asia-australia","geo-alt-fill","search"],
                    default_index=0,
                    orientation="horizontal"
                    )

if selected == "AGGREGATION":
    x=st.radio("Choose the Option",["Insurance","Transaction","Users"])
    if x == "Insurance":
        tab_titles=["States","Year","Quarter"]
        tabs=st.tabs(tab_titles)
        with tabs[0]:
            year_list = list(agg_ins_df.Year.unique())[::-1]
            option1 = st.selectbox("Choose the year",year_list)
            aggregated_insurance(option1)

        with tabs[1]:
            aggregated_insurance_by_year()

        with tabs[2]:
            aggregated_insurance_by_quarter()


    if x == "Transaction":
        tab_titles1=["States","Year","Quarter"]
        tabs1=st.tabs(tab_titles1)
        
        with tabs1[0]:
            year_list = list(agg_trans_df.Year.unique())[::-1]
            option2 = st.selectbox("Choose the year",year_list)
            aggregated_transaction(option2)

        with tabs1[1]:
            aggregated_transaction_by_year()

        with tabs1[2]:
            aggregated_transaction_by_quarter()
    
    if x == "Users":
        st.write("Loading")

elif selected == "MAP":
    y=st.radio("Choose the Option",["Insurance","Transaction","Users"])
    if y == "Insurance":
        year_list = list(map_ins_df.Year.unique())[::-1]
        option2 = st.selectbox("Choose the year",year_list)
        fig_india, fig_india1 = agg_transaction(option2)
        
        # Display the choropleth maps
        st.plotly_chart(fig_india)
        st.plotly_chart(fig_india1)
        

                





    




