# Capstone2
Phonepe Pulse Data Visualization and Exploration

https://icons.getbootstrap.com/

AGG_INS
AGG_TRANS
AGG_USER

MAP_INS
MAP_TRANS
MAP_USER

TOP_INS
TOP_TRANS
TOP_USER

def pie():
    sql_query='''SELECT Transaction_Type, SUM(Transaction_Amount) AS total_amount 
    FROM aggregated_transaction 
    GROUP BY Transaction_Type'''
    cursor.execute(sql_query)
    results = cursor.fetchall()
    transaction_types = []
    total_amounts = []
    for row in results:
        transaction_types.append(row[0])
        total_amounts.append(row[1])
    fig = px.pie(
        values=total_amounts,
        names=transaction_types,
        title='Transaction Type Distribution')
    fig.update_traces(textposition='inside', textinfo='none')
    st.plotly_chart(fig)


