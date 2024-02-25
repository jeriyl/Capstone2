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


# 3. District-Level Analysis
    district_level_analysis = map_ins_df.groupby(["State", "District_Name"])[["Count", "Amount"]].sum().reset_index()

    # Visualize top districts with highest insurance counts
    top_districts = district_level_analysis.nlargest(10, "Count")

    fig_top_districts = px.bar(top_districts, x="District_Name", y="Count",
                            title="Top 10 Districts by Insurance Counts",
                            labels={"District_Name": "District"})
    st.plotly_chart(fig_top_districts)

