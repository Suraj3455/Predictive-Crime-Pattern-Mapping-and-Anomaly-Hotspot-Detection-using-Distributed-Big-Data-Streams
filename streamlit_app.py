import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

# Set page title and layout
st.set_page_config(page_title="Crime Pattern Dashboard", layout="wide")

# Connect to MySQL database
db = mysql.connector.connect(
    host="database-1.cds2mw260a4s.ap-south-1.rds.amazonaws.com",
    user="admin",
    password="dUVrohpx4MdG1xqdmdW8",
    database="crime_db"
)
cursor = db.cursor()

# Fetch data
query = "SELECT * FROM crime_reports"
df = pd.read_sql(query, con=db)

# Convert date column to datetime
df['date_reported'] = pd.to_datetime(df['date_reported'], errors='coerce')

# Title
st.title("📊 Crime Pattern Analysis Dashboard")

# Sidebar Filters
st.sidebar.header("🔍 Filter Data")

# Crime Type Filter
crime_types = ['All'] + sorted(df['crime'].dropna().unique())
selected_crime = st.sidebar.selectbox("Select Crime Type", crime_types)

# Area Filter
areas = ['All'] + sorted(df['area'].dropna().unique())
selected_area = st.sidebar.selectbox("Select Area", areas)

# Date Range Filter — with safe default fallback
if df['date_reported'].isnull().all():
    min_date = pd.to_datetime("2022-01-01")
    max_date = pd.to_datetime("2022-12-31")
else:
    min_date = df['date_reported'].min()
    max_date = df['date_reported'].max()

selected_date = st.sidebar.date_input(
    "Select Date Range",
    [min_date.date(), max_date.date()],
    min_value=min_date.date(),
    max_value=max_date.date()
)

# Apply filters
if selected_crime != 'All':
    df = df[df['crime'] == selected_crime]

if selected_area != 'All':
    df = df[df['area'] == selected_area]

start_date, end_date = pd.to_datetime(selected_date[0]), pd.to_datetime(selected_date[1])
df = df[(df['date_reported'] >= start_date) & (df['date_reported'] <= end_date)]

# Metrics Card — Total crimes reported
st.subheader("🔴 Total Crimes Reported")
st.metric(label="Total Reports", value=len(df))

# Show filtered data
st.subheader("📑 Filtered Crime Data")
st.dataframe(df)

# Check if data available for charts
if df.empty:
    st.warning("No data available for selected filters.")
else:
    # Pie Chart: Weapon Distribution
    weapon_counts = df['weapon'].value_counts().reset_index()
    weapon_counts.columns = ['Weapon', 'Count']

    if not weapon_counts.empty:
        fig1 = px.pie(weapon_counts, names='Weapon', values='Count', title='Weapon Usage Distribution')
        st.plotly_chart(fig1, use_container_width=True)

    # Bar Chart: Crimes per Day
    daily_counts = df.groupby(df['date_reported'].dt.date).size().reset_index(name='Count')

    if not daily_counts.empty:
        fig2 = px.bar(daily_counts, x='date_reported', y='Count', title='Daily Crime Reports')
        st.plotly_chart(fig2, use_container_width=True)

    # Bar Chart: Crimes by Area
    area_counts = df['area'].value_counts().reset_index()
    area_counts.columns = ['Area', 'Count']

    if not area_counts.empty:
        fig3 = px.bar(area_counts, x='Area', y='Count', title='Crimes by Area')
        st.plotly_chart(fig3, use_container_width=True)

    # Bar Chart: Crimes by Type
    crime_counts = df['crime'].value_counts().reset_index()
    crime_counts.columns = ['Crime Type', 'Count']

    if not crime_counts.empty:
        fig4 = px.bar(crime_counts, x='Crime Type', y='Count', title='Crimes by Type')
        st.plotly_chart(fig4, use_container_width=True)

    # Line Chart: Trend over Time
    trend_df = df.groupby(df['date_reported'].dt.date).size().reset_index(name='Count')

    if not trend_df.empty:
        fig5 = px.line(trend_df, x='date_reported', y='Count', title='Crime Trend Over Time')
        st.plotly_chart(fig5, use_container_width=True)

# Close DB connection
cursor.close()
db.close()
