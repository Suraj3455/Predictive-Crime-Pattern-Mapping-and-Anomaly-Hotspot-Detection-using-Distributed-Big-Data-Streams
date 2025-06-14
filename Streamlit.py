import streamlit as st
import mysql.connector
import pandas as pd

db = mysql.connector.connect(
    host="database-1.cds2mw260a4s.ap-south-1.rds.amazonaws.com",
    user="admin",
    password="dUVrohpx4MdG1xqdmdW8",
    database="crime_db"
)

query = "SELECT area, COUNT(*) as total FROM crime_reports GROUP BY area"
df = pd.read_sql(query, db)

st.title("Crime Reports by Area")
st.bar_chart(df.set_index('area'))
