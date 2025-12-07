import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.title("🌍 Earthquake Analytics Dashboard")
st.write("Select a question to run the analysis.")

# -----------------------------------------------------
# 1. Connect to MySQL (LOCAL)
# -----------------------------------------------------
try:
    engine = create_engine("mysql+pymysql://root:bala7598@localhost:3306/EQdb")
    db_connected = True
except:
    db_connected = False

# -----------------------------------------------------
# 2. If MySQL is not available (Streamlit Cloud), load CSV
# -----------------------------------------------------
df = None
if db_connected:
    try:
        df = pd.read_sql("SE_
