# app.py
import streamlit as st
import pandas as pd

# -------------------------------
# 1. LOAD CSV DATA FROM GITHUB
# -------------------------------
from sqlalchemy import create_engine

engine = create_engine(
    "mysql+pymysql://root:bala7598@localhost:3306/EQdb"
)

df = pd.read_sql("SELECT * FROM earthquakes", engine)


# Ensure 'time' column is datetime
if 'time' in df.columns:
    df['time'] = pd.to_datetime(df['time'], errors='coerce')

# -------------------------------
# 2. LIST OF QUESTIONS
# -------------------------------
QUESTION_LIST = [
    "1. Top 10 strongest earthquakes (mag)",
    "2. Top 10 deepest earthquakes (depth_km)",
    "3. Shallow earthquakes < 50 km and mag > 7.5",
    "4. Average depth per continent",
    "5. Average magnitude per magnitude type (magType)",
    "6. Year with most earthquakes",
    "7. Month with highest number of earthquakes",
    "8. Day of week with most earthquakes",
    "9. Count of earthquakes per hour",
    "10. Most active reporting network (net)",
    "11. Top 5 places with highest casualties",
    "12. Total estimated economic loss per continent",
    "13. Average economic loss by alert level",
    "14. Count reviewed vs automatic (status)",
    "15. Count by earthquake type (type)",
    "16. Number of earthquakes by data type (types)",
    "17. Average RMS and gap per continent",
    "18. Events with high station coverage (nst > 50)",
    "19. Number of tsunamis triggered per year",
    "20. Count earthquakes by alert levels",
    "21. Top 5 countries with highest avg magnitude (10 yrs)",
    "22. Countries with both shallow & deep EQ in same month",
    "23. YoY growth in total earthquakes",
    "24. Top 3 most active regions (frequency + avg mag)",
    "25. Avg depth for countries ±5° latitude from equator",
    "26. Countries with highest ratio shallow/deep",
    "27. Avg magnitude diff: tsunami vs no-tsunami",
    "28. Lowest reliability using gap & rms",
    "29. Consecutive EQ within 50 km & 1 hour",
    "30. Regions with most deep-focus EQ (depth > 300 km)"
]

# -------------------------------
# 3. DEFINE FUNCTIONS FOR QUESTIONS
# -------------------------------
def q1(df): return df.nlargest(10,'mag')[['mag','place','time']]
def q2(df): return df.nlargest(10,'depth')[['depth','place','mag','time']]
def q3(df): return df[(df['depth']<50) & (df['mag']>7.5)][['place','mag','depth','time']]
def q4(df): return df.groupby('continent')['depth'].mean().reset_index(name='avg_depth')
def q5(df): return df.groupby('magType')['mag'].mean().reset_index(name='avg_mag')
def q6(df): return df.groupby(df['time'].dt.year).size().reset_index(name='total').sort_values('total',ascending=False)
def q7(df): return df.groupby(df['time'].dt.month_name()).size().reset_index(name='total').sort_values('total',ascending=False)
def q8(df): return df.groupby(df['time'].dt.day_name()).size().reset_index(name='total').sort_values('total',ascending=False)
def q9(df): return df.groupby(df['time'].dt.hour).size().reset_index(name='total')
def q10(df): return df.groupby('net').size().reset_index(name='total').sort_values('total',ascending=False)
def q11(df): return df.nlargest(5,'casualties')[['place','casualties']]
def q12(df): return df.groupby('continent')['economic_loss'].sum().reset_index(name='total_loss')
def q13(df): return df.groupby('alert')['economic_loss'].mean().reset_index(name='avg_loss')
def q14(df): return df.groupby('status').size().reset_index(name='total')
def q15(df): return df.groupby('type').size().reset_index(name='total')
def q16(df): return df.groupby('types').size().reset_index(name='total')
def q17(df): return df.groupby('continent')[['rms','gap']].mean().reset_index()
def q18(df): return df[df['nst']>50][['place','mag','depth','nst']]
def q19(df): return df[df['tsunami']==1].groupby(df['time'].dt.year).size().reset_index(name='tsunami_count')
def q20(df): return df.groupby('alert').size().reset_index(name='total')
def q21(df): 
    recent_df = df[df['time'].dt.year >= df['time'].dt.year.max() - 10]
    return recent_df.groupby('country')['mag'].mean().reset_index(name='avg_mag').nlargest(5,'avg_mag')
def q22(df):
    grouped = df.groupby(['country', df['time'].dt.to_period('M')])
    result = grouped.apply(lambda x: (x['depth'] < 70).any() & (x['depth'] > 300).any())
    return result[result].reset_index(name='both_shallow_deep')
def q23(df):
    yearly = df.groupby(df['time'].dt.year).size().reset_index(name='total')
    yearly['yoy_growth'] = yearly['total'].pct_change() * 100
    return yearly
def q24(df):
    grouped = df.groupby('region').agg(freq=('mag','count'), avg_mag=('mag','mean'))
    grouped['score'] = grouped['freq']*grouped['avg_mag']
    return grouped.sort_values('score',ascending=False).head(3).reset_index()
def q25(df): return df[df['latitude'].abs()<=5].groupby('country')['depth'].mean().reset_index(name='avg_depth')
def q26(df):
    grouped = df.groupby('country').agg(shallow=lambda x:(x<70).sum(), deep=lambda x:(x>300).sum())
    grouped['ratio'] = grouped['shallow'] / grouped['deep'].replace(0,pd.NA)
    return grouped.sort_values('ratio',ascending=False).reset_index()
def q27(df):
    avg_t = df[df['tsunami']==1]['mag'].mean()
    avg_nt = df[df['tsunami']==0]['mag'].mean()
    return pd.DataFrame({'avg_tsunami':[avg_t],'avg_no_tsunami':[avg_nt]})
def q28(df):
    df['error_score'] = df['gap'].fillna(0) + df['rms'].fillna(0)
    return df.sort_values('error_score',ascending=False)[['place','mag','depth','gap','rms','error_score']]
def q29(df):
    df_sorted = df.sort_values('time')
    results=[]
    for i in range(len(df_sorted)-1):
        t1 = df_sorted.iloc[i]
        t2 = df_sorted.iloc[i+1]
        if pd.notnull(t1.latitude) and pd.notnull(t1.longitude) and pd.notnull(t2.latitude) and pd.notnull(t2.longitude):
            distance = ((t1.latitude-t2.latitude)**2 + (t1.longitude-t2.longitude)**2)**0.5*111
            time_diff = (t2.time-t1.time).total_seconds()/3600
            if distance <=50 and time_diff<=1:
                results.append({'eq1':t1.id,'eq2':t2.id,'t1':t1.time,'t2':t2.time,'distance_km':distance})
    return pd.DataFrame(results)
def q30(df): return df[df['depth']>300].groupby('region').size().reset_index(name='deep_count').sort_values('deep_count',ascending=False)

# -------------------------------
# 4. MAP QUESTIONS TO FUNCTIONS
# -------------------------------
question_funcs = {QUESTION_LIST[i]: globals()[f'q{i+1}'] for i in range(30)}

# -------------------------------
# 5. STREAMLIT UI
# -------------------------------
st.title("🌍 Earthquake Data Explorer – Streamlit Dashboard")

selected = st.selectbox("Select Your Analysis Question", QUESTION_LIST)

if st.button("Run Analysis"):
    result_df = question_funcs[selected](df)
    st.write("### 🔎 Result:")
    st.dataframe(result_df)


