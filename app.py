import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# -------------------------------
# 1. DB CONNECTION
# -------------------------------
engine = create_engine("mysql+pymysql://root:bala7598@localhost:3306/EQdb")

# -------------------------------------------------------
# 2. ALL QUESTIONS – Dropdown Labels + SQL/Pandas Logic
# -------------------------------------------------------

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


# --------------------------------------------------------------
# 3. FUNCTION TO EXECUTE SQL
# --------------------------------------------------------------
def run_query(query):
    return pd.read_sql(query, engine)


# --------------------------------------------------------------
# 4. MAP QUESTIONS TO SQL QUERIES
# --------------------------------------------------------------
def fetch_result(question):

    # --- 1 ---
    if question.startswith("1."):
        return run_query("""
            SELECT mag, place, time 
            FROM earthquakes 
            ORDER BY mag DESC 
            LIMIT 10;
        """)

    # --- 2 ---
    if question.startswith("2."):
        return run_query("""
            SELECT depth, place, mag, time 
            FROM earthquakes 
            ORDER BY depth DESC 
            LIMIT 10;
        """)

    # --- 3 ---
    if question.startswith("3."):
        return run_query("""
            SELECT place, mag, depth, time 
            FROM earthquakes 
            WHERE depth < 50 AND mag > 7.5;
        """)

    # --- 4 ---
    if question.startswith("4."):
        return run_query("""
            SELECT continent, AVG(depth) AS avg_depth
            FROM earthquakes
            GROUP BY continent;
        """)

    # --- 5 ---
    if question.startswith("5."):
        return run_query("""
            SELECT magType, AVG(mag) AS avg_magnitude
            FROM earthquakes
            GROUP BY magType;
        """)

    # --- 6 ---
    if question.startswith("6."):
        return run_query("""
            SELECT YEAR(time) AS year, COUNT(*) AS total
            FROM earthquakes
            GROUP BY year
            ORDER BY total DESC;
        """)

    # --- 7 ---
    if question.startswith("7."):
        return run_query("""
            SELECT MONTHNAME(time) AS month, COUNT(*) AS total
            FROM earthquakes
            GROUP BY month
            ORDER BY total DESC;
        """)

    # --- 8 ---
    if question.startswith("8."):
        return run_query("""
            SELECT DAYNAME(time) AS weekday, COUNT(*) AS total
            FROM earthquakes
            GROUP BY weekday
            ORDER BY total DESC;
        """)

    # --- 9 ---
    if question.startswith("9."):
        return run_query("""
            SELECT HOUR(time) AS hour, COUNT(*) AS total
            FROM earthquakes
            GROUP BY hour
            ORDER BY hour;
        """)

    # --- 10 ---
    if question.startswith("10."):
        return run_query("""
            SELECT net, COUNT(*) AS total
            FROM earthquakes
            GROUP BY net
            ORDER BY total DESC;
        """)

    # --- 11 ---
    if question.startswith("11."):
        return run_query("""
            SELECT place, casualties
            FROM earthquakes
            ORDER BY casualties DESC
            LIMIT 5;
        """)

    # --- 12 ---
    if question.startswith("12."):
        return run_query("""
            SELECT continent, SUM(economic_loss) AS total_loss
            FROM earthquakes
            GROUP BY continent;
        """)

    # --- 13 ---
    if question.startswith("13."):
        return run_query("""
            SELECT alert, AVG(economic_loss) AS avg_loss
            FROM earthquakes
            GROUP BY alert;
        """)

    # --- 14 ---
    if question.startswith("14."):
        return run_query("""
            SELECT status, COUNT(*) AS total
            FROM earthquakes
            GROUP BY status;
        """)

    # --- 15 ---
    if question.startswith("15."):
        return run_query("""
            SELECT type, COUNT(*) AS total
            FROM earthquakes
            GROUP BY type;
        """)

    # --- 16 ---
    if question.startswith("16."):
        return run_query("""
            SELECT types, COUNT(*) AS total
            FROM earthquakes
            GROUP BY types;
        """)

    # --- 17 ---
    if question.startswith("17."):
        return run_query("""
            SELECT continent, AVG(rms) AS avg_rms, AVG(gap) AS avg_gap
            FROM earthquakes
            GROUP BY continent;
        """)

    # --- 18 ---
    if question.startswith("18."):
        return run_query("""
            SELECT place, mag, depth, nst
            FROM earthquakes
            WHERE nst > 50;
        """)

    # --- 19 ---
    if question.startswith("19."):
        return run_query("""
            SELECT YEAR(time) AS year, COUNT(*) AS tsunami_count 
            FROM earthquakes
            WHERE tsunami = 1
            GROUP BY year;
        """)

    # --- 20 ---
    if question.startswith("20."):
        return run_query("""
            SELECT alert, COUNT(*) AS total
            FROM earthquakes
            GROUP BY alert;
        """)

    # --- 21 ---
    if question.startswith("21."):
        return run_query("""
            SELECT country, AVG(mag) AS avg_mag
            FROM earthquakes
            WHERE YEAR(time) >= YEAR(CURDATE()) - 10
            GROUP BY country
            ORDER BY avg_mag DESC
            LIMIT 5;
        """)

    # --- 22 ---
    if question.startswith("22."):
        return run_query("""
            SELECT country, MONTH(time) AS month
            FROM earthquakes
            GROUP BY country, month
            HAVING SUM(depth < 70) > 0 AND SUM(depth > 300) > 0;
        """)

    # --- 23 ---
    if question.startswith("23."):
        return run_query("""
            SELECT 
                YEAR(time) AS year,
                COUNT(*) AS total,
                (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY YEAR(time))) /
                LAG(COUNT(*)) OVER (ORDER BY YEAR(time)) * 100 AS yoy_growth
            FROM earthquakes
            GROUP BY year;
        """)

    # --- 24 ---
    if question.startswith("24."):
        return run_query("""
            SELECT region,
                COUNT(*) AS freq,
                AVG(mag) AS avg_mag,
                (COUNT(*) * AVG(mag)) AS score
            FROM earthquakes
            GROUP BY region
            ORDER BY score DESC
            LIMIT 3;
        """)

    # --- 25 ---
    if question.startswith("25."):
        return run_query("""
            SELECT country, AVG(depth) AS avg_depth
            FROM earthquakes
            WHERE ABS(latitude) <= 5
            GROUP BY country;
        """)

    # --- 26 ---
    if question.startswith("26."):
        return run_query("""
            SELECT country,
                SUM(depth < 70) AS shallow,
                SUM(depth > 300) AS deep,
                SUM(depth < 70) / NULLIF(SUM(depth > 300), 0) AS ratio
            FROM earthquakes
            GROUP BY country
            ORDER BY ratio DESC;
        """)

    # --- 27 ---
    if question.startswith("27."):
        return run_query("""
            SELECT 
                (SELECT AVG(mag) FROM earthquakes WHERE tsunami=1) AS avg_tsunami,
                (SELECT AVG(mag) FROM earthquakes WHERE tsunami=0) AS avg_no_tsunami;
        """)

    # --- 28 ---
    if question.startswith("28."):
        return run_query("""
            SELECT place, mag, depth, gap, rms,
                   (gap + rms) AS error_score
            FROM earthquakes
            ORDER BY error_score DESC;
        """)

    # --- 29 ---
    if question.startswith("29."):
        return run_query("""
            SELECT 
                e1.id AS eq1, e2.id AS eq2,
                e1.time AS t1, e2.time AS t2,
                ST_Distance_Sphere(
                    POINT(e1.longitude, e1.latitude),
                    POINT(e2.longitude, e2.latitude)
                )/1000 AS distance_km
            FROM earthquakes e1
            JOIN earthquakes e2
            ON e2.time > e1.time
            AND TIMESTAMPDIFF(MINUTE, e1.time, e2.time) <= 60
            HAVING distance_km <= 50;
        """)

    # --- 30 ---
    if question.startswith("30."):
        return run_query("""
            SELECT region, COUNT(*) AS deep_count
            FROM earthquakes
            WHERE depth > 300
            GROUP BY region
            ORDER BY deep_count DESC;
        """)


# --------------------------------------------------------------
# 5. STREAMLIT UI
# --------------------------------------------------------------
st.title("🌍 Earthquake Data Explorer – Streamlit Dashboard")

selected = st.selectbox("Select Your Analysis Question", QUESTION_LIST)

if st.button("Run Analysis"):
    df = fetch_result(selected)
    st.write("### 🔎 Result:")
    st.dataframe(df)
