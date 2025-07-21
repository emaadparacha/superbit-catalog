import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="Combined Sources Explorer", layout="wide")

st.title("SuperBIT Targets and Sources Explorer 🔭")

PARQUET_URL = 'http://hen.astro.utoronto.ca/data/combined_sources.parquet'

@st.cache_resource
def connect_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    return con

con = connect_duckdb()

st.info(f"Connected to: {PARQUET_URL}")

# Get list of unique SOURCE_NAMEs
@st.cache_data
def get_unique_sources():
    query = f"""
    SELECT DISTINCT SOURCE_NAME
    FROM read_parquet('{PARQUET_URL}')
    WHERE SOURCE_NAME IS NOT NULL
    ORDER BY SOURCE_NAME
    """
    df_sources = con.execute(query).fetchdf()
    return df_sources['SOURCE_NAME'].tolist()

unique_sources = get_unique_sources()
selected_source = st.selectbox("Select a SOURCE_NAME target:", [""] + unique_sources)

if selected_source:
    st.write(f"Showing first 10 rows for target: {selected_source}")
    
    # Show only 10 rows in app
    query_display = f"""
    SELECT *
    FROM read_parquet('{PARQUET_URL}')
    WHERE SOURCE_NAME = '{selected_source}'
    LIMIT 10
    """
    df_display = con.execute(query_display).fetchdf()
    st.dataframe(df_display, use_container_width=True, height=500)
    
    # For download, pull all rows
    query_all = f"""
    SELECT *
    FROM read_parquet('{PARQUET_URL}')
    WHERE SOURCE_NAME = '{selected_source}'
    """
    df_all = con.execute(query_all).fetchdf()
    
    csv_data = df_all.to_csv(index=False).encode('utf-8')
    st.download_button("Download full target data as CSV", csv_data, file_name=f"{selected_source}_all.csv", mime='text/csv')

st.markdown("---")

st.subheader("Custom SQL Query (default view limited to 150 rows)")

st.write("Note: To query the dataset, you must use this format:")
st.code(f"FROM read_parquet('{PARQUET_URL}')", language='sql')

default_sql = f"""
SELECT *
FROM read_parquet('{PARQUET_URL}')
LIMIT 150
"""

user_query = st.text_area("Enter your SQL query below:", value=default_sql, height=200)

if st.button("Run SQL Query"):
    try:
        df_query = con.execute(user_query).fetchdf()
        st.success(f"Query ran successfully. Showing {len(df_query)} rows (display limited to 150 by default).")
        
        # Show limited view (150 or user-controlled)
        st.dataframe(df_query, use_container_width=True, height=500)

        # Download all query results
        csv_query_data = df_query.to_csv(index=False).encode('utf-8')
        st.download_button("Download query result as CSV", csv_query_data, file_name="query_result.csv", mime='text/csv')

    except Exception as e:
        st.error(f"Query failed: {e}")
