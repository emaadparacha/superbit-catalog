import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="Combined Sources Explorer", layout="wide")

st.title("Combined Sources Explorer")

PARQUET_URL = 'http://hen.astro.utoronto.ca/data/combined_sources.parquet'

@st.cache_resource
def connect_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")  # Enable reading from HTTP
    return con

con = connect_duckdb()

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
selected_source = st.selectbox("Select a SOURCE_NAME target (optional):", [""] + unique_sources)

if selected_source:
    st.write(f"Showing first 100 rows for target: {selected_source}")
    query = f"""
    SELECT *
    FROM read_parquet('{PARQUET_URL}')
    WHERE SOURCE_NAME = '{selected_source}'
    LIMIT 100
    """
else:
    st.write("No target selected. Showing one row per unique target (default limit 150).")
    query = f"""
    SELECT *
    FROM read_parquet('{PARQUET_URL}')
    WHERE SOURCE_NAME IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE_NAME ORDER BY ALPHA_SKY) = 1
    LIMIT 150
    """

df_default = con.execute(query).fetchdf()
st.dataframe(df_default, use_container_width=True, height=500)

# Download button (CSV only)
csv_data = df_default.to_csv(index=False).encode('utf-8')
st.download_button("Download table as CSV", csv_data, file_name="default_view.csv", mime='text/csv')

st.markdown("---")

st.subheader("Custom SQL Query (default limit 150)")

st.write("Note: To query the dataset, you must use:")
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
        st.success(f"Query ran successfully. Showing {len(df_query)} rows.")
        st.dataframe(df_query, use_container_width=True, height=500)

        csv_query_data = df_query.to_csv(index=False).encode('utf-8')
        st.download_button("Download query result as CSV", csv_query_data, file_name="query_result.csv", mime='text/csv')

    except Exception as e:
        st.error(f"Query failed: {e}")
