import streamlit as st
import duckdb

st.set_page_config(page_title="Combined Sources SQL Explorer", layout="wide")

st.title("Combined Sources SQL Explorer")

PARQUET_URL = 'http://hen.astro.utoronto.ca/data/combined_sources.parquet'

@st.cache_resource
def connect_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")  # Enable reading from HTTP
    return con

con = connect_duckdb()

st.info(f"Connected to: `{PARQUET_URL}`")

# Default query
default_query = f"""
SELECT *
FROM read_parquet('{PARQUET_URL}')
LIMIT 100
"""

query = st.text_area("✏️ Enter your SQL query:", value=default_query, height=200)

if st.button("Run Query"):
    try:
        df = con.execute(query).fetchdf()
        st.success(f"Query ran successfully. {len(df)} rows returned.")
        st.dataframe(df, use_container_width=True)

        # Download buttons
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv_data, file_name="query_result.csv", mime='text/csv')

        parquet_data = df.to_parquet(index=False)
        st.download_button("Download Parquet", parquet_data, file_name="query_result.parquet", mime='application/octet-stream')

    except Exception as e:
        st.error(f"Query failed: {e}")

st.markdown("""
**Examples you can try:**
- `SELECT COUNT(*) FROM read_parquet('...')`
- `SELECT SOURCE_NAME, COUNT(*) AS count FROM read_parquet('...') GROUP BY SOURCE_NAME ORDER BY count DESC LIMIT 10`
- `SELECT * FROM read_parquet('...') WHERE MAG_AUTO < 20 LIMIT 50`
""")
