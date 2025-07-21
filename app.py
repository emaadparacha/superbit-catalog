import streamlit as st
import pandas as pd

st.set_page_config(page_title="Combined Sources Viewer", layout="wide")

st.title("Combined Sources Viewer")

# Load data from public URL
@st.cache_data
def load_data():
    url = 'http://hen.astro.utoronto.ca/data/combined_sources.parquet'
    df = pd.read_parquet(url)
    return df

# Load data
df = load_data()
st.write(f"Loaded {df.shape[0]:,} rows and {df.shape[1]} columns.")

# Sidebar filters
st.sidebar.header("Filter Data")
unique_sources = df['SOURCE_NAME'].dropna().unique()
selected_sources = st.sidebar.multiselect("Select SOURCE_NAME(s)", sorted(unique_sources))

filtered_df = df
if selected_sources:
    filtered_df = df[df['SOURCE_NAME'].isin(selected_sources)]

# Show filtered DataFrame
st.subheader("Data Preview")
st.dataframe(filtered_df, use_container_width=True, height=500)

# Download buttons
st.subheader("Download Data")
csv_data = filtered_df.to_csv(index=False).encode('utf-8')
st.download_button("Download as CSV", csv_data, file_name="filtered_data.csv", mime='text/csv')

parquet_data = filtered_df.to_parquet(index=False)
st.download_button("Download as Parquet", parquet_data, file_name="filtered_data.parquet", mime='application/octet-stream')

st.info("Use the sidebar to filter data. You can download the filtered results above.")
