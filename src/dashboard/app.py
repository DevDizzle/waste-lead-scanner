import streamlit as st
import pandas as pd
from google.cloud import bigquery
import yaml
import folium
from streamlit_folium import st_folium
import json
from collections import Counter

# Configuration
st.set_page_config(page_title="Waste Lead Scanner", layout="wide")

@st.cache_data(ttl=600)
def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

config = load_config()
project_id = config['gcp']['project_id']
dataset = config['gcp']['dataset']

@st.cache_data(ttl=300)
def load_data():
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT * FROM `{project_id}.{dataset}.scored_leads`
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Failed to load data from BigQuery: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data found in BigQuery. Please run the scanner first.")
    st.stop()

# --- Sidebar ---
client_name = config['clients']['arrow']['name']
st.sidebar.title(f"{client_name}")
st.sidebar.subheader("Filter Leads")

min_date = df['file_date'].min()
max_date = df['file_date'].max()
st.sidebar.write(f"**Date Range:** {min_date} to {max_date}")

min_score = st.sidebar.slider("Minimum Score", 1.0, 10.0, 5.0, step=0.5)

permit_types = df['permit_type'].dropna().unique().tolist()
selected_permit_types = st.sidebar.multiselect("Permit Type", permit_types, default=permit_types)

property_types = df['property_type'].dropna().unique().tolist()
# Add 'Unknown' if there are nulls
if df['property_type'].isnull().any():
    property_types.append("Unknown")
selected_prop_types = st.sidebar.multiselect("Property Type", property_types, default=property_types)

# --- Filter Data ---
filtered_df = df[df['score'] >= min_score]
if selected_permit_types:
    filtered_df = filtered_df[filtered_df['permit_type'].isin(selected_permit_types)]

if selected_prop_types:
    # Handle Unknown mapped to null
    mask = filtered_df['property_type'].isin(selected_prop_types)
    if "Unknown" in selected_prop_types:
        mask = mask | filtered_df['property_type'].isnull()
    filtered_df = filtered_df[mask]

# --- Section 1: Summary Bar ---
st.title("Waste Lead Scanner Dashboard")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Leads Found", len(filtered_df))
col2.metric("High Priority (Score >= 7)", len(filtered_df[filtered_df['score'] >= 7]))
commercial_count = len(filtered_df[filtered_df['property_type'] == 'commercial'])
col3.metric("Commercial Permits", commercial_count)

def has_new_const_or_demo(tags):
    if not isinstance(tags, (list, tuple, set)):
        return False
    return 'new_construction' in tags or 'demolition' in tags

nc_demo_count = len(filtered_df[filtered_df['tags'].apply(has_new_const_or_demo)])
col4.metric("New Construction / Demolition", nc_demo_count)

st.markdown("---")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Lead Cards", "Map View", "Contractor Leaderboard"])

with tab1:
    st.subheader("Top Leads")
    sorted_df = filtered_df.sort_values(by='score', ascending=False)
    
    for _, row in sorted_df.head(50).iterrows(): # Show top 50 to avoid slow rendering
        with st.container():
            c1, c2, c3 = st.columns([1, 6, 2])
            
            # Score Badge
            score = row['score']
            color = "green" if score >= 7 else "orange" if score >= 5 else "gray"
            c1.markdown(f"<h2 style='color: {color}; text-align: center;'>{score}</h2>", unsafe_allow_html=True)
            
            # Details
            address = row['address'] or "Unknown Address"
            city = row['city'] or ""
            zipcode = row['zip'] or ""
            c2.markdown(f"**{address}, {city} {zipcode}**")
            
            tags = row['tags']
            tags_str = ", ".join(tags) if isinstance(tags, (list, tuple, set)) else str(tags)
            c2.markdown(f"*{row['permit_type']}* | Tags: {tags_str}")
            
            prop_type = row['property_type'] or "Unknown"
            c2.markdown(f"**Property:** {prop_type} | **Filed:** {row['file_date']}")
            
            # Contractor info
            contractor = row['contractor_name'] or "Unknown Contractor"
            c3.markdown(f"**{contractor}**")
            if row['job_value']:
                c3.markdown(f"Job Value: ${row['job_value']:,.2f}")
                
            employees = row['contractor_employees']
            if employees and isinstance(employees, str):
                try:
                    emp_list = json.loads(employees)
                    if emp_list:
                        names = [e.get('name') for e in emp_list if e.get('name')]
                        c3.markdown(f"<small>Contacts: {', '.join(names[:2])}</small>", unsafe_allow_html=True)
                except:
                    pass
        st.markdown("---")

with tab2:
    st.subheader("Map View")
    # Filter valid lat/lng
    map_df = filtered_df.dropna(subset=['lat', 'lng'])
    if not map_df.empty:
        center_lat = map_df['lat'].mean()
        center_lng = map_df['lng'].mean()
        m = folium.Map(location=[center_lat, center_lng], zoom_start=10)
        
        for _, row in map_df.iterrows():
            score = row['score']
            color = "green" if score >= 7 else "orange" if score >= 5 else "gray"
            
            popup_html = f"<b>Score: {score}</b><br>{row['address']}<br>{row['permit_type']}<br>{row['contractor_name']}"
            
            folium.Marker(
                location=[row['lat'], row['lng']],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color)
            ).add_to(m)
            
        st_folium(m, width=800, height=500)
    else:
        st.write("No location data available for map.")

with tab3:
    st.subheader("Contractor Leaderboard")
    
    if not filtered_df.empty:
        # Group by contractor
        leaderboard = []
        for contractor, group in filtered_df.groupby('contractor_name'):
            if not contractor or pd.isna(contractor):
                continue
                
            permit_count = len(group)
            avg_score = group['score'].mean()
            
            # Get top permit types
            types = group['permit_type'].tolist()
            top_types = ", ".join([k for k, v in Counter(types).most_common(2)])
            
            leaderboard.append({
                "Contractor Name": contractor,
                "Permits": permit_count,
                "Avg Score": round(avg_score, 1),
                "Top Permit Types": top_types
            })
            
        if leaderboard:
            ldf = pd.DataFrame(leaderboard).sort_values(by="Permits", ascending=False)
            st.dataframe(ldf, use_container_width=True)
            
            top_contractor = ldf.iloc[0]['Contractor Name']
            top_count = ldf.iloc[0]['Permits']
            st.info(f"**Insight:** {top_contractor} pulled {top_count} permits in your area matching criteria. Are they your customer?")
        else:
            st.write("No contractor data available.")
