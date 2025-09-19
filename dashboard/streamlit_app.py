import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
from datetime import datetime, timedelta
import numpy as np
import os

# Page config
st.set_page_config(
    page_title="Bitcoin Economics Analytics Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection with correct path
@st.cache_resource
def get_db_connection():
    """Connect to DuckDB database"""
    database_path = r'D:\data\capstone\btc_economics.duckdb'
    
    if os.path.exists(database_path):
        st.sidebar.success(f"✅ Database found: {database_path}")
        return duckdb.connect(database_path, read_only=True)
    else:
        st.error("❌ Database not found. Please check the following:")
        st.info(f"""
        **Expected database location:**
        - `{database_path}`
        
        **To fix this:**
        1. Run `dbt run` to ensure your models are built
        2. Check that the database file exists at the specified path
        3. Verify the path is correct for your system
        """)
        st.stop()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(query):
    """Load data from DuckDB with caching"""
    try:
        conn = get_db_connection()
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return pd.DataFrame()

# Verify table existence
@st.cache_data(ttl=600)
def verify_tables():
    """Verify that our specific tables exist"""
    required_tables = [
        'main_marts.fact_bitcoin_prices',
        'main_marts.fact_bitcoin_network', 
        'main_marts.dim_economic_data'
    ]
    
    table_status = {}
    
    for table in required_tables:
        try:
            test_query = f"SELECT * FROM {table} LIMIT 1"
            result = load_data(test_query)
            if not result.empty:
                table_status[table] = {
                    'exists': True,
                    'columns': result.columns.tolist(),
                    'sample_row_count': len(result)
                }
            else:
                table_status[table] = {'exists': False, 'error': 'Table empty'}
        except Exception as e:
            table_status[table] = {'exists': False, 'error': str(e)}
    
    return table_status

# Main title
st.title("₿ Bitcoin Economics Analytics Dashboard")
st.markdown("---")

# Verify and display table status
st.sidebar.markdown("### 🔍 Database Verification")
table_status = verify_tables()

for table, status in table_status.items():
    if status['exists']:
        st.sidebar.success(f"✅ {table}")
        st.sidebar.text(f"Columns: {len(status['columns'])}")
    else:
        st.sidebar.error(f"❌ {table}")
        st.sidebar.text(f"Error: {status.get('error', 'Unknown')}")

# Sidebar filters
st.sidebar.header("📊 Dashboard Filters")

# Enhanced date range selector with more options
st.sidebar.subheader("📅 Date Range Selection")

# Quick date range buttons
date_option = st.sidebar.selectbox(
    "Quick Select:",
    ["Custom Range", "Last 7 days", "Last 30 days", "Last 90 days", "Year to Date"]
)

# Set default dates based on selection
if date_option == "Last 7 days":
    default_start = datetime.now() - timedelta(days=7)
    default_end = datetime.now()
elif date_option == "Last 30 days":
    default_start = datetime.now() - timedelta(days=30)
    default_end = datetime.now()
elif date_option == "Last 90 days":
    default_start = datetime.now() - timedelta(days=90)
    default_end = datetime.now()
elif date_option == "Year to Date":
    default_start = datetime(datetime.now().year, 1, 1)
    default_end = datetime.now()
else:  # Custom Range
    default_start = datetime.now() - timedelta(days=30)
    default_end = datetime.now()

# Date range selector
date_range = st.sidebar.date_input(
    "Select Custom Date Range:",
    value=(default_start, default_end),
    max_value=datetime.now(),
    help="Select start and end dates for analysis"
)

# Display selected range
if len(date_range) == 2:
    start_date, end_date = date_range
    days_selected = (end_date - start_date).days
    st.sidebar.info(f"📊 **Selected Range**: {days_selected} days")
else:
    st.sidebar.warning("⚠️ Please select both start and end dates")
    start_date, end_date = default_start.date(), default_end.date()

# Metric selector
st.sidebar.subheader("🔍 Analysis Focus")
analysis_type = st.sidebar.selectbox(
    "Choose analysis type:",
    ["Overview", "Price Analysis", "Economic Correlations"],
    help="Select the type of analysis to display"
)

# Direct data loading functions using exact table names
@st.cache_data(ttl=300)
def load_bitcoin_prices(start_date, end_date):
    """Load Bitcoin price data from main_marts.fact_bitcoin_prices"""
    query = f"""
    SELECT 
        date,
        price_usd,
        total_volume_usd,
        daily_return,
        weekly_return,
        monthly_return
    FROM main_marts.fact_bitcoin_prices
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    result = load_data(query)
    if not result.empty:
        st.sidebar.success(f"✅ Loaded {len(result)} price records")
    else:
        st.sidebar.warning("⚠️ No price data found for selected date range")
    
    return result

@st.cache_data(ttl=300)
def load_bitcoin_network(start_date, end_date):
    """Load Bitcoin network data from main_marts.fact_bitcoin_network"""
    query = f"""
    SELECT 
        date,
        daily_block_count,
        avg_block_size,
        daily_transaction_count,
        avg_block_fullness_pct,
        avg_mempool_size,
        daily_congestion_level,
        daily_avg_immediate_fee,
        daily_avg_standard_fee,
        network_usage_level,
        avg_difficulty
    FROM main_marts.fact_bitcoin_network
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    result = load_data(query)
    if not result.empty:
        st.sidebar.success(f"✅ Loaded {len(result)} network records")
        
        # Debug info for transaction count
        tx_count_info = result['daily_transaction_count'].describe()
        st.sidebar.info(f"Transaction count stats: Min: {tx_count_info['min']:.0f}, Max: {tx_count_info['max']:.0f}, Nulls: {result['daily_transaction_count'].isnull().sum()}")
        
        # Debug info for block fullness
        fullness_info = result['avg_block_fullness_pct'].describe()
        st.sidebar.info(f"Block fullness stats: Min: {fullness_info['min']:.1f}%, Max: {fullness_info['max']:.1f}%, Nulls: {result['avg_block_fullness_pct'].isnull().sum()}")
    else:
        st.sidebar.warning("⚠️ No network data found for selected date range")
    
    return result

@st.cache_data(ttl=300)
def load_economic_data(start_date, end_date):
    """Load economic data from main_marts.dim_economic_data"""
    query = f"""
    SELECT 
        date,
        series_id,
        series_name,
        indicator_name,
        indicator_category,
        numeric_value,
        value_30d_avg,
        daily_change,
        weekly_change,
        monthly_change,
        unit_of_measure,
        daily_trend,
        weekly_trend,
        monthly_trend
    FROM main_marts.dim_economic_data
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
        AND numeric_value IS NOT NULL
        AND indicator_name NOT LIKE '%continued%unemployment%'  -- Filter out continued unemployment claims specifically
        AND indicator_name NOT LIKE '%Continued%Unemployment%'  -- Case variations
        AND indicator_name NOT LIKE '%CONTINUED%UNEMPLOYMENT%'  -- Case variations
        AND indicator_name NOT LIKE '%continued_unemployment%'  -- Snake case variation
    ORDER BY date, series_id
    """
    
    result = load_data(query)
    if not result.empty:
        st.sidebar.success(f"✅ Loaded {len(result)} economic records")
        # Show the actual date range of economic data
        min_date = result['date'].min()
        max_date = result['date'].max()
        unique_indicators = result['indicator_name'].nunique()
        st.sidebar.info(f"Economic data range: {min_date} to {max_date}")
        st.sidebar.info(f"Available indicators: {unique_indicators}")
        
        # Enhanced debugging: Show specific series we're looking for
        target_series = result[result['series_id'].isin(['DPRIME', 'FEDFUNDS'])]
        if not target_series.empty:
            series_info = target_series.groupby(['series_id', 'indicator_name']).size().reset_index(name='count')
            st.sidebar.info("🎯 **Target Series Found:**")
            for _, row in series_info.iterrows():
                st.sidebar.text(f"  {row['series_id']}: {row['indicator_name']} ({row['count']} records)")
        else:
            st.sidebar.warning("⚠️ DPRIME or FEDFUNDS series not found in selected date range")
        
        # Show which unemployment indicators are included
        unemployment_indicators = result[result['indicator_name'].str.contains('unemployment', case=False, na=False)]['indicator_name'].unique()
        if len(unemployment_indicators) > 0:
            st.sidebar.info(f"📊 Unemployment indicators included: {', '.join(unemployment_indicators)}")
    else:
        st.sidebar.warning("⚠️ No economic data found for selected date range")
        
        # Enhanced debug: Check what economic data is actually available
        debug_query = f"""
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(*) as total_records,
            COUNT(DISTINCT indicator_name) as unique_indicators,
            COUNT(DISTINCT series_id) as unique_series,
            STRING_AGG(DISTINCT CONCAT(series_id, ':', indicator_name), ', ' ORDER BY series_id) as available_series
        FROM main_marts.dim_economic_data
        WHERE numeric_value IS NOT NULL
            AND indicator_name NOT LIKE '%continued%unemployment%'
            AND indicator_name NOT LIKE '%Continued%Unemployment%'
            AND indicator_name NOT LIKE '%CONTINUED%UNEMPLOYMENT%'
            AND indicator_name NOT LIKE '%continued_unemployment%'
        """
        debug_result = load_data(debug_query)
        if not debug_result.empty:
            st.sidebar.info(f"📅 Available economic data: {debug_result.iloc[0]['earliest_date']} to {debug_result.iloc[0]['latest_date']} ({debug_result.iloc[0]['total_records']} records)")
            st.sidebar.info(f"📊 Series available: {debug_result.iloc[0]['available_series']}")
    
    return result

# Add a new function to pivot economic data for easier analysis
@st.cache_data(ttl=300)
def get_pivoted_economic_data(start_date, end_date):
    """Get economic data pivoted by indicator for easier correlation analysis"""
    
    # Load raw economic data
    economic_data = load_economic_data(start_date, end_date)
    
    if economic_data.empty:
        return pd.DataFrame()
    
    # Show available indicators and series
    available_indicators = economic_data['indicator_name'].unique()
    available_series = economic_data['series_id'].unique()
    st.sidebar.info(f"📊 Economic indicators found: {', '.join(available_indicators)}")
    st.sidebar.info(f"📋 Series IDs found: {', '.join(available_series)}")
    
    # Check for our target series
    if 'DPRIME' in available_series:
        st.sidebar.success("✅ DPRIME (Prime Rate) found")
    if 'FEDFUNDS' in available_series:
        st.sidebar.success("✅ FEDFUNDS (Federal Funds Rate) found")
    else:
        st.sidebar.error("❌ FEDFUNDS (Federal Funds Rate) NOT found")
        
        # Show what unemployment-related series we have
        unemployment_series = economic_data[economic_data['indicator_name'].str.contains('unemployment', case=False, na=False)]['series_id'].unique()
        if len(unemployment_series) > 0:
            st.sidebar.info(f"📊 Unemployment series found: {', '.join(unemployment_series)}")
    
    # Pivot the data to have indicators as columns
    try:
        pivoted_data = economic_data.pivot_table(
            index='date',
            columns='indicator_name', 
            values='numeric_value',
            aggfunc='mean'  # In case there are duplicates
        ).reset_index()
        
        # Clean column names (remove spaces, make lowercase)
        pivoted_data.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace('%', 'pct') if col != 'date' else col for col in pivoted_data.columns]
        
        # Show what columns we ended up with
        econ_columns = [col for col in pivoted_data.columns if col != 'date']
        st.sidebar.info(f"🔄 Pivoted columns: {', '.join(econ_columns)}")
        
        # Specifically check if we have federal funds rate data
        fed_rate_columns = [col for col in econ_columns if 'federal' in col.lower() or 'fed' in col.lower()]
        if fed_rate_columns:
            st.sidebar.success(f"✅ Federal rate columns found: {', '.join(fed_rate_columns)}")
        else:
            st.sidebar.warning("⚠️ No federal rate columns found in pivoted data")
        
        return pivoted_data
        
    except Exception as e:
        st.sidebar.error(f"Error pivoting economic data: {str(e)}")
        return pd.DataFrame()

# Move the debug_network_data_availability function to right after the get_pivoted_economic_data function (around line 280):

# Add the debug function HERE (before it's called in the Overview section)
@st.cache_data(ttl=300)
def debug_network_data_availability(start_date, end_date):
    """Debug function to check network data availability"""
    
    # Check overall data availability
    debug_query = f"""
    SELECT 
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(*) as total_records,
        COUNT(daily_transaction_count) as tx_count_records,
        COUNT(avg_block_fullness_pct) as fullness_records,
        AVG(daily_transaction_count) as avg_tx_count,
        AVG(avg_block_fullness_pct) as avg_fullness,
        SUM(CASE WHEN daily_transaction_count IS NULL THEN 1 ELSE 0 END) as tx_nulls,
        SUM(CASE WHEN avg_block_fullness_pct IS NULL THEN 1 ELSE 0 END) as fullness_nulls,
        SUM(CASE WHEN daily_transaction_count = 0 THEN 1 ELSE 0 END) as tx_zeros
    FROM main_marts.fact_bitcoin_network
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    debug_result = load_data(debug_query)
    
    if not debug_result.empty:
        info = debug_result.iloc[0]
        st.sidebar.markdown("#### 🔍 Network Data Debug Info")
        st.sidebar.text(f"Date range: {info['earliest_date']} to {info['latest_date']}")
        st.sidebar.text(f"Total records: {info['total_records']}")
        st.sidebar.text(f"TX count records: {info['tx_count_records']}")
        st.sidebar.text(f"Fullness records: {info['fullness_records']}")
        st.sidebar.text(f"TX nulls: {info['tx_nulls']}, zeros: {info['tx_zeros']}")
        st.sidebar.text(f"Fullness nulls: {info['fullness_nulls']}")
        
        # Check for data gaps
        gaps_query = f"""
        SELECT 
            date,
            daily_transaction_count,
            avg_block_fullness_pct,
            CASE 
                WHEN daily_transaction_count IS NULL THEN 'TX_NULL'
                WHEN daily_transaction_count = 0 THEN 'TX_ZERO'
                ELSE 'TX_OK'
            END as tx_status,
            CASE 
                WHEN avg_block_fullness_pct IS NULL THEN 'FULLNESS_NULL'
                ELSE 'FULLNESS_OK'
            END as fullness_status
        FROM main_marts.fact_bitcoin_network
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
            AND (daily_transaction_count IS NULL 
                 OR daily_transaction_count = 0 
                 OR avg_block_fullness_pct IS NULL)
        ORDER BY date
        LIMIT 10
        """
        
        gaps_result = load_data(gaps_query)
        if not gaps_result.empty:
            st.sidebar.markdown("#### ⚠️ Data Issues Found:")
            st.sidebar.dataframe(gaps_result)
    
    return debug_result

# Add this function after the debug_network_data_availability function (around line 370):

@st.cache_data(ttl=300)
def check_missing_federal_funds_rate(start_date, end_date):
    """Check specifically for Federal Funds Rate data availability"""
    
    # Check for FEDFUNDS series specifically
    fedfunds_query = f"""
    SELECT 
        COUNT(*) as fedfunds_records,
        MIN(date) as earliest_fedfunds,
        MAX(date) as latest_fedfunds,
        AVG(numeric_value) as avg_fedfunds,
        indicator_name,
        series_id
    FROM main_marts.dim_economic_data
    WHERE series_id = 'FEDFUNDS'
        AND date BETWEEN '{start_date}' AND '{end_date}'
        AND numeric_value IS NOT NULL
    GROUP BY indicator_name, series_id
    """
    
    fedfunds_result = load_data(fedfunds_query)
    
    if not fedfunds_result.empty:
        st.sidebar.success("✅ FEDFUNDS data found!")
        st.sidebar.info(f"FEDFUNDS records: {fedfunds_result.iloc[0]['fedfunds_records']}")
        st.sidebar.info(f"FEDFUNDS range: {fedfunds_result.iloc[0]['earliest_fedfunds']} to {fedfunds_result.iloc[0]['latest_fedfunds']}")
        st.sidebar.info(f"FEDFUNDS indicator name: '{fedfunds_result.iloc[0]['indicator_name']}'")
    else:
        st.sidebar.error("❌ FEDFUNDS data not found!")
        
        # Check if FEDFUNDS exists at all
        all_fedfunds_query = """
        SELECT 
            MIN(date) as earliest_all,
            MAX(date) as latest_all,
            COUNT(*) as total_records,
            indicator_name,
            series_id
        FROM main_marts.dim_economic_data
        WHERE series_id = 'FEDFUNDS'
        GROUP BY indicator_name, series_id
        """
        
        all_fedfunds_result = load_data(all_fedfunds_query)
        if not all_fedfunds_result.empty:
            st.sidebar.warning(f"⚠️ FEDFUNDS exists but not in date range {start_date} to {end_date}")
            st.sidebar.info(f"FEDFUNDS available: {all_fedfunds_result.iloc[0]['earliest_all']} to {all_fedfunds_result.iloc[0]['latest_all']}")
        else:
            st.sidebar.error("❌ FEDFUNDS series not found in database at all!")
    
    return fedfunds_result

# Load data using direct table references
btc_data = load_bitcoin_prices(start_date, end_date)
economic_data = load_economic_data(start_date, end_date)

# Check specifically for Federal Funds Rate
fedfunds_check = check_missing_federal_funds_rate(start_date, end_date)

# Show what was actually found
st.sidebar.markdown("### 📊 Data Status")
st.sidebar.info(f"""
**Price Data**: {'✅ Found' if not btc_data.empty else '❌ Not found'}  
**Economic Data**: {'✅ Found' if not economic_data.empty else '❌ Not found'}  
**Federal Funds Rate**: {'✅ Found' if not fedfunds_check.empty else '❌ Missing'}  
**Records Found**: {len(btc_data)} price, {len(economic_data)} economic
""")

# Check if we have any data at all
if btc_data.empty:
    st.error("❌ No data available for analysis.")
    st.info("""
    **To resolve this issue:**
    1. Ensure your dbt models have been successfully run: `dbt run`
    2. Verify that the tables contain data for the selected date range
    3. Check the table verification section in the sidebar for specific errors
    """)
    st.stop()

# Overview Dashboard
if analysis_type == "Overview":
    st.header("📈 Bitcoin Market Overview")
    
    if btc_data.empty:
        st.warning("📊 No price data available for the selected date range")
    else:
        # Key metrics row - now only showing current price
        latest_data = btc_data.iloc[-1]
        
        st.metric(
            "Current Price",
            f"${latest_data['price_usd']:,.2f}" if pd.notna(latest_data['price_usd']) else "N/A",
            delta=f"{latest_data['daily_return']:.2%}" if pd.notna(latest_data['daily_return']) else None
        )

        # Price chart
        st.subheader("Bitcoin Price Trend")
        fig_price = px.line(
            btc_data, 
            x='date', 
            y='price_usd',
            title="Bitcoin Price Over Time",
            labels={'price_usd': 'Price (USD)', 'date': 'Date'}
        )
        fig_price.update_layout(height=400)
        st.plotly_chart(fig_price, use_container_width=True)

# Price Analysis
elif analysis_type == "Price Analysis":
    st.header("💰 Bitcoin Price Analysis")
    
    if btc_data.empty:
        st.error("❌ No price data available for analysis")
    else:
        # Returns analysis
        col1, col2 = st.columns(2)
        
        with col1:
            # Daily returns distribution
            if not btc_data['daily_return'].isna().all():
                fig_returns = px.histogram(
                    btc_data.dropna(subset=['daily_return']), 
                    x='daily_return',
                    title="Daily Returns Distribution",
                    nbins=50
                )
                fig_returns.update_xaxes(tickformat='.2%')
                st.plotly_chart(fig_returns, use_container_width=True)
            else:
                st.info("No daily return data available")
        
        with col2:
            # Price trend over time (replacing volatility chart)
            fig_trend = px.line(
                btc_data, 
                x='date', 
                y='price_usd',
                title="Bitcoin Price Trend"
            )
            fig_trend.update_layout(height=400)
            st.plotly_chart(fig_trend, use_container_width=True)
        
        # Risk metrics - removed volatility from here too
        st.subheader("Risk Metrics")
        if not btc_data['daily_return'].isna().all():
            returns = btc_data['daily_return'].dropna()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Avg Daily Return", f"{returns.mean():.3%}")
            with col2:
                st.metric("Max Drawdown", f"{returns.min():.3%}")
            with col3:
                # Calculate Sharpe ratio using calculated daily volatility
                daily_vol = returns.std()
                sharpe = returns.mean() / daily_vol * np.sqrt(365) if daily_vol > 0 else 0
                st.metric("Annualized Sharpe", f"{sharpe:.2f}")
        else:
            st.info("No return data available for risk metric calculations")

        # Price vs Volume correlation
        if not btc_data['total_volume_usd'].isna().all():
            st.subheader("Price vs Volume Analysis")
            fig_price_vol = px.scatter(
                btc_data,
                x='total_volume_usd',
                y='price_usd',
                title="Bitcoin Price vs Trading Volume",
                labels={'total_volume_usd': 'Volume (USD)', 'price_usd': 'Price (USD)'},
                trendline="ols"
            )
            st.plotly_chart(fig_price_vol, use_container_width=True)

# Economic Correlations
elif analysis_type == "Economic Correlations":
    st.header("📊 Bitcoin vs Economic Indicators")
    
    if btc_data.empty:
        st.error("❌ No price data available for correlation analysis")
    else:
        # Get pivoted economic data
        pivoted_economic_data = get_pivoted_economic_data(start_date, end_date)
        
        if pivoted_economic_data.empty:
            st.warning("⚠️ No economic data available for the selected date range")
            st.info("Economic data is available up to September 10th, 2025. Try selecting an earlier date range.")
            
            # Show raw economic data structure for debugging
            raw_econ_data = load_economic_data(start_date, end_date)
            if not raw_econ_data.empty:
                st.subheader("Available Economic Data (First 10 rows)")
                st.dataframe(raw_econ_data.head(10))
                
                st.subheader("Available Economic Indicators")
                indicators_summary = raw_econ_data.groupby('indicator_name').agg({
                    'date': ['min', 'max', 'count'],
                    'numeric_value': ['mean', 'std']
                }).round(3)
                st.dataframe(indicators_summary)
        else:
            # Merge price data with pivoted economic data
            merged_data = pd.merge(btc_data, pivoted_economic_data, on='date', how='inner')
            
            if merged_data.empty:
                st.warning("⚠️ No overlapping dates between price and economic data")
                st.info(f"""
                **Price data range**: {btc_data['date'].min()} to {btc_data['date'].max()}  
                **Economic data range**: {pivoted_economic_data['date'].min()} to {pivoted_economic_data['date'].max()}
                """)
            else:
                st.success(f"✅ Successfully merged {len(merged_data)} records with both price and economic data")
                
                # Show available economic columns
                econ_columns = [col for col in pivoted_economic_data.columns if col != 'date']
                st.info(f"**Available economic indicators**: {', '.join(econ_columns)}")
                
                # Prepare correlation analysis
                correlation_cols = ['price_usd'] + econ_columns
                available_cols = [col for col in correlation_cols if col in merged_data.columns and not merged_data[col].isna().all()]
                
                if len(available_cols) > 1:
                    st.subheader("Correlation Matrix")
                    
                    # Create correlation matrix
                    corr_data = merged_data[available_cols].corr()
                    
                    fig_corr = px.imshow(
                        corr_data,
                        title="Correlation Matrix: Bitcoin vs Economic Indicators",
                        color_continuous_scale='RdBu',
                        aspect='auto',
                        text_auto=True
                    )
                    fig_corr.update_layout(height=500)
                    st.plotly_chart(fig_corr, use_container_width=True)
                    
                    # Economic indicator selector and dual-axis chart
                    st.subheader("Bitcoin Price vs Economic Indicators")
                    
                    economic_indicators = [col for col in econ_columns if col in available_cols]
                    
                    if economic_indicators:
                        indicator = st.selectbox(
                            "Select Economic Indicator",
                            economic_indicators,
                            help="Choose an economic indicator to compare with Bitcoin price"
                        )
                        
                        # Filter data to remove any null values for the selected indicator
                        chart_data = merged_data.dropna(subset=['price_usd', indicator])
                        
                        if not chart_data.empty:
                            # Create dual-axis chart
                            fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
                            
                            # Bitcoin price
                            fig_dual.add_trace(
                                go.Scatter(
                                    x=chart_data['date'], 
                                    y=chart_data['price_usd'], 
                                    name="Bitcoin Price", 
                                    line=dict(color='orange', width=2)
                                ),
                                secondary_y=False,
                            )
                            
                            # Economic indicator
                            fig_dual.add_trace(
                                go.Scatter(
                                    x=chart_data['date'], 
                                    y=chart_data[indicator], 
                                    name=indicator.replace('_', ' ').title(), 
                                    line=dict(color='blue', width=2)
                                ),
                                secondary_y=True,
                            )
                            
                            fig_dual.update_xaxes(title_text="Date")
                            fig_dual.update_yaxes(title_text="Bitcoin Price (USD)", secondary_y=False)
                            fig_dual.update_yaxes(title_text=indicator.replace('_', ' ').title(), secondary_y=True)
                            fig_dual.update_layout(
                                height=400,
                                title=f"Bitcoin Price vs {indicator.replace('_', ' ').title()}"
                            )
                            

                            st.plotly_chart(fig_dual, use_container_width=True)
                            
                            # Show correlation coefficient and statistics
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                correlation = chart_data['price_usd'].corr(chart_data[indicator])
                                st.metric("Correlation Coefficient", f"{correlation:.3f}")
                            
                            with col2:
                                # Calculate R-squared
                                correlation_squared = correlation ** 2
                                st.metric("R-squared", f"{correlation_squared:.3f}")
                            

                            with col3:
                                # Data points used
                                st.metric("Data Points", f"{len(chart_data)}")
                            

                            # Interpretation
                            if abs(correlation) > 0.7:
                                interpretation = "Strong correlation"
                            elif abs(correlation) > 0.3:
                                interpretation = "Moderate correlation"
                            else:
                                interpretation = "Weak correlation"
                            

                            direction = "positive" if correlation > 0 else "negative"
                            st.info(f"**Interpretation**: {interpretation} ({direction})")
                            
                        else:
                            st.info(f"No valid data for {indicator} comparison")
                    else:
                        st.info("No economic indicator data available for comparison")
                        
                    # Economic indicator trends table
                    if len(econ_columns) > 0:
                        st.subheader("Economic Indicators Summary")
                        
                        # Create summary statistics for each indicator
                        summary_data = []
                        for indicator in econ_columns:
                            if indicator in merged_data.columns:
                                indicator_data = merged_data[indicator].dropna()
                                if not indicator_data.empty:
                                    summary_data.append({
                                        'Indicator': indicator.replace('_', ' ').title(),
                                        'Latest Value': f"{indicator_data.iloc[-1]:.3f}",
                                        'Average': f"{indicator_data.mean():.3f}",
                                        'Min': f"{indicator_data.min():.3f}",
                                        'Max': f"{indicator_data.max():.3f}",
                                        'Std Dev': f"{indicator_data.std():.3f}",
                                        'Correlation with BTC': f"{merged_data['price_usd'].corr(indicator_data):.3f}"
                                    })
                        
                        if summary_data:
                            summary_df = pd.DataFrame(summary_data)
                            st.dataframe(summary_df, use_container_width=True)
                        
                else:
                    st.info("Insufficient data for correlation analysis")

# Data quality info - updated to remove network data references
st.sidebar.markdown("---")
st.sidebar.markdown("### 📋 Data Quality")
st.sidebar.info(f"""
**Date Range**: {start_date} to {end_date}  
**Price Records**: {len(btc_data)}  
**Economic Records**: {len(economic_data)}  
**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}
""")

# Footer
st.markdown("---")
st.markdown("*Dashboard powered by dbt, DuckDB, and Streamlit | Data from CoinGecko and FRED APIs*")