# Bitcoin Economics Analytics Dashboard

A comprehensive data engineering capstone project that analyzes the relationship between Bitcoin price movements and macroeconomic indicators through automated data pipelines, advanced transformations, and interactive analytics.

## 🎯 Project Overview

This project builds a complete data engineering solution to explore correlations between Bitcoin's price behavior and traditional economic indicators. It demonstrates end-to-end data pipeline development, from raw API ingestion to production-ready analytics dashboards.

### Key Features

- **Multi-Source Data Integration**: CoinGecko (Bitcoin prices), FRED (economic indicators), Blockstream (network data)
- **Automated ETL Pipelines**: Python-based data ingestion with MinIO object storage
- **Advanced Data Modeling**: dbt transformations following medallion architecture (Bronze → Silver → Gold)
- **Interactive Analytics**: Streamlit dashboard with correlation analysis and economic insights
- **Production-Ready Infrastructure**: Docker containerization, robust error handling, and data quality monitoring

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│    MinIO     │───▶│   dbt Models    │───▶│   Dashboard     │
│                 │    │   (Bronze)   │    │ (Silver & Gold) │    │  (Streamlit)    │
├─────────────────┤    └──────────────┘    ├─────────────────┤    └─────────────────┘
│ • CoinGecko API │                        │ • Staging       │
│ • FRED API      │                        │ • Intermediate  │
│ • Blockstream   │                        │ • Marts         │
└─────────────────┘                        └─────────────────┘
```

## 📊 Data Sources

### Bitcoin & Cryptocurrency Data (CoinGecko)
- **Daily price data**: USD prices, market cap, trading volume
- **Historical coverage**: 365+ days of comprehensive market data
- **Data quality**: Real-time validation and completeness scoring

### Economic Indicators (FRED)
- **Interest Rates**: Federal funds rate, Treasury yields (2Y, 10Y, 3M), Prime rate
- **SOFR Data**: Secured Overnight Financing Rate and related indices
- **Employment**: Initial/continued unemployment claims, unemployment rate
- **Market Data**: S&P 500 index for traditional market correlation

### Bitcoin Network Data (Blockstream)
- **Block Information**: Transaction counts, block sizes, mining difficulty
- **Fee Analysis**: Current and historical fee estimates across confirmation targets
- **Mempool Metrics**: Real-time transaction pool status and congestion levels

## 🛠️ Technology Stack

### Data Engineering
- **Languages**: Python, SQL
- **Storage**: MinIO (S3-compatible object storage)
- **Data Warehouse**: DuckDB (embedded analytics database)
- **Orchestration**: dbt (data build tool)

### Analytics & Visualization
- **Dashboard**: Streamlit
- **Visualization**: Plotly
- **Analysis**: Pandas, NumPy
- **Statistics**: Correlation analysis, volatility calculations, trend detection

### Infrastructure
- **Containerization**: Docker & Docker Compose
- **Environment Management**: Python dotenv
- **Version Control**: Git with comprehensive .gitignore

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Git

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/capstone-bitcoin-economics.git
cd capstone-bitcoin-economics
```

### 2. Environment Setup
```bash
# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Copy environment template and configure
cp .env.example .env
# Edit .env with your API keys (see Configuration section)
```

### 3. Infrastructure Setup
```bash
# Start MinIO storage
docker-compose up -d

# Verify MinIO is running
curl http://localhost:9000/minio/health/live
```

### 4. Data Collection
```bash
# Collect Bitcoin price data
python data-ingestion/coingecko_api.py

# Collect economic indicators
python data-ingestion/fred_api.py

# Optional: Collect network data (large dataset)
python data-ingestion/blockstream_api.py --current-only
```

### 5. Data Transformation
```bash
cd btc_economics_dataset

# Install dbt dependencies
dbt deps

# Run transformations
dbt run

# Optional: Run data quality tests
dbt test
```

### 6. Launch Dashboard
```bash
cd dashboard
streamlit run streamlit_app.py
```

Access the dashboard at: `http://localhost:8501`

## ⚙️ Configuration

### API Registration

1. **CoinGecko**: Sign up at [coingecko.com](https://www.coingecko.com/en/api) for free API access
2. **FRED**: Register at [FRED API](https://fred.stlouisfed.org/docs/api/api_key.html)
3. **Blockstream**: Optional - contact Blockstream for API access

## 📈 Dashboard Features

### Overview Section
- **Current Bitcoin Metrics**: Price with daily change percentage
- **Price Trend Visualization**: Interactive time series chart
- **Date Range Filtering**: Flexible time period selection

### Price Analysis
- **Return Distribution**: Histogram of daily Bitcoin returns
- **Risk Metrics**: Daily return statistics, max drawdown, Sharpe ratio
- **Volume Correlation**: Price vs trading volume scatter analysis

### Economic Correlations
- **Correlation Matrix**: Heatmap showing Bitcoin relationships with economic indicators
- **Dual-Axis Charts**: Side-by-side Bitcoin price and economic indicator visualization
- **Statistical Insights**: Correlation coefficients, R-squared values, sample sizes
- **Economic Context**: Detailed indicator descriptions and trend analysis

## 🏛️ Data Architecture

### Bronze Layer (Raw Data)
- **Source**: Direct API responses stored as JSON in MinIO
- **Purpose**: Immutable data lake with complete historical records
- **Structure**: Organized by source (coingecko/, fred/, blockstream/)

### Silver Layer (Cleaned Data)
- **dbt Models**: Staging models with data type conversion and validation
- **Transformations**: JSON parsing, null handling, data quality flags
- **Purpose**: Cleaned, typed data ready for analysis

### Gold Layer (Analytics-Ready)
- **Marts**: Fact and dimension tables optimized for analysis
- **Key Tables**:
  - `fact_bitcoin_prices`: Daily Bitcoin metrics with economic context
  - `dim_economic_data`: Economic indicators with metadata and trends
  - `fact_bitcoin_network`: Network statistics and operational metrics

### Data Model Highlights

#### Economic Data Pivoting
```sql
-- Transform tall economic data into wide format for correlation analysis
economic_daily AS (
    SELECT 
        date,
        MAX(CASE WHEN series_key = 'federal_funds_rate' THEN numeric_value END) AS fed_funds_rate,
        MAX(CASE WHEN series_key = 'treasury_10y' THEN numeric_value END) AS treasury_10y,
        -- ... other indicators
    FROM economic_indicators
    GROUP BY date
)
```

#### Return Calculations
```sql
-- Proper return calculation for volatility analysis
CASE 
    WHEN LAG(price_usd, 1) OVER (ORDER BY date) > 0
    THEN (price_usd / LAG(price_usd, 1) OVER (ORDER BY date)) - 1
    ELSE NULL
END AS daily_return
```

#### Forward-Filling Economic Data
```sql
-- Handle weekend/holiday gaps in economic data
LAST_VALUE(fed_funds_rate IGNORE NULLS) OVER (
    ORDER BY date 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS fed_funds_rate
```

## 🔍 Key Insights & Analysis

### Economic Correlations Discovered
- **Interest Rate Sensitivity**: Bitcoin often shows inverse correlation with Federal funds rate
- **Inflation Hedge Properties**: Mixed correlation with Treasury yields during different market regimes
- **Risk Asset Behavior**: Positive correlation with S&P 500 during risk-on periods
- **Employment Impact**: Initial unemployment claims provide leading indicators for Bitcoin volatility

### Technical Analysis Features
- **Volatility Regimes**: 30-day rolling volatility classification
- **Risk Metrics**: Sharpe ratio calculation for risk-adjusted returns
- **Volume Confirmation**: Price-volume relationship analysis for trend validation
- **Trend Detection**: Multi-timeframe moving averages and momentum indicators

## 🧪 Data Quality & Testing

### dbt Tests Implemented
- **Schema Tests**: Primary key uniqueness, referential integrity
- **Data Quality Tests**: Null checks, range validations, enum value verification
- **Custom Tests**: Data freshness, extreme outlier detection, completeness scoring

### Error Handling
- **API Rate Limiting**: Intelligent retry logic with exponential backoff
- **Data Validation**: TRY_CAST for safe type conversions
- **Graceful Degradation**: Partial data processing continues on individual failures

## 📁 Project Structure

```
capstone-nealhalper/
├── 📂 data-ingestion/           # Data collection scripts
│   ├── coingecko_api.py        # Bitcoin price data
│   ├── fred_api.py             # Economic indicators
│   └── blockstream_api.py      # Network data
├── 📂 btc_economics_dataset/   # dbt project
│   ├── 📂 models/
│   │   ├── 📂 staging/         # Bronze → Silver transformations
│   │   ├── 📂 intermediate/    # Business logic layer
│   │   └── 📂 marts/           # Analytics-ready tables
│   ├── 📂 tests/               # Data quality tests
│   ├── 📂 macros/              # Reusable SQL functions
│   └── dbt_project.yml         # dbt configuration
├── 📂 dashboard/               # Streamlit analytics dashboard
│   └── streamlit_app.py
├── docker-compose.yml          # MinIO infrastructure
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## 🚦 Troubleshooting

### Common Issues

#### "No data available for analysis"
```bash
# Check data collection
python data-ingestion/coingecko_api.py
python data-ingestion/fred_api.py

# Verify dbt models
cd btc_economics_dataset
dbt run --models staging
dbt run --models marts
```

#### MinIO Connection Issues
```bash
# Restart MinIO container
docker-compose restart minio

# Check MinIO health
curl http://localhost:9000/minio/health/live
```

#### API Rate Limiting
- **CoinGecko**: Free tier allows 30 calls/minute
- **FRED**: 120 calls/minute limit
- **Blockstream**: Varies by endpoint (~700 calls/hour)

### Performance Optimization

#### For Large Datasets
```yaml
# dbt_project.yml - Increase memory for large operations
vars:
  memory_limit: '16GB'
  max_memory: '16GB'
  threads: 4
```

#### Dashboard Performance
- Data is cached for 5 minutes by default
- Adjust TTL in `@st.cache_data(ttl=300)` decorators
- Use smaller date ranges for initial exploration

## 🤝 Contributing

### Development Workflow
1. Fork the repository
2. Create feature branch (`git checkout -b feature/new-analysis`)
3. Make changes with appropriate tests
4. Run data quality checks (`dbt test`)
5. Submit pull request with detailed description

### Adding New Economic Indicators
1. Add series to data collection script
2. Update intermediate model for new category
3. Extend dimension table with metadata
4. Add to dashboard correlation analysis

## 🙏 Acknowledgments

- **Data Providers**: CoinGecko, Federal Reserve Economic Data (FRED), Blockstream
- **Open Source Tools**: dbt, DuckDB, Streamlit, Plotly
- **Educational Support**: Nashville Software School Data Engineering Program

## 📞 Contact

**Neal Halper**  
🔗 [LinkedIn](linkedin.com/in/neal-halper-95668718a)  
🐙 [GitHub](https://github.com/nealhalper)
