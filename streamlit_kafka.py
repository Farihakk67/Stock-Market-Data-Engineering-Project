import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
from datetime import datetime
import time

st.set_page_config(layout="wide", page_title="Kafka Stock Dashboard")
st.title("📊 Real Kafka Stock Dashboard")
st.markdown("---")

# Initialize session state
if 'kafka_data' not in st.session_state:
    st.session_state.kafka_data = []

# Status panel
st.sidebar.header("🔧 System Status")

# Check Kafka connection
try:
    test_consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        consumer_timeout_ms=1000
    )
    topics = test_consumer.topics()
    test_consumer.close()
    st.sidebar.success("✅ Kafka Connected")
    st.sidebar.info(f"Topic: stock_prices")
except Exception as e:
    st.sidebar.error("❌ Kafka Error")
    st.sidebar.info("Run: docker-compose up -d")

# Control buttons
st.sidebar.header("📊 Controls")

# Number of messages to fetch
num_messages = st.sidebar.slider("Messages to fetch", 1, 20, 5)

if st.sidebar.button("📥 FETCH MESSAGES", type="primary"):
    with st.spinner(f"Fetching {num_messages} messages from Kafka..."):
        try:
            # CRITICAL: Create consumer with correct config
            consumer = KafkaConsumer(
                'stock_prices',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',  # Read from beginning
                enable_auto_commit=False,      # Don't commit offsets
                consumer_timeout_ms=5000        # Wait 5 seconds
            )
            
            messages_fetched = 0
            new_messages = []
            
            # Get messages
            for message in consumer:
                data = message.value
                data['fetch_time'] = datetime.now().strftime("%H:%M:%S")
                data['offset'] = message.offset
                
                st.session_state.kafka_data.append(data)
                new_messages.append(data)
                messages_fetched += 1
                
                if messages_fetched >= num_messages:
                    break
            
            consumer.close()
            
            if messages_fetched > 0:
                st.sidebar.success(f"✅ Fetched {messages_fetched} messages!")
                st.rerun()  # Refresh to show data
            else:
                st.sidebar.warning("⚠️ No messages found!")
                st.sidebar.info("Run: python stock_producer.py first")
                
        except Exception as e:
            st.sidebar.error(f"Error: {str(e)}")

if st.sidebar.button("🗑️ Clear All Data"):
    st.session_state.kafka_data = []
    st.rerun()

# Display connection info
st.sidebar.markdown("---")
st.sidebar.info("""
**Steps:**
1. Run producer: `python stock_producer.py`
2. Click FETCH button above
3. Messages will appear
""")

# MAIN DASHBOARD AREA
st.subheader("📊 Dashboard")

if st.session_state.kafka_data:
    # Show metrics
    total_msgs = len(st.session_state.kafka_data)
    latest = st.session_state.kafka_data[-1]
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Messages", total_msgs)
    col2.metric("Latest Index", latest.get('Index', 'N/A'))
    col3.metric("Latest Price", f"${latest.get('Close', 0):.2f}")
    col4.metric("Latest Date", latest.get('Date', 'N/A'))
    
    st.markdown("---")
    
    # Show data in tabs
    tab1, tab2, tab3 = st.tabs(["📋 Data Table", "📈 Charts", "🔍 Raw JSON"])
    
    with tab1:
        # Prepare data for table
        table_data = []
        for msg in st.session_state.kafka_data[-20:]:  # Last 20
            table_data.append({
                'Time': msg.get('fetch_time', ''),
                'Index': msg.get('Index', ''),
                'Date': msg.get('Date', ''),
                'Price': f"${msg.get('Close', 0):.2f}",
                'Volume': f"{msg.get('Volume', 0):,}",
                'Open': f"${msg.get('Open', 0):.2f}",
                'High': f"${msg.get('High', 0):.2f}",
                'Low': f"${msg.get('Low', 0):.2f}"
            })
        
        if table_data:
            st.dataframe(pd.DataFrame(table_data), use_container_width=True)
        else:
            st.info("No data to display")
    
    with tab2:
        if len(st.session_state.kafka_data) >= 2:
            # Create chart data
            chart_df = pd.DataFrame({
                'Message': range(1, len(st.session_state.kafka_data) + 1),
                'Price': [msg.get('Close', 0) for msg in st.session_state.kafka_data]
            })
            st.line_chart(chart_df.set_index('Message'))
            
            # Volume chart
            vol_df = pd.DataFrame({
                'Message': range(1, len(st.session_state.kafka_data) + 1),
                'Volume': [msg.get('Volume', 0) for msg in st.session_state.kafka_data]
            })
            st.bar_chart(vol_df.set_index('Message'))
        else:
            st.info("Need at least 2 messages for charts")
    
    with tab3:
        for i, msg in enumerate(st.session_state.kafka_data[-5:]):  # Last 5
            st.json(msg)
    
    # Show all data expander
    with st.expander("View All Fetched Data"):
        st.write(f"Total messages in session: {total_msgs}")
        for i, msg in enumerate(st.session_state.kafka_data):
            st.text(f"{i+1}. {msg.get('Index', 'N/A')} - ${msg.get('Close', 0)} - {msg.get('Date', 'N/A')}")
            
else:
    # Empty state
    st.info("👈 Click 'FETCH MESSAGES' in the sidebar to load data from Kafka")
    
    # Show sample of what to expect
    st.markdown("---")
    st.subheader("📝 Sample Data Structure")
    sample = {
        "Index": "HSI",
        "Date": "1990-11-21",
        "Open": 3031,
        "High": 3031,
        "Low": 2991,
        "Close": 3013,
        "Volume": 0
    }
    st.json(sample)

st.markdown("---")
st.caption("Kafka Stock Market Data Engineering Project | Live Data Pipeline")