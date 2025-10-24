import streamlit as st
import pandas as pd
import log_analyzer as la

@st.cache_data
def load_data(uploaded_file):
    df = la.parse_log_file(uploaded_file)
    return df

st.set_page_config(layout="wide")
st.title("SECS/GEM Log Analysis Dashboard")

st.sidebar.title("Upload Log File")
uploaded_file = st.sidebar.file_uploader("Choose a log file (.txt)", type="txt")

if uploaded_file is None:
    st.info("Please upload a log file using the sidebar to begin analysis.")
else:
    df = load_data(uploaded_file)

    if df.empty:
        st.error("Could not parse any data from the uploaded file. Please check the file format.")
    else:
        st.header("High-Level Summary")
        summary = la.get_summary_statistics(df)
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Log Entries", f"{summary['Total Entries']:,}")
        col2.metric("Log Start Time (UTC)", summary['Start Time'].strftime("%H:%M:%S"))
        col3.metric("Log End Time (UTC)", summary['End Time'].strftime("%H:%M:%S"))
        
        with st.expander("Show Full Parsed Data"):
            st.dataframe(df)

        tab1, tab2, tab3 = st.tabs(["Transaction Performance", "Event Frequency", "Lifecycle Audit"])

        with tab1:
            st.subheader("Transaction Process Time Analysis")
            perf_stats, perf_fig = la.analyze_transaction_performance(df)
            
            # --- ADDED CHECK ---
            if perf_stats is not None and perf_fig is not None:
                st.write("Descriptive statistics for transaction times (in milliseconds):")
                st.dataframe(perf_stats)
                st.pyplot(perf_fig)
            else:
                st.warning("No process time data found in the log file to generate a performance analysis.")

        with tab2:
            st.subheader("Event and Message Frequency")
            top_messages, freq_fig = la.analyze_event_frequency(df)

            # --- ADDED CHECK ---
            if top_messages is not None and freq_fig is not None:
                st.write("Top 10 most frequent messages:")
                st.dataframe(top_messages)
                st.pyplot(freq_fig)
            else:
                st.warning("No 'MessageName' data found to generate a frequency analysis.")

        with tab3:
            st.subheader("Transaction Lifecycle Audit")
            lifecycle_data = la.analyze_transaction_lifecycle(df)

            # --- ADDED CHECK ---
            if "error" in lifecycle_data:
                st.warning(lifecycle_data["error"])
            else:
                st.metric("Initiated Transactions", f"{lifecycle_data.get('initiated', 0):,}")
                st.metric("Completed Transactions", f"{lifecycle_data.get('completed', 0):,}")
                
                if lifecycle_data.get('orphaned_count', 0) > 0:
                    st.error(f"Found {lifecycle_data['orphaned_count']} orphaned transactions (started but not completed).")
                    with st.expander("Show Orphaned Transaction IDs"):
                        st.write(lifecycle_data['orphaned_ids'])
                else:
                    st.success("All initiated transactions were successfully completed.")
