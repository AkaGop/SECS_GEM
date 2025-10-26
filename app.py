import streamlit as st
import pandas as pd
import log_analyzer as la

# Use Streamlit's caching to avoid re-parsing the file on every interaction
@st.cache_data
def load_data(uploaded_file):
    """Loads and processes the log file."""
    df = la.parse_log_file(uploaded_file)
    return df

# --- App Layout ---
st.set_page_config(layout="wide")
st.title("SECS/GEM Log Analysis Dashboard")

# Sidebar for file upload
st.sidebar.title("Upload Log File")
uploaded_file = st.sidebar.file_uploader("Choose a log file (.txt)", type="txt")

if uploaded_file is None:
    st.info("Please upload a log file using the sidebar to begin analysis.")
else:
    # --- Main App Logic ---
    df = load_data(uploaded_file)

    if df.empty:
        st.error("Could not parse any data from the uploaded file. Please check the file format.")
    else:
        # 1. Display Summary
        st.header("High-Level Summary")
        summary = la.get_summary_statistics(df)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Log Entries", f"{summary['Total Entries']:,}")
        col2.metric("Log Start Time (UTC)", summary['Start Time'].strftime("%H:%M:%S"))
        col3.metric("Log End Time (UTC)", summary['End Time'].strftime("%H:%M:%S"))
        
        # Calculate and display total panels processed
        if 'Message' in df.columns:
            panels_processed = df['Message'].str.contains("UnloadedFromTool|LoadedToToolCompleted", case=False, na=False).sum()
            col4.metric("Panels Processed", f"{panels_processed:,}")

        # 2. Create Tabs for Different User Workflows
        op_tab, maint_tab = st.tabs(["Operational Analysis", "Maintenance & Alarm Analysis"])

        with op_tab:
            st.header("Operational Overview")
            sub_tab1, sub_tab2, sub_tab3 = st.tabs(["Transaction Performance", "Event Frequency", "Advanced Audits"])
            
            with sub_tab1:
                perf_stats, perf_fig = la.analyze_transaction_performance(df)
                if perf_stats is not None:
                    st.subheader("Transaction Process Time Analysis")
                    st.write("Descriptive statistics for transaction times (in milliseconds):")
                    st.dataframe(perf_stats)
                    st.pyplot(perf_fig)
                else:
                    st.warning("No process time data found to generate a performance analysis.")

            with sub_tab2:
                top_messages, freq_fig = la.analyze_event_frequency(df)
                if top_messages is not None:
                    st.subheader("Event and Message Frequency")
                    st.write("Top 10 most frequent messages:")
                    st.dataframe(top_messages)
                    st.pyplot(freq_fig)
                else:
                    st.warning("No 'MessageName' data available for frequency analysis.")
            
            with sub_tab3:
                st.subheader("Transaction Lifecycle Audit")
                lifecycle_data = la.analyze_transaction_lifecycle(df)
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

                st.subheader("Automated Performance Anomaly Detection")
                anomalies_df = la.detect_performance_anomalies(df)
                if not anomalies_df.empty:
                    st.error(f"Found {len(anomalies_df)} performance anomalies.")
                    display_cols = ['timestamp', 'TransactionID', 'MessageName', 'ProcessTime_ms']
                    st.dataframe(anomalies_df[display_cols])
                else:
                    st.success("No significant performance anomalies were detected in this log file.")

        with maint_tab:
            st.header("Maintenance & Downtime Analysis")
            alarm_summary, alarm_events = la.analyze_alarms(df)
            
            if alarm_summary is None:
                st.success("No alarm or error messages were detected in this log file.")
            else:
                st.subheader("Alarm Frequency")
                st.write("This table shows the most common alarms and errors found in the log.")
                st.dataframe(alarm_summary)

                st.subheader("Downtime Event Drill-Down")
                st.write("Select a specific alarm event to see the surrounding log activity and context.")

                if not alarm_events.empty:
                    alarm_events['display'] = alarm_events['timestamp'].astype(str) + " - " + alarm_events['Message']
                    selected_event_display = st.selectbox("Select an Alarm Event:", options=alarm_events['display'])

                    if selected_event_display:
                        selected_event_row = alarm_events[alarm_events['display'] == selected_event_display].iloc[0]
                        context_logs, context_data = la.get_context_around_event(df, selected_event_row['timestamp'])
                        
                        st.write("#### Context at Time of Event")
                        if not context_data:
                            st.warning("No contextual data (Operator, Lot, etc.) found before this event.")
                        else:
                            st.json(context_data)

                        st.write(f"#### Log Timeline (5 minutes before and after the event)")
                        st.dataframe(context_logs)
                else:
                    st.info("No specific alarm events to select.")

        with st.expander("Show Full Parsed Data (Raw)"):
            st.dataframe(df)
