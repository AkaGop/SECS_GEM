import streamlit as st
import pandas as pd
import log_analyzer as la

@st.cache_data
def load_data(uploaded_file):
    """Loads and processes the log file."""
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
        # --- High-Level Summary ---
        st.header("High-Level Summary")
        summary = la.get_summary_statistics(df)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Log Entries", f"{summary['Total Entries']:,}")
        col2.metric("Log Start Time (UTC)", summary['Start Time'].strftime("%H:%M:%S"))
        col3.metric("Log End Time (UTC)", summary['End Time'].strftime("%H:%M:%S"))
        
        if 'Message' in df.columns:
            panels_processed = df['Message'].str.contains("UnloadedFromTool|LoadedToToolCompleted", case=False, na=False).sum()
            col4.metric("Panels Processed", f"{panels_processed:,}")

        # --- Main Tabs ---
        op_tab, maint_tab = st.tabs(["Operational Analysis", "Maintenance & Alarm Analysis"])

        with op_tab:
            st.header("Operational Overview")
            sub_tab1, sub_tab2, sub_tab3 = st.tabs(["Transaction Performance", "Event Frequency", "Advanced Audits"])
            
            with sub_tab1:
                perf_stats, perf_fig = la.analyze_transaction_performance(df)
                if perf_stats is not None:
                    st.subheader("Transaction Process Time Analysis")
                    st.dataframe(perf_stats)
                    st.pyplot(perf_fig)
                else:
                    st.warning("No process time data found.")

            with sub_tab2:
                top_messages, freq_fig = la.analyze_event_frequency(df)
                if top_messages is not None:
                    st.subheader("Event and Message Frequency")
                    st.dataframe(top_messages)
                    st.pyplot(freq_fig)
                else:
                    st.warning("No 'MessageName' data available.")
            
            with sub_tab3:
                st.subheader("Transaction Lifecycle Audit")
                # ... (Lifecycle audit code remains the same)

                st.subheader("Automated Performance Anomaly Detection")
                # ... (Anomaly detection code remains the same)
        
        with maint_tab:
            st.header("Maintenance & Downtime Analysis")
            
            # --- NEW: CURRENT STATE SNAPSHOT ---
            st.subheader("Current State Snapshot (at end of log)")
            snap_cols = st.columns(4)
            
            # Find the last known value for each key identifier
            last_state = {col: df[col].dropna().iloc[-1] for col in ['ControlState', 'OperatorID', 'LotID', 'MagazineID'] if col in df.columns and not df[col].dropna().empty}
            
            snap_cols[0].metric("Control State", last_state.get('ControlState', 'N/A'))
            snap_cols[1].metric("Operator ID", last_state.get('OperatorID', 'N/A'))
            snap_cols[2].metric("Lot ID", last_state.get('LotID', 'N/A'))
            snap_cols[3].metric("Magazine ID", last_state.get('MagazineID', 'N/A'))

            st.divider()

            issue_summary, all_issues = la.analyze_alarms(df)
            
            if issue_summary is None:
                st.success("No alarms, errors, or protocol issues were detected in this log file.")
            else:
                st.subheader("🚨 Potential Machine-Halting Events")
                halting_events = la.find_halting_events(df, all_issues)
                if not halting_events.empty:
                    st.write("These are critical issues that were followed by a period of NO processing activity, indicating a potential machine halt.")
                    st.dataframe(halting_events[['timestamp', 'Message', 'TransactionID']])
                else:
                    st.info("No events were directly correlated with a machine halt in this log.")

                st.subheader("All Detected Issues & Alarms")
                st.dataframe(issue_summary)

                st.subheader("Downtime Event Drill-Down")
                if not all_issues.empty:
                    all_issues['display'] = all_issues['timestamp'].astype(str) + " - " + all_issues['Message']
                    selected_event_display = st.selectbox("Select an Issue/Alarm Event:", options=all_issues['display'], key="alarm_select")

                    if selected_event_display:
                        selected_event_row = all_issues[all_issues['display'] == selected_event_display].iloc[0]
                        context_logs, context_data = la.get_context_around_event(df, selected_event_row['timestamp'])
                        
                        st.write("#### Context at Time of Event")
                        st.json(context_data)

                        st.write(f"#### Log Timeline (5 minutes before and after the event)")
                        st.dataframe(context_logs)
                else:
                    st.info("No specific alarm events to select.")

        with st.expander("Show Full Enriched Data (Raw)"):
            st.dataframe(df)
