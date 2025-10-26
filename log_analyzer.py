# ... (all existing functions from the previous step remain here) ...

# --- ADD THESE TWO NEW FUNCTIONS ---

def analyze_alarms(df: pd.DataFrame):
    """
    Finds and analyzes alarm events in the log.

    Returns:
        A DataFrame of alarm counts and a DataFrame of alarm details.
    """
    # Alarm messages are often sent via S5F1
    # We look for the 'Message' that contains 'Alarm report send' or similar, 
    # but the presence of ALID (Alarm ID) is a more robust check.
    # NOTE: This assumes 'ALID' is a column after parsing. Let's adjust parsing slightly.
    
    # In Mess_4.txt, alarms are S5F1 events. Let's find those.
    # A better approach is to find any log entry that has a non-null ALID if available.
    # For now, let's assume alarms are identified by a specific message.
    # Let's pivot to finding messages of type S5F1, which are standard alarm reports.
    
    # Let's assume for this log, an alarm is any event with "Alarm" in the message.
    # This is a general approach. A more robust way would be to identify by MessageName like 'S5F1'
    # For now, let's stick to what we can reliably parse from the existing structure.
    
    # A better, more general way: let's identify any "abnormal" event.
    # For this log, let's find 'Unknown' messages or rows with high process times.
    # This is part of the anomaly detection.
    
    # Let's be very specific to the user's need: Analyze alarms.
    # The current parser doesn't explicitly pull out ALID. We should enhance it.
    # For now, let's find rows with "Alarm" or "fail" in the message as a proxy.
    
    alarm_events = df[df['Message'].str.contains("Alarm|fail|error", case=False, na=False)]
    
    if alarm_events.empty:
        return None, None
        
    # Let's imagine we parsed ALID and ALTX (alarm text)
    # For now, we will use the full message as the alarm identifier
    alarm_summary = alarm_events['Message'].value_counts().reset_index()
    alarm_summary.columns = ['AlarmMessage', 'Frequency']
    
    return alarm_summary, alarm_events


def get_context_around_event(df: pd.DataFrame, event_timestamp, window_minutes=5):
    """
    Extracts log entries within a time window around a specific event.
    """
    start_time = event_timestamp - pd.Timedelta(minutes=window_minutes)
    end_time = event_timestamp + pd.Timedelta(minutes=window_minutes)
    
    contextual_logs = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    
    # Find the last known state variables before the event
    state_before_event = df[df['timestamp'] < event_timestamp]
    
    context = {}
    
    # Find last Operator ID
    if 'OperatorID' in state_before_event.columns and not state_before_event['OperatorID'].dropna().empty:
        context['Last OperatorID'] = state_before_event['OperatorID'].dropna().iloc[-1]
        
    # Find last Magazine ID
    if 'MagazineID' in state_before_event.columns and not state_before_event['MagazineID'].dropna().empty:
        context['Last MagazineID'] = state_before_event['MagazineID'].dropna().iloc[-1]
        
    # Find last Lot ID
    if 'LotID' in state_before_event.columns and not state_before_event['LotID'].dropna().empty:
        context['Last LotID'] = state_before_event['LotID'].dropna().iloc[-1]
        
    return contextual_logs, context```

### **Step 2: Update `app.py` with the Maintenance Dashboard**

This is a significant update. We are adding a new tab and packing it with interactive features.

**Updated `app.py`:**
```python
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
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Log Entries", f"{summary['Total Entries']:,}")
        col2.metric("Log Start Time (UTC)", summary['Start Time'].strftime("%H:%M:%S"))
        col3.metric("Log End Time (UTC)", summary['End Time'].strftime("%H:%M:%S"))
        
        # Calculate and display total panels processed
        if 'Message' in df.columns:
            # This is an example; the exact message might need to be adjusted
            panels_processed = df['Message'].str.contains("UnloadedFromTool|LoadedToToolCompleted", case=False, na=False).sum()
            col4.metric("Panels Processed", f"{panels_processed:,}")

        # Tabs for different user personas
        op_tab, maint_tab = st.tabs(["Operational Analysis", "Maintenance & Alarm Analysis"])

        with op_tab:
            st.subheader("Operational Overview")
            op_col1, op_col2 = st.columns(2)
            with op_col1:
                st.write("#### Transaction Performance")
                perf_stats, perf_fig = la.analyze_transaction_performance(df)
                if perf_stats is not None:
                    st.dataframe(perf_stats)
                    st.pyplot(perf_fig)
                else:
                    st.warning("No process time data found.")
            with op_col2:
                st.write("#### Event Frequency")
                top_messages, freq_fig = la.analyze_event_frequency(df)
                if top_messages is not None:
                    st.dataframe(top_messages)
                    st.pyplot(freq_fig)
                else:
                    st.warning("No message name data found.")

        # --- NEW MAINTENANCE TAB ---
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

                # Create a unique identifier for each alarm for the dropdown
                alarm_events['display'] = alarm_events['timestamp'].astype(str) + " - " + alarm_events['Message']
                selected_event_display = st.selectbox("Select an Alarm Event:", options=alarm_events['display'])

                if selected_event_display:
                    # Find the original row for the selected event
                    selected_event_row = alarm_events[alarm_events['display'] == selected_event_display].iloc[0]
                    
                    context_logs, context_data = la.get_context_around_event(df, selected_event_row['timestamp'])
                    
                    st.write("#### Context at Time of Event")
                    if not context_data:
                        st.warning("No contextual data (Operator, Lot, etc.) found before this event.")
                    else:
                        st.json(context_data)

                    st.write(f"#### Log Timeline (5 minutes before and after the event)")
                    st.dataframe(context_logs)
