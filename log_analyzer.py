import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_log_file(uploaded_file) -> pd.DataFrame:
    """
    Parses an uploaded SECS/GEM log file and returns a structured Pandas DataFrame.
    """
    # This regex captures the main components of a standard log line.
    log_pattern = re.compile(
        r'^(?P<timestamp>\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}\.\d{6}),'
        r'\[(?P<log_type>[^\]]+)\],'
        r'(?P<details>.*)$'
    )
    
    parsed_data = []
    # Read from the uploaded file's in-memory text buffer
    for line in uploaded_file.getvalue().decode("utf-8").splitlines():
        match = log_pattern.match(line.strip())
        if match:
            log_entry = match.groupdict()
            details_str = log_entry['details']
            
            # This logic robustly parses key=value pairs, even if values contain spaces.
            pairs = re.split(r'(\w+=)', details_str)[1:]
            details = dict(zip(pairs[0::2], pairs[1::2]))
            details_cleaned = {k.replace('=', ''): v.strip().strip('"') for k, v in details.items()}
            
            log_entry.update(details_cleaned)
            del log_entry['details']
            parsed_data.append(log_entry)

    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)
    return clean_data(df)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, transforms, and enriches the DataFrame for analysis.
    """
    # Convert timestamp column to datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Convert TransactionID to a numeric type, handling errors
    if 'TransactionID' in df.columns:
        df['TransactionID'] = pd.to_numeric(df['TransactionID'], errors='coerce')
    
    # Extract Process Time from the 'Message' column
    if 'Message' in df.columns:
        time_pattern = r"Process time of the transaction\(ID=\d+\) is ([\d.]+) msec"
        df['ProcessTime_ms'] = df['Message'].str.extract(time_pattern, expand=False).astype(float)
        
    # Propagate 'MessageName' across all rows within the same transaction group
    if 'TransactionID' in df.columns and 'MessageName' in df.columns:
        df = df.sort_values(by=['TransactionID', 'timestamp'])
        df['MessageName'] = df.groupby('TransactionID')['MessageName'].ffill().bfill()
        
    return df

def get_summary_statistics(df: pd.DataFrame) -> dict:
    """Returns a dictionary of high-level summary statistics."""
    if df.empty:
        return {}
    duration = df['timestamp'].max() - df['timestamp'].min()
    return {
        "Total Entries": len(df),
        "Start Time": df['timestamp'].min(),
        "End Time": df['timestamp'].max(),
        "Duration": duration
    }

def analyze_transaction_performance(df: pd.DataFrame):
    """Analyzes transaction process times, returning stats and a figure."""
    required_cols = ['ProcessTime_ms', 'MessageName']
    if not all(col in df.columns for col in required_cols):
        return None, None

    perf_df = df.dropna(subset=required_cols)
    if perf_df.empty:
        return None, None
    
    performance_stats = perf_df.groupby('MessageName')['ProcessTime_ms'].describe()
    
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.boxplot(ax=ax, data=perf_df, x='ProcessTime_ms', y='MessageName', 
                order=performance_stats.sort_values('median', ascending=False).index)
    ax.set_title('Distribution of Process Times by Message Name')
    ax.set_xlabel('Process Time (ms)')
    ax.set_ylabel('Message Name')
    ax.set_xscale('log')
    plt.tight_layout()
    
    return performance_stats, fig

def analyze_event_frequency(df: pd.DataFrame):
    """Analyzes message frequency, returning counts and a figure."""
    if 'MessageName' not in df.columns:
        return None, None
        
    top_10_messages = df['MessageName'].value_counts().nlargest(10)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.barplot(ax=ax, y=top_10_messages.index, x=top_10_messages.values, orient='h')
    ax.set_title('Top 10 Most Frequent Message Names')
    ax.set_xlabel('Count')
    ax.set_ylabel('Message Name')
    plt.tight_layout()
    
    return top_10_messages, fig

def analyze_transaction_lifecycle(df: pd.DataFrame) -> dict:
    """Finds transactions that were started but never completed."""
    required_cols = ['Message', 'TransactionID']
    if not all(col in df.columns for col in required_cols):
        return {"error": "Lifecycle analysis requires 'Message' and 'TransactionID' columns."}
        
    added = df[df['Message'].str.contains("added to", na=False)]
    deleted = df[df['Message'].str.contains("deleted from", na=False)]
    
    added_ids = set(added['TransactionID'].dropna().unique())
    deleted_ids = set(deleted['TransactionID'].dropna().unique())
    
    orphaned_ids = added_ids - deleted_ids
    
    return {
        "initiated": len(added_ids),
        "completed": len(deleted_ids),
        "orphaned_count": len(orphaned_ids),
        "orphaned_ids": sorted(list(orphaned_ids))
    }

def detect_performance_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Identifies performance anomalies using the IQR method."""
    required_cols = ['ProcessTime_ms', 'MessageName']
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame()

    perf_df = df.dropna(subset=required_cols)
    if perf_df.empty:
        return pd.DataFrame()

    anomalies = []
    for name, group in perf_df.groupby('MessageName'):
        q1 = group['ProcessTime_ms'].quantile(0.25)
        q3 = group['ProcessTime_ms'].quantile(0.75)
        iqr = q3 - q1
        upper_bound = q3 + (1.5 * iqr)
        
        group_anomalies = group[group['ProcessTime_ms'] > upper_bound]
        
        if not group_anomalies.empty:
            anomalies.append(group_anomalies)

    if not anomalies:
        return pd.DataFrame()
    
    return pd.concat(anomalies).sort_values(by='timestamp')

def analyze_alarms(df: pd.DataFrame):
    """
    Finds and summarizes alarm/error events and critical anomalies from the log.
    """
    # Look for explicit errors OR messages containing "Unknown"
    alarm_events = df[df['Message'].str.contains("Alarm|fail|error|Unknown", case=False, na=False)]
    
    # Separately find impossible process times
    if 'ProcessTime_ms' in df.columns:
        negative_time_anomalies = df[df['ProcessTime_ms'] < 0]
        if not negative_time_anomalies.empty:
            # Combine both types of anomalies into one DataFrame
            alarm_events = pd.concat([alarm_events, negative_time_anomalies]).drop_duplicates().sort_values(by='timestamp')

    if alarm_events.empty:
        return None, None
        
    alarm_summary = alarm_events['Message'].value_counts().reset_index()
    alarm_summary.columns = ['AlarmMessage', 'Frequency']
    
    return alarm_summary, alarm_events

def get_context_around_event(df: pd.DataFrame, event_timestamp, window_minutes=5):
    """Extracts log entries and key state data within a time window around a specific event."""
    start_time = event_timestamp - pd.Timedelta(minutes=window_minutes)
    end_time = event_timestamp + pd.Timedelta(minutes=window_minutes)
    
    contextual_logs = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    
    state_before_event = df[df['timestamp'] < event_timestamp]
    
    context = {}
    key_identifiers = ['OperatorID', 'MagazineID', 'LotID', 'PortID']
    
    for identifier in key_identifiers:
        if identifier in state_before_event.columns and not state_before_event[identifier].dropna().empty:
            context[f'Last Known {identifier}'] = state_before_event[identifier].dropna().iloc[-1]
            
    return contextual_logs, context
