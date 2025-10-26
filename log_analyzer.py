import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_log_file(uploaded_file) -> pd.DataFrame:
    """
    Parses an uploaded SECS/GEM log file and returns a structured Pandas DataFrame.
    """
    log_pattern = re.compile(
        r'^(?P<timestamp>\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}\.\d{6}),'
        r'\[(?P<log_type>[^\]]+)\],'
        r'(?P<details>.*)$'
    )
    
    parsed_data = []
    # Read from the uploaded file's in-memory buffer
    for line in uploaded_file.getvalue().decode("utf-8").splitlines():
        match = log_pattern.match(line.strip())
        if match:
            log_entry = match.groupdict()
            details_str = log_entry['details']
            
            pairs = re.split(r'(\w+=)', details_str)[1:]
            details = dict(zip(pairs[0::2], pairs[1::2]))
            details_cleaned = {k.replace('=', ''): v.strip().strip('"') for k, v in details.items()}
            
            log_entry.update(details_cleaned)
            del log_entry['details']
            parsed_data.append(log_entry)

    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)
    return clean_and_enrich_data(df)

def clean_and_enrich_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, transforms, and enriches the DataFrame by extracting detailed engineering data.
    """
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    if 'TransactionID' in df.columns:
        df['TransactionID'] = pd.to_numeric(df['TransactionID'], errors='coerce')
    
    if 'Message' in df.columns:
        # Extract Process Time
        time_pattern = r"Process time of the transaction\(ID=\d+\) is ([\d.-]+) msec"
        df['ProcessTime_ms'] = df['Message'].str.extract(time_pattern, expand=False).astype(float)
        
        # --- NEW DATA EXTRACTION FOR MAINTENANCE ENGINEER ---
        df['OperatorID'] = df['Message'].str.extract(r"OPERATORID(?:' | >\s*)<A\[\d+\] \"(\w+)\"")
        df['MagazineID'] = df['Message'].str.extract(r"MAGAZINEID(?:' | >\s*)<A\[\d+\] \"(\w+)\"")
        df['LotID'] = df['Message'].str.extract(r"LOTID(?:' | >\s*)<A\[\d+\] \"(.*?)\"")
        df['PanelID'] = df['Message'].str.extract(r"PanelID<A\[\d+\] \"(.*?)\"")
        df['PortID'] = df['Message'].str.extract(r"PORTID(?:' | >\s*)<A\[\d+\] \"(\w+)\"")
        df['SourcePortID'] = df['Message'].str.extract(r"SRCPORTID(?:' | >\s*)<A\[\d+\] \"(\w+)\"")
        df['DestPortID'] = df['Message'].str.extract(r"DESTPORTID(?:' | >\s*)<A\[\d+\] \"(\w+)\"")
        
        # Forward fill extracted data to make it available for context
        cols_to_fill = ['OperatorID', 'MagazineID', 'LotID', 'PortID', 'SourcePortID', 'DestPortID']
        for col in cols_to_fill:
            if col in df.columns:
                df[col] = df[col].ffill()

    # Propagate MessageName across transaction groups
    if 'TransactionID' in df.columns and 'MessageName' in df.columns:
        df = df.sort_values(by=['TransactionID', 'timestamp'])
        df['MessageName'] = df.groupby('TransactionID')['MessageName'].ffill().bfill()
        
    # Track Machine Control State (Local/Remote)
    if 'Message' in df.columns:
        df['ControlStateChange'] = df['Message'].str.extract(r"(LOCAL|REMOTE)")
        df['ControlState'] = df['ControlStateChange'].ffill()

    return df

def get_summary_statistics(df: pd.DataFrame) -> dict:
    # ... (This function remains the same)
    if df.empty: return {}
    return {
        "Total Entries": len(df),
        "Start Time": df['timestamp'].min(),
        "End Time": df['timestamp'].max(),
        "Duration": df['timestamp'].max() - df['timestamp'].min()
    }

def analyze_transaction_performance(df: pd.DataFrame):
    # ... (This function remains the same)
    required_cols = ['ProcessTime_ms', 'MessageName']
    if not all(col in df.columns for col in required_cols): return None, None
    perf_df = df.dropna(subset=required_cols)
    if perf_df.empty: return None, None
    performance_stats = perf_df.groupby('MessageName')['ProcessTime_ms'].describe()
    fig, ax = plt.subplots(figsize=(12, 8)); sns.boxplot(ax=ax, data=perf_df, x='ProcessTime_ms', y='MessageName', order=performance_stats.sort_values('median', ascending=False).index); ax.set_title('Distribution of Process Times by Message Name'); ax.set_xlabel('Process Time (ms)'); ax.set_ylabel('Message Name'); ax.set_xscale('log'); plt.tight_layout();
    return performance_stats, fig

def analyze_event_frequency(df: pd.DataFrame):
    # ... (This function remains the same)
    if 'MessageName' not in df.columns: return None, None
    top_10_messages = df['MessageName'].value_counts().nlargest(10)
    fig, ax = plt.subplots(figsize=(12, 8)); sns.barplot(ax=ax, y=top_10_messages.index, x=top_10_messages.values, orient='h'); ax.set_title('Top 10 Most Frequent Message Names'); ax.set_xlabel('Count'); ax.set_ylabel('Message Name'); plt.tight_layout();
    return top_10_messages, fig

def analyze_transaction_lifecycle(df: pd.DataFrame) -> dict:
    # ... (This function remains the same)
    required_cols = ['Message', 'TransactionID'];
    if not all(col in df.columns for col in required_cols): return {"error": "Lifecycle analysis requires 'Message' and 'TransactionID' columns."}
    added = df[df['Message'].str.contains("added to", na=False)]; deleted = df[df['Message'].str.contains("deleted from", na=False)]; added_ids = set(added['TransactionID'].dropna().unique()); deleted_ids = set(deleted['TransactionID'].dropna().unique()); orphaned_ids = added_ids - deleted_ids
    return {"initiated": len(added_ids), "completed": len(deleted_ids), "orphaned_count": len(orphaned_ids), "orphaned_ids": sorted(list(orphaned_ids))}

def detect_performance_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    # ... (This function remains the same)
    required_cols = ['ProcessTime_ms', 'MessageName'];
    if not all(col in df.columns for col in required_cols): return pd.DataFrame()
    perf_df = df.dropna(subset=required_cols)
    if perf_df.empty: return pd.DataFrame()
    anomalies = [];
    for name, group in perf_df.groupby('MessageName'):
        q1 = group['ProcessTime_ms'].quantile(0.25); q3 = group['ProcessTime_ms'].quantile(0.75); iqr = q3 - q1; upper_bound = q3 + (1.5 * iqr)
        group_anomalies = group[group['ProcessTime_ms'] > upper_bound]
        if not group_anomalies.empty: anomalies.append(group_anomalies)
    if not anomalies: return pd.DataFrame()
    return pd.concat(anomalies).sort_values(by='timestamp')

def analyze_alarms(df: pd.DataFrame):
    """
    Identifies alarms, errors, and critical protocol issues.
    """
    keyword_events = df[df['Message'].str.contains("Alarm|fail|error", case=False, na=False)]
    unknown_events = df[df['Message'].str.contains("Unknown:", case=False, na=False)]
    negative_time_events = pd.DataFrame()
    if 'ProcessTime_ms' in df.columns:
        negative_time_events = df[df['ProcessTime_ms'] < 0]
    all_potential_issues = pd.concat([keyword_events, unknown_events, negative_time_events]).drop_duplicates().sort_values(by='timestamp')
    if all_potential_issues.empty:
        return None, None
    issue_summary = all_potential_issues['Message'].value_counts().reset_index()
    issue_summary.columns = ['Issue/Alarm Message', 'Frequency']
    return issue_summary, all_potential_issues

def find_halting_events(df: pd.DataFrame, potential_issues: pd.DataFrame, halt_threshold_minutes: int = 5) -> pd.DataFrame:
    # ... (This function remains the same)
    if potential_issues is None or potential_issues.empty: return pd.DataFrame()
    processing_keywords = "LOAD|UNLOAD|TRANSFER|PROCESS|MAPPING|DOCK"; processing_events = df[df['Message'].str.contains(processing_keywords, case=False, na=False)]; halting_events = []
    for index, issue in potential_issues.iterrows():
        issue_time = issue['timestamp']; window_end_time = issue_time + pd.Timedelta(minutes=halt_threshold_minutes)
        activity_after_issue = processing_events[(processing_events['timestamp'] > issue_time) & (processing_events['timestamp'] <= window_end_time)]
        if activity_after_issue.empty: halting_events.append(issue)
    if not halting_events: return pd.DataFrame()
    return pd.DataFrame(halting_events)

def get_context_around_event(df: pd.DataFrame, event_timestamp, window_minutes=5):
    """
    Extracts log entries and an enriched context dictionary around a specific event.
    """
    start_time = event_timestamp - pd.Timedelta(minutes=window_minutes)
    end_time = event_timestamp + pd.Timedelta(minutes=window_minutes)
    contextual_logs = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
    state_before_event = df[df['timestamp'] <= event_timestamp]
    context = {}
    key_identifiers = ['ControlState', 'OperatorID', 'MagazineID', 'LotID', 'PortID', 'SourcePortID', 'DestPortID']
    for identifier in key_identifiers:
        if identifier in state_before_event.columns and not state_before_event[identifier].dropna().empty:
            context[f'Last Known {identifier}'] = state_before_event[identifier].dropna().iloc[-1]
    return contextual_logs, context
