import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def parse_log_file(uploaded_file) -> pd.DataFrame:
    """
    Parses an uploaded SECS/GEM log file and returns a structured DataFrame.
    """
    log_pattern = re.compile(
        r'^(?P<timestamp>\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}\.\d{6}),'
        r'\[(?P<log_type>[^\]]+)\],'
        r'(?P<details>.*)$'
    )
    details_pattern = re.compile(r'(\w+)=([^,]+(?:\s[^,]+)*)')

    parsed_data = []
    # Read from the uploaded file's in-memory buffer
    for line in uploaded_file.getvalue().decode("utf-8").splitlines():
        match = log_pattern.match(line.strip())
        if match:
            log_entry = match.groupdict()
            details = dict(details_pattern.findall(log_entry['details']))
            log_entry.update(details)
            del log_entry['details']
            parsed_data.append(log_entry)

    if not parsed_data:
        return pd.DataFrame()

    df = pd.DataFrame(parsed_data)
    return clean_data(df)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and transforms the DataFrame columns for analysis."""
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    if 'TransactionID' in df.columns:
        df['TransactionID'] = pd.to_numeric(df['TransactionID'], errors='coerce')
    if 'Message' in df.columns:
        time_pattern = r"Process time of the transaction\(ID=\d+\) is ([\d.]+) msec"
        df['ProcessTime_ms'] = df['Message'].str.extract(time_pattern).astype(float)
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
    """Analyzes transaction process times and returns stats and a figure."""
    perf_df = df.dropna(subset=['ProcessTime_ms', 'MessageName'])
    if perf_df.empty:
        return None, None
    
    performance_stats = perf_df.groupby('MessageName')['ProcessTime_ms'].describe()
    
    # Visualization
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
    """Analyzes the frequency of messages and returns counts and a figure."""
    if df.empty or 'MessageName' not in df.columns:
        return None, None
        
    top_10_messages = df['MessageName'].value_counts().nlargest(10)
    
    # Visualization
    fig, ax = plt.subplots(figsize=(12, 8))
    sns.barplot(ax=ax, y=top_10_messages.index, x=top_10_messages.values, orient='h')
    ax.set_title('Top 10 Most Frequent Message Names')
    ax.set_xlabel('Count')
    ax.set_ylabel('Message Name')
    plt.tight_layout()
    
    return top_10_messages, fig

def analyze_transaction_lifecycle(df: pd.DataFrame) -> dict:
    """Finds transactions that were started but never completed."""
    if 'Message' not in df.columns or 'TransactionID' not in df.columns:
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
