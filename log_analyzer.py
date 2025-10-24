# --- ADD THIS NEW FUNCTION to log_analyzer.py ---

def detect_performance_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifies performance anomalies using the IQR method.

    An anomaly is a data point that falls above Q3 + 1.5 * IQR.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing only the anomalous log entries, or an empty DataFrame if none are found.
    """
    required_cols = ['ProcessTime_ms', 'MessageName']
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame() # Return empty if columns don't exist

    perf_df = df.dropna(subset=required_cols)
    if perf_df.empty:
        return pd.DataFrame()

    # Group by message name to calculate anomalies for each type of transaction
    anomalies = []
    for name, group in perf_df.groupby('MessageName'):
        q1 = group['ProcessTime_ms'].quantile(0.25)
        q3 = group['ProcessTime_ms'].quantile(0.75)
        iqr = q3 - q1
        
        # Define the upper bound for outlier detection
        upper_bound = q3 + (1.5 * iqr)
        
        # Find all data points in the group that are above the upper bound
        group_anomalies = group[group['ProcessTime_ms'] > upper_bound]
        
        if not group_anomalies.empty:
            anomalies.append(group_anomalies)

    if not anomalies:
        return pd.DataFrame()
    
    # Combine all found anomalies into a single DataFrame and sort by time
    anomalies_df = pd.concat(anomalies).sort_values(by='timestamp')
    return anomalies_df
