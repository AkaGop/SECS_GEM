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
    Cleans, transforms, and enriches the DataFrame with deep parsing for maintenance analysis.
    """
    # --- Basic Cleaning ---
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    if 'TransactionID' in df.columns:
        df['TransactionID'] = pd.to_numeric(df['TransactionID'], errors='coerce')

    # --- Feature Extraction from 'Message' Column ---
    if 'Message' in df.columns:
        # Performance Metric
        time_pattern = r"Process time of the transaction\(ID=\d+\) is ([\d.-]+) msec"
        df['ProcessTime_ms'] = df['Message'].str.extract(time_pattern, expand=False).astype(float)

        # --- DEEP PARSING FOR MAINTENANCE ---
        # Using regex to find specific patterns within the complex 'Message' string
        df['OperatorID'] = df['Message'].str.extract(r"<A\[\d+\] \"(\d{5})\" > \/\/ OperatorID")
        df['MagazineID'] = df['Message'].str.extract(r"<A\[\d+\] \"(M\d+)\" > \/\/ MagazineID")
        df['LotID'] = df['Message'].str.extract(r"<A\[\d+\] \"(.*?)\" > \/\/ LotID")
        df['PanelID'] = df['Message'].str.extract(r"<A\[\d+\] \"(\d{9})\" > \/\/ PanelID")
        df['SlotID'] = df['Message'].str.extract(r" > \/\/ SlotID\s*4\. <A\[\d+\] \"(\d+)\"") # More complex pattern
        df['PortID'] = df['Message'].str.extract(r"<U1 (\d+) > \/\/ PortID")
        df['SourcePortID'] = df['Message'].str.extract(r"<U1 (\d+) > \/\/ Source PortID")
        df['DestPortID'] = df['Message'].str.extract(r"<U1 (\d+) > \/\/ Dest PortID")
        
        # Track Machine Control State (Local/Remote)
        df['ControlStateChange'] = df['Message'].str.extract(r"(LOCAL|REMOTE)")
        df['ControlState'] = df['ControlStateChange'].ffill().bfill()
        
    # --- Data Propagation ---
    # Forward fill key context data to make it available for all rows
    cols_to_fill = ['OperatorID', 'MagazineID', 'LotID', 'PortID', 'ControlState']
    for col in cols_to_fill:
        if col in df.columns:
            df[col] = df[col].ffill()

    # Propagate MessageName across transaction groups
    if 'TransactionID' in df.columns and 'MessageName' in df.columns:
        df = df.sort_values(by=['TransactionID', 'timestamp'])
        df['MessageName'] = df.groupby('TransactionID')['MessageName'].ffill().bfill()
        
    return df

# --- All other analysis functions (get_summary_statistics, analyze_alarms, etc.) remain the same ---
# They will now automatically work with the new, richer data.
