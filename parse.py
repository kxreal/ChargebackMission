from pathlib import Path
from collections import defaultdict
import pandas as pd
import statistics
import csv
import json

# Initial setup
root_dir = Path('.')
payment_processors = ['Adyen', 'Ingenico', 'Stripe']
file_types = {
    'Adyen': '.csv',
    'Ingenico': '.json',
    'Stripe': '.txt'
}
# Global variables
master_data = []
normalized_data = []

# Diagnostics
def analyze_file_sizes(root_dir):
    sizes = []
    processor_sizes = defaultdict(list)

    for processor in payment_processors:
        folder = root_dir / processor
        if folder.exists():
            for path in folder.rglob('*'):
                if path.is_file():
                    size_kb = path.stat().st_size / 1024
                    sizes.append(size_kb)
                    processor_sizes[processor].append(size_kb)
    
    print("FILE SIZE DIAGNOSTICS")
    print(f"Total files: {len(sizes)}")

    # Overall stats
    print(f"\nOverall:")
    print(f"  Avg size: {statistics.mean(sizes):.1f} KB")
    print(f"  Median: {statistics.median(sizes):.1f} KB")
    print(f"  < 1KB: {sum(1 for s in sizes if s < 1)} ({sum(1 for s in sizes if s < 1)/len(sizes)*100:.1f}%)")
    print(f"  1-10KB: {sum(1 for s in sizes if 1 <= s < 10)}")
    print(f"  > 10KB: {sum(1 for s in sizes if s >= 10)}")
    
    # By processor
    print(f"\nBy processor:")
    for proc, proc_sizes in processor_sizes.items():
        print(f"  {proc}: {len(proc_sizes)} files, "
              f"avg {statistics.mean(proc_sizes):.1f}KB, "
              f"<1KB: {sum(1 for s in proc_sizes if s < 1)}")

# Sub function to check file types
def check_file_type(folder_path, file_type):
    all_files = list(folder_path.rglob('*'))
    expected_files = [f for f in all_files if f.is_file() and f.suffix == file_type]
    unexpected_files = [f for f in all_files if f.is_file() and f.suffix != file_type]
    #print(f"{folder_path.name}: {len(expected_files)} {file_type} files")

    if len(unexpected_files) > 0:
        print(f"Found {len(unexpected_files)} unexpected files")
        for file in unexpected_files[:5]:
            print(f"    - {file.name}")
        if len(unexpected_files) > 5:
            print(f"    ... and {len(unexpected_files) - 5} more")
    return expected_files

# Check delimiter for txt file
def detect_delimiter(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        first_line = f.readline()
        try:
            return csv.Sniffer().sniff(first_line).delimiter
        except csv.Error:
            if "|" in first_line:
                return "|"
            return "\t"

# Function to add metadata to each row
def add_metadata(row_dict, path, processor):
    row_dict['source_file'] = str(path)
    row_dict['source_filename'] = path.name
    row_dict['processor'] = processor
    row_dict['service_type'] = path.parent.name
    return row_dict

# Function to extract data from csv & txt
def process_delimited_file(path, processor, delimiter):
    with open(path, 'r', encoding='utf-8-sig', newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader,[])
        headers = [h.strip() for h in headers if h and h.strip()]
        for row in reader:
            if row:
                row_dict = dict(zip(headers, row))
                row_dict = add_metadata(row_dict, path, processor)
                master_data.append(row_dict)

# Normalisation function helper
def get_first_value(row, *columns):
    for col in columns:
        val = row.get(col)
        if val is not None and str(val).strip() != '':
            return val
    return None

# Normalisation function date format
def parse_record_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    date_str = str(date_str).strip()
    formats = [
        '%Y-%m-%dT%H:%M:%S',   # YYYY-MM-DDTHH:MM:SS
        '%Y-%m-%d %H:%M:%S',   # YYYY-MM-DD HH:MM:SS
        '%m/%d/%Y %H:%M:%S',   # MM/DD/YYYY HH:MM:SS
        '%d-%m-%Y %H:%M:%S',   # DD-MM-YYYY HH:MM:SS
    ]

    if 'T' in date_str:
        date_str = date_str.rstrip('Z')
        candidates = ['%Y-%m-%dT%H:%M:%S']
    elif '/' in date_str:
        candidates = ['%m/%d/%Y %H:%M:%S']
    elif date_str[:4].isdigit() and int(date_str[:4]) >= 2000:
        candidates = ['%Y-%m-%d %H:%M:%S']
    else:
        candidates = ['%d-%m-%Y %H:%M:%S']
    for fmt in candidates:
        try:
            return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            continue
    return None

# Look up the daily exchange rate for a currency on a given date.
def get_exchange_rate(currency, date_str):
    try:
        if not currency or not date_str:
            return None
        rate = exchange_rates.get(str(date_str), {}).get(str(currency).strip().upper())
        return float(rate) if rate else None
    except (ValueError, TypeError):
        return None

# Convert an amount from a given currency to USD using the daily exchange rate.
def convert_to_usd(amount, currency, date_str):
    try:
        if not amount or not currency or not date_str:
            return None
        rate = get_exchange_rate(currency, date_str)
        if rate and rate != 0:
            return round(float(amount) / rate, 2)
    except (ValueError, TypeError):
        pass
    return None

def normalize_row(row):
    return {
        # Metadata
        'source_file': row.get('source_file'),
        'source_filename': row.get('source_filename'),
        'processor': row.get('processor'),
        'service_type': row.get('service_type'),

        # General Info
        'company_account': row.get('Company Account'),
        'merchant_account': row.get('Merchant Account'),
        'psp_reference': row.get('Psp Reference'),
        'payment_method': row.get('Payment Method'),
        
        # Dispute Info
        'record_date': parse_record_date(get_first_value(row, 'Transaction Date', 'Record Date', 'Date')),
        'dispute_date': row.get('Dispute Date'),
        'dispute_date_timezone': row.get('Dispute Date TimeZone'),
        'dispute_end_date': row.get('Dispute End Date'),
        'dispute_end_date_timezone': row.get('Dispute End Date TimeZone'),
        'dispute_amount': get_first_value(row, 'Dispute Amount', 'Chargeback Value', 'Amount'),
        'dispute_currency': get_first_value(row, 'Curr','Dispute Currency', 'CurrencyCode'),
        'dispute_amount_usd': convert_to_usd(
            get_first_value(row, 'Dispute Amount', 'Chargeback Value', 'Amount'),
            get_first_value(row, 'Curr', 'Dispute Currency', 'CurrencyCode'),
            parse_record_date(get_first_value(row, 'Transaction Date', 'Record Date', 'Date'))
        ),
        'fx_rate_used': get_exchange_rate(
            get_first_value(row, 'Curr', 'Dispute Currency', 'CurrencyCode'),
            parse_record_date(get_first_value(row, 'Transaction Date', 'Record Date', 'Date'))
        ),
        'scheme_code': get_first_value(row, 'CB Scheme Code', 'NoF Scheme Code', 'RFI Scheme Code'),
        'reason_code': get_first_value(row, 'CB Reason Code', 'NoF Reason Code', 'RFI Reason Code'),
        'record_type': row.get('Record Type'),
        'dispute_psp_reference': row.get('Dispute PSP Reference'),
        'dispute_reason': row.get('Dispute Reason'),
        'risk_scoring': row.get('Risk Scoring'),
        
        # Transation Info
        'payment_date': row.get('Payment Date'),
        'payment_date_timezone': row.get('Payment Date TimeZone'),
        'payment_amount': row.get('Payment Amount'),
        'shopper_interaction': row.get('Shopper Interaction'),
        'shopper_country': row.get('Shopper Country'),

        # Card Info
        'iban': row.get('Iban'),
        'bic': row.get('Bic'),
        'issuer_country': row.get('Issuer Country'),
        'issuer_id': row.get('Issuer Id'),
        '3d_directory_response': row.get('3D Directory Response'),
        '3d_authentication_response': row.get('3D Authentication Response'),
        'cvc2_response': row.get('CVC2 Response'),
        'avs_response': row.get('AVS Response'),
        'dispute_auto_defended': row.get('Dispute Auto Defended')
    }

# Main - Validate file types and extract the expected type files
with open('exchange_rates.json', 'r', encoding='utf-8-sig') as f:
    exchange_rates = json.load(f)

for processor in payment_processors:
    expected_files = check_file_type(root_dir / processor, file_types[processor])
    
    for path in expected_files:
        try:
            if processor == 'Adyen':
                process_delimited_file(path, processor, ',')

            elif processor == 'Ingenico':
                with open(path, 'r', encoding='utf-8-sig', newline="") as f:
                    data = json.load(f)
                    if data and len(data) > 0:
                        headers = list(data[0].keys())
                        for row in data:
                            row_dict = dict(row)
                            row_dict = add_metadata(row_dict, path, processor)
                            master_data.append(row_dict)

            elif processor == 'Stripe':
                delimiter = detect_delimiter(path)
                process_delimited_file(path, processor, delimiter)

        except Exception as e:
            print(f"Error reading {path}: {e}")

for row_dict in master_data:
    normalized_row = normalize_row(row_dict)
    normalized_data.append(normalized_row)
normalized_df = pd.DataFrame(normalized_data)
normalized_df.to_csv('normalized_data.csv', index=False)


# DEBUGS
'''
# EXTRACTED RESULT INSPECTION
print(f"Total rows extracted: {len(master_data)}")
print("First row sample:")
print(master_data[0])  # Shows structure + keys
print("\nFirst 3 rows:")
for i, row in enumerate(master_data[:3]):
    print(f"Row {i}: {len(row)} keys, Chargeback Value: {row.get('Chargeback Value')}")
'''
# WORKING CODE
'''
# json reader test
for path in json_dir.rglob("*.json"):
    try:
        with open(path) as f:
            data = json.load(f)
            if data and len(data) > 0:
                headers = list(data[0].keys())
                all_headers.update(headers)
    except Exception as e:
        print(f"Error reading {path}: {e}")
# csv reader test
for path in csv_dir.rglob("*.csv"):
    try:
        with open(path, 'r', encoding='utf-8-sig', newline="") as f:
            reader = csv.reader(f)
            headers = next(reader,[])
            headers = [h.strip() for h in headers if h and h.strip()]
            all_headers.update(headers)
    except Exception as e:
        print(f"Error reading {path}: {e}")
# txt reader test
for path in txt_dir.rglob("*.txt"):
    try:
        delimiter = detect_delimiter(path)
        with open(path, 'r', encoding='utf-8-sig', newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            headers = next(reader,[])
            headers = [h.strip() for h in headers if h and h.strip()]
            all_headers.update(headers)
    except Exception as e:
        print(f"Error reading {path}: {e}")
'''