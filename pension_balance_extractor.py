import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

# Constants for field names
BALANCE_FIELDS = [
    'ITZULAT-KOL', 'YITZULAT-KOL', 'TOTAL-BALANCE', 'TOTAL_BALANCE', 
    'balanceTotal', 'saldoTotal', 'YITRAT-MASLUL', 'YITRA', 
    'BALANCE', 'SCHUM-YITRA', 'saldo', 'trackBalance'
]

TAGMULIM_FIELDS = [
    'ITZULAT-TAGMULIM', 'TAGMULIM-BALANCE', 'depositsBalance',
    'YITRAT-TAGMULIM', 'TAGMULIM', 'tagmulimBalance'
]

PITZUIM_FIELDS = [
    'ITZULAT-PITZUIM', 'PITZUIM-BALANCE', 'severanceBalance',
    'YITRAT-PITZUIM', 'PITZUIM', 'pitzuimBalance'
]

def normalize_number(value: str) -> float:
    """Normalize number string to float."""
    if not value or not isinstance(value, str):
        return 0.0
    
    # Remove currency symbols, spaces, and thousands separators
    clean = re.sub(r'[₪\s,]', '', value.strip())
    
    # Handle decimal separator (both . and , are common)
    if '.' in clean and ',' in clean:
        # If both separators exist, assume comma is thousand and dot is decimal
        clean = clean.replace(',', '')
    elif ',' in clean:
        # If only comma exists, treat as decimal
        clean = clean.replace(',', '.')
    
    try:
        return max(0.0, float(clean))
    except (ValueError, TypeError):
        return 0.0

def find_amount(element, field_names: List[str]) -> float:
    """Find amount in element using multiple possible field names."""
    for field in field_names:
        # Check direct child
        child = element.find(field)
        if child is not None and child.text:
            return normalize_number(child.text)
        
        # Check in all descendants with case-insensitive matching
        for elem in element.iter():
            if elem.tag.upper() == field.upper() and elem.text:
                return normalize_number(elem.text)
    
    return 0.0

def process_account(account: ET.Element) -> Dict[str, float]:
    """Process a single account/policy element."""
    # Initialize balances
    balances = {
        'total': 0.0,
        'tagmulim': 0.0,
        'pitzuim': 0.0,
        'as_of_date': None
    }
    
    # 1. Try to get total balance from account level
    balances['total'] = find_amount(account, BALANCE_FIELDS)
    
    # 2. Try to get tagmulim and pitzuim from account level
    balances['tagmulim'] = find_amount(account, TAGMULIM_FIELDS)
    balances['pitzuim'] = find_amount(account, PITZUIM_FIELDS)
    
    # 3. Process tracks if they exist
    tracks = account.findall('.//track') + account.findall('.//maslul') + account.findall('.//investmentTrack')
    
    if tracks:
        track_totals = {'tagmulim': 0.0, 'pitzuim': 0.0, 'other': 0.0}
        
        for track in tracks:
            # Get track balance
            track_balance = find_amount(track, BALANCE_FIELDS)
            
            # Determine track type
            track_type = 'other'
            
            # Check if track has type indicator
            type_elem = track.find('trackType') or track.find('sugMaslul')
            if type_elem is not None and type_elem.text:
                track_type_text = type_elem.text.lower()
                if 'tagmul' in track_type_text:
                    track_type = 'tagmulim'
                elif 'pitzui' in track_type_text or 'pitzuim' in track_type_text:
                    track_type = 'pitzuim'
            
            # Add to appropriate total
            track_totals[track_type] += track_balance
            
            # If we couldn't determine type, add to other
            if track_type == 'other':
                track_totals['other'] += track_balance
        
        # If we have track data but no explicit account-level data, use tracks
        if balances['tagmulim'] == 0 and balances['pitzuim'] == 0:
            balances['tagmulim'] = track_totals['tagmulim']
            balances['pitzuim'] = track_totals['pitzuim']
            
            # If we have other tracks, add them to the higher of tagmulim or pitzuim
            if track_totals['other'] > 0:
                if balances['tagmulim'] > balances['pitzuim']:
                    balances['tagmulim'] += track_totals['other']
                else:
                    balances['pitzuim'] += track_totals['other']
    
    # 4. Reconcile balances if needed
    if (balances['tagmulim'] > 0 or balances['pitzuim'] > 0) and \
       abs(balances['total'] - (balances['tagmulim'] + balances['pitzuim'])) > 0.01:
        balances['total'] = balances['tagmulim'] + balances['pitzuim']
    
    # 5. If we still don't have a total, sum the components
    if balances['total'] == 0 and (balances['tagmulim'] > 0 or balances['pitzuim'] > 0):
        balances['total'] = balances['tagmulim'] + balances['pitzuim']
    
    # 6. Get as of date
    date_elem = account.find('asOfDate') or account.find('cutoffDate') or account.find('TAARICH-ERECH-TZVIROT')
    if date_elem is not None and date_elem.text:
        balances['as_of_date'] = date_elem.text
    
    return balances

def process_xml_file(file_path: str) -> List[Dict]:
    """Process a single XML file and extract account balances."""
    try:
        # Read and parse XML file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Clean content
        content = content.replace('\x1a', '')
        root = ET.fromstring(content)
        
        # Get company name
        company = root.find('.//SHEM-YATZRAN') or root.find('.//companyName')
        company_name = company.text if company is not None else "לא ידוע"
        
        # Process all accounts
        accounts = []
        for account in root.findall('.//HeshbonOPolisa') + root.findall('.//account') + root.findall('.//policy'):
            # Get account number
            acc_num = account.find('MISPAR-POLISA-O-HESHBON') or account.find('accountNumber') or account.find('policyNumber')
            acc_num = acc_num.text if acc_num is not None else "לא ידוע"
            
            # Get plan name
            plan = account.find('SHEM-TOCHNIT') or account.find('planName')
            plan_name = plan.text if plan is not None else "לא צוין"
            
            # Determine plan type
            plan_type = "קופת גמל"
            if 'השתלמות' in plan_name:
                plan_type = "קרן השתלמות"
            
            # Process account balances
            balances = process_account(account)
            
            accounts.append({
                'account_number': acc_num,
                'plan_type': plan_type,
                'plan_name': plan_name,
                'company': company_name,
                'total_balance': balances['total'],
                'tagmulim': balances['tagmulim'],
                'pitzuim': balances['pitzuim'],
                'as_of_date': balances['as_of_date']
            })
        
        return accounts
    
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return []

def main():
    # Get all XML files
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) 
                if f.startswith('51683845') and f.endswith('.xml')]
    
    if not xml_files:
        print("לא נמצאו קבצי XML")
        return
    
    all_accounts = []
    
    # Process each file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"Processing file: {xml_file}")
        accounts = process_xml_file(file_path)
        all_accounts.extend(accounts)
    
    # Print results
    if all_accounts:
        print("\nסיכום יתרות פנסיה")
        print("=" * 120)
        print(f"{'חשבון':<15} {'סוג תכנית':<15} {'שם תכנית':<25} {'חברה':<25} {'יתרת צבירה':>15} {'תגמולים':>15} {'פיצויים':>15} {'תאריך עדכון':<12}")
        print("-" * 120)
        
        total_balance = 0
        total_tagmulim = 0
        total_pitzuim = 0
        
        for acc in all_accounts:
            print(f"{acc['account_number'][:14]:<15} {acc['plan_type']:<15} {acc['plan_name'][:22]:<25} {acc['company'][:22]:<25} {acc['total_balance']:>15,.2f} {acc['tagmulim']:>15,.2f} {acc['pitzuim']:>15,.2f} {acc['as_of_date'] or 'לא ידוע':<12}")
            
            total_balance += acc['total_balance']
            total_tagmulim += acc['tagmulim']
            total_pitzuim += acc['pitzuim']
        
        print("-" * 120)
        print(" " * 75 + f"סה""כ: {total_balance:>15,.2f} {total_tagmulim:>15,.2f} {total_pitzuim:>15,.2f}")
    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    main()
