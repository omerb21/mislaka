import os
import re
import xml.etree.ElementTree as ET

def extract_balance(element, tag_names):
    """Extract balance from element using multiple possible tag names."""
    for tag in tag_names:
        # Try direct child
        child = element.find(tag)
        if child is not None and child.text:
            return clean_number(child.text)
        
        # Try case-insensitive search in all descendants
        for elem in element.iter():
            if elem.tag.upper() == tag.upper() and elem.text:
                return clean_number(elem.text)
    return 0.0

def clean_number(value):
    """Convert string number to float, handling various formats."""
    if not value:
        return 0.0
    
    # Remove currency symbols, spaces, and thousands separators
    clean = re.sub(r'[₪\s,]', '', str(value).strip())
    
    try:
        return float(clean)
    except (ValueError, TypeError):
        return 0.0

def process_xml_file(file_path):
    """Process a single XML file and extract account balances."""
    try:
        # Read and clean XML content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        content = content.replace('\x1a', '')
        root = ET.fromstring(content)
        
        # Get company name
        company = root.find('.//SHEM-YATZRAN')
        company_name = company.text if company is not None else "לא ידוע"
        
        results = []
        
        # Process each account
        for account in root.findall('.//HeshbonOPolisa'):
            # Get account number
            acc_num = account.find('MISPAR-POLISA-O-HESHBON')
            acc_num = acc_num.text if acc_num is not None else "לא ידוע"
            
            # Get plan name
            plan = account.find('SHEM-TOCHNIT')
            plan_name = plan.text if plan is not None else "לא צוין"
            
            # Determine plan type
            plan_type = "קופת גמל"
            if 'השתלמות' in plan_name:
                plan_type = "קרן השתלמות"
            
            # Extract balances
            total_balance = extract_balance(account, ['ITZULAT-KOL', 'YITZULAT-KOL', 'TOTAL-BALANCE'])
            tagmulim = extract_balance(account, ['ITZULAT-TAGMULIM', 'TAGMULIM-BALANCE'])
            pitzuim = extract_balance(account, ['ITZULAT-PITZUIM', 'PITZUIM-BALANCE'])
            
            # If no total balance, try to sum components
            if total_balance == 0 and (tagmulim > 0 or pitzuim > 0):
                total_balance = tagmulim + pitzuim
            
            # If still no balance, look for any balance field
            if total_balance == 0:
                total_balance = extract_balance(account, ['YITRA', 'BALANCE', 'SCHUM-YITRA'])
            
            # Get as of date if available
            date_elem = account.find('TAARICH-ERECH-TZVIROT')
            as_of_date = date_elem.text if date_elem is not None and date_elem.text else "לא ידוע"
            
            results.append({
                'account': acc_num,
                'type': plan_type,
                'name': plan_name,
                'company': company_name,
                'total': total_balance,
                'tagmulim': tagmulim,
                'pitzuim': pitzuim,
                'as_of_date': as_of_date
            })
        
        return results
    
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
        print(f"Processing: {xml_file}")
        accounts = process_xml_file(file_path)
        all_accounts.extend(accounts)
    
    # Print results
    if all_accounts:
        print("\nסיכום יתרות פנסיה")
        print("=" * 110)
        print("""חשבון           סוג        תכנית               חברה                                 סה""כ         תגמולים         פיצויים      תאריך""")
        print("-" * 110)
        
        total = 0
        tagmulim_total = 0
        pitzuim_total = 0
        
        for acc in all_accounts:
            print(f"{acc['account'][:14]:<15} {acc['type']:<10} {acc['name'][:18]:<20} {acc['company'][:18]:<20} {acc['total']:>15,.2f} {acc['tagmulim']:>15,.2f} {acc['pitzuim']:>15,.2f} {acc['as_of_date']:>10}")
            
            total += acc['total']
            tagmulim_total += acc['tagmulim']
            pitzuim_total += acc['pitzuim']
        
        print("-" * 110)
        print(" " * 60 + f"סה""כ: {total:>15,.2f} {tagmulim_total:>15,.2f} {pitzuim_total:>15,.2f}")
    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    main()
