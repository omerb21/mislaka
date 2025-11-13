import xml.etree.ElementTree as ET
import os
from collections import defaultdict

def analyze_xml_structure():
    """Analyze the XML structure to identify exact balance fields"""
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml')]
    
    # Known client accounts with their expected balances
    client_accounts = {
        '391117': 157642.02,
        '421789': 46031.88,
        '8557281': 2779.62,
        '56544653': 437662.00,
        '2887123': 1605668.32,
        '6029331': 170639.80,
        '6504775': 380295.12,
        '4337198': 597172.19,
        '1873538': 544863.59,
        '1880186': 585086.53,
        '9675237': 27197.84
    }

    # Track all potential balance fields
    potential_balance_fields = defaultdict(list)
    
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"\n{'='*120}")
        print(f"Analyzing: {xml_file}")
        print(f"{'='*120}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Clean content and parse
            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # Find all accounts in the file
            accounts = root.findall('.//HeshbonOPolisa')
            print(f"\nFound {len(accounts)} accounts in file")
            
            for account in accounts:
                # Get account number
                account_num_elem = account.find('MISPAR-POLISA-O-HESHBON')
                if account_num_elem is None or not account_num_elem.text:
                    continue
                    
                account_num = account_num_elem.text.strip()
                
                # Check if this is one of our target accounts
                is_target_account = account_num in client_accounts
                
                if is_target_account:
                    print(f"\n🔍 Found target account: {account_num} (Expected: {client_accounts[account_num]:,.2f})")
                
                # Get plan name if available
                plan_name = account.find('SHEM-TOCHNIT')
                plan_text = plan_name.text if plan_name is not None and plan_name.text else 'Unknown'
                
                if is_target_account:
                    print(f"  Plan: {plan_text}")
                
                # Look for all numeric fields in this account
                for elem in account.iter():
                    if elem.text and any(c.isdigit() for c in elem.text):
                        try:
                            value = float(elem.text.replace(',', ''))
                            if value > 0:  # Only consider positive values
                                field_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                                
                                # Record this field and value
                                potential_balance_fields[field_name].append((account_num, value, xml_file))
                                
                                # If this is a target account and the value matches the expected balance
                                if is_target_account and abs(value - client_accounts[account_num]) < 0.01:
                                    print(f"  ✅ Exact match: {field_name} = {value:,.2f}")
                                # If this is a target account and the value is close to expected
                                elif is_target_account and 0.9 * client_accounts[account_num] < value < 1.1 * client_accounts[account_num]:
                                    print(f"  🔍 Close match: {field_name} = {value:,.2f} (Expected: {client_accounts[account_num]:,.2f})")
                                
                        except (ValueError, AttributeError):
                            continue
        
        except Exception as e:
            print(f"Error analyzing {xml_file}: {str(e)}")
    
    # Analyze the potential balance fields
    print("\n" + "="*120)
    print("ANALYSIS OF POTENTIAL BALANCE FIELDS")
    print("="*120)
    
    # Sort fields by how many accounts they appear in (most common first)
    sorted_fields = sorted(potential_balance_fields.items(), 
                          key=lambda x: len(set(acc for acc, val, f in x[1])), 
                          reverse=True)
    
    print(f"\nFound {len(sorted_fields)} unique numeric fields across all files")
    
    # Print top 50 most common fields with their values
    print("\nMost common numeric fields:")
    for field, values in sorted_fields[:50]:
        unique_accounts = set(acc for acc, val, f in values)
        unique_files = set(f for acc, val, f in values)
        
        # Check if this field has values matching any of our target accounts
        matching_targets = []
        for acc, expected in client_accounts.items():
            for acc_num, val, f in values:
                if acc_num == acc and abs(val - expected) < 0.01:
                    matching_targets.append(f"{acc} ({val:,.2f})")
                    break
        
        print(f"\n{field}:")
        print(f"  Found in {len(unique_accounts)} accounts across {len(unique_files)} files")
        
        if matching_targets:
            print(f"  ✅ Matches target accounts: {', '.join(matching_targets)}")
        
        # Show some sample values
        sample_values = [val for acc, val, f in values[:5]]
        print(f"  Sample values: {[f'{v:,.2f}' for v in sample_values]}")
        
        # Show range of values
        if values:
            min_val = min(val for acc, val, f in values)
            max_val = max(val for acc, val, f in values)
            print(f"  Value range: {min_val:,.2f} to {max_val:,.2f}")
    
    # Now look for fields that match our target values
    print("\n" + "="*120)
    print("POTENTIAL BALANCE FIELDS MATCHING TARGET ACCOUNTS")
    print("="*120)
    
    found_matches = False
    for field, values in sorted_fields:
        matching_targets = []
        for acc, expected in client_accounts.items():
            for acc_num, val, f in values:
                if acc_num == acc and abs(val - expected) < 0.01:
                    matching_targets.append(acc)
                    break
        
        if matching_targets:
            found_matches = True
            print(f"\n🔍 Field: {field}")
            print(f"   Matches accounts: {', '.join(matching_targets)}")
            
            # Show the values for these accounts
            for acc in matching_targets:
                for acc_num, val, f in values:
                    if acc_num == acc:
                        print(f"   - {acc}: {val:,.2f} (in {f})")
    
    if not found_matches:
        print("\n❌ No fields exactly matched the target account balances.")
        print("Trying to find the closest matches...")
        
        for acc, expected in client_accounts.items():
            print(f"\n🔍 Closest matches for account {acc} (expected: {expected:,.2f}):")
            closest = []
            
            for field, values in sorted_fields:
                for acc_num, val, f in values:
                    if acc_num == acc:
                        diff = abs(val - expected)
                        closest.append((diff, val, field, f))
            
            # Sort by closest match
            closest.sort()
            
            # Show top 5 closest matches
            for diff, val, field, f in closest[:5]:
                print(f"   - {field}: {val:,.2f} (diff: {diff:,.2f}, in {f})")

if __name__ == "__main__":
    analyze_xml_structure()
