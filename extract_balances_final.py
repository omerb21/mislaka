import os
import xml.etree.ElementTree as ET
import pandas as pd

def extract_balances():
    results = []
    
    # Get all XML files in the current directory
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) 
                if f.endswith('.xml') and f.startswith('51683845')]
    
    if not xml_files:
        print("No XML files found")
        return
    
    # Process each file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"Processing: {xml_file}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Clean content and parse
            content = content.replace('\x1a', '')
            root = ET.fromstring(content)
            
            # Get company name
            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'Unknown'
            
            # Process each account
            for heshbon in root.findall('.//HeshbonOPolisa'):
                account = heshbon.find('MISPAR-POLISA-O-HESHBON')
                plan_name = heshbon.find('SHEM-TOCHNIT')
                
                if account is None or account.text is None:
                    continue
                    
                acc_num = account.text.strip()
                
                # Get plan type
                plan_type = 'Pension Fund'
                if plan_name is not None and 'השתלמות' in str(plan_name.text):
                    plan_type = 'Training Fund'
                
                # Initialize values
                val_date = 'Unknown'
                balance = 0.0
                
                # Find Yitrot section
                yitrot_section = heshbon.find('.//Yitrot')
                if yitrot_section is not None:
                    # Get valuation date
                    val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if val_elem is not None and val_elem.text:
                        val_date = val_elem.text
                    
                    # Find balance in YITRAT-KASPEY-TAGMULIM
                    balance_elem = yitrot_section.find('.//YITRAT-KASPEY-TAGMULIM')
                    if balance_elem is not None and balance_elem.text:
                        try:
                            balance = float(balance_elem.text)
                        except (ValueError, TypeError):
                            pass
                
                # Add to results
                results.append({
                    'Account': acc_num,
                    'Plan Type': plan_type,
                    'Plan Name': plan_name.text if plan_name is not None else 'Not specified',
                    'Company': company_name,
                    'Balance': balance,
                    'Valuation Date': val_date
                })
        
        except Exception as e:
            print(f"Error processing file {xml_file}: {str(e)}")
    
    # Create and save Excel report
    if results:
        df = pd.DataFrame(results)
        
        # Set column order
        columns = ['Account', 'Plan Type', 'Plan Name', 'Company', 'Balance', 'Valuation Date']
        df = df[columns]
        
        # Save to Excel with timestamp
        import time
        timestamp = int(time.time())
        output_file = f'pension_balances_{timestamp}.xlsx'
        
        try:
            df.to_excel(output_file, index=False, sheet_name='Summary')
            print(f"\nExcel file created successfully: {os.path.abspath(output_file)}")
            
            # Print summary to console
            print("\nPension Balances Summary")
            print("=" * 100)
            print(f"{'Account':<20} {'Plan Type':<15} {'Plan Name':<25} {'Company':<25} {'Balance':>15} {'Valuation Date':<15}")
            print("-" * 100)
            
            total_balance = 0
            for _, row in df.iterrows():
                print(f"{row['Account']:<20} {row['Plan Type']:<15} {str(row['Plan Name'])[:22]:<25} {row['Company'][:22]:<25} {row['Balance']:>15,.2f} {row['Valuation Date']:<15}")
                total_balance += row['Balance']
            
            print("-" * 100)
            total_str = f"Total: {total_balance:>15,.2f} ₪"
            print(" " * (100 - len(total_str)) + total_str)
            
        except Exception as e:
            print(f"\nError saving file: {str(e)}")
    else:
        print("No pension data found")

if __name__ == "__main__":
    extract_balances()
