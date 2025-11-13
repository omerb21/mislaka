import os
import xml.etree.ElementTree as ET
import pandas as pd
import time

def extract_pension_balances():
    """Extract pension balances from XML files"""
    results = []

    # Get all XML files
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print(f"Processing {len(xml_files)} XML files...")

    # Process each file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

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

                # Determine plan type
                plan_type = 'Pension Fund'
                if plan_name is not None and 'השתלמות' in str(plan_name.text):
                    plan_type = 'Training Fund'

                # Initialize values
                val_date = 'Unknown'
                balance = 0.0

                # Find balance in Yitrot section
                yitrot_section = heshbon.find('.//Yitrot')
                if yitrot_section is not None:
                    # Get valuation date
                    val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if val_elem is not None and val_elem.text:
                        val_date = val_elem.text

                    # Extract balance from YITRAT-KASPEY-TAGMULIM
                    balance_elem = yitrot_section.find('.//YITRAT-KASPEY-TAGMULIM')
                    if balance_elem is not None and balance_elem.text:
                        try:
                            balance = float(balance_elem.text.strip())
                        except (ValueError, TypeError):
                            balance = 0.0

                # Add to results
                results.append({
                    'Account': acc_num,
                    'Plan Type': plan_type,
                    'Plan Name': plan_name.text if plan_name is not None else 'Not specified',
                    'Company': company_name,
                    'Balance': balance,
                    'Valuation Date': val_date,
                    'Source File': xml_file
                })

        except Exception as e:
            print(f"Error processing {xml_file}: {str(e)}")

    # Create DataFrame and save to Excel
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values('Balance', ascending=False)

        # Create timestamp for filename
        timestamp = int(time.time())
        output_file = f'pension_balances_final_{timestamp}.xlsx'

        # Save to Excel
        df.to_excel(output_file, index=False)
        print(f"\nExcel file created: {os.path.abspath(output_file)}")

        # Print summary to console
        print(f"\n{'='*80}")
        print("PENSION BALANCES SUMMARY")
        print(f"{'='*80}")
        print(f"{'Account':<20} {'Type':<15} {'Plan Name':<25} {'Company':<25} {'Balance':>15} {'Date':<15}")
        print("-" * 120)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['Account']:<20} {row['Plan Type']:<15} {str(row['Plan Name'])[:22]:<25} {row['Company'][:22]:<25} {row['Balance']:>15,.2f} {row['Valuation Date']:<15}")
            total_balance += row['Balance']

        print("-" * 120)
        print(f"{'TOTAL:':>75} {total_balance:>15,.2f} NIS")
        print(f"{'Number of Plans:':>75} {len(df)}")

        return df

    else:
        print("No pension data found")
        return None

if __name__ == "__main__":
    extract_pension_balances()
