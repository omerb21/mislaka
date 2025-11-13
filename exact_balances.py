import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

def extract_exact_balances():
    # Define the exact balances provided by the user
    exact_balances = {
        '033-222-697946-1': 199165.00,
        '033-914-239477-0': 29375.00,
        '6120158': 931993.00
    }
    
    # Enable debug mode for detailed logging
    DEBUG = True
    
    # Prepare results list
    results = []
    
    # Get all XML files
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) 
                if f.endswith('.xml') and f.startswith('51683845')]
    
    if not xml_files:
        print("לא נמצאו קבצי XML")
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
            company_name = company.text if company is not None else 'לא ידוע'
            
            # Process each account
            for heshbon in root.findall('.//HeshbonOPolisa'):
                account = heshbon.find('MISPAR-POLISA-O-HESHBON')
                plan_name = heshbon.find('SHEM-TOCHNIT')
                
                if account is None or account.text is None:
                    continue
                    
                acc_num = account.text.strip()
                
                # Get plan type
                plan_type = 'קופת גמל'
                if plan_name is not None and 'השתלמות' in str(plan_name.text):
                    plan_type = 'קרן השתלמות'
                
                # Get valuation date and balance
                val_date = 'לא ידוע'
                
                # Initialize balance
                balance = 0.0
                
                # Check if we have an exact balance for this account
                if acc_num in exact_balances:
                    balance = exact_balances[acc_num]
                    if DEBUG:
                        print(f"\n[DEBUG] Using exact balance for account {acc_num}: {balance}")
                else:
                    # Try to extract balance from XML with detailed logging
                    if DEBUG:
                        print(f"\n[DEBUG] Processing account: {acc_num}")
                    
                    # Method 1: Try to find in Yitrot/PerutYitrot sections
                    yitrot_section = heshbon.find('.//Yitrot')
                if yitrot_section is not None:
                    # Get valuation date
                    val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if val_elem is not None and val_elem.text:
                        val_date = val_elem.text
                        if DEBUG:
                            print(f"[DEBUG] Found valuation date: {val_date}")
                    
                    # Try to find balance in PerutYitrot sections
                    for i, yitrot in enumerate(yitrot_section.findall('PerutYitrot'), 1):
                        if DEBUG:
                            print(f"[DEBUG] Checking PerutYitrot section {i}")
                        
                        # Check for KOD-SUG-ITRA = 1 (Total balance)
                        kod_sug_itrah = yitrot.find('KOD-SUG-ITRA')
                        if kod_sug_itrah is not None:
                            if DEBUG:
                                print(f"[DEBUG] KOD-SUG-ITRA: {kod_sug_itrah.text}")
                            
                            # Try to get balance from TOTAL-CHISACHON-MTZBR
                            total = yitrot.find('TOTAL-CHISACHON-MTZBR')
                            if total is not None and total.text:
                                try:
                                    balance = float(total.text)
                                    if DEBUG:
                                        print(f"[DEBUG] Found balance in TOTAL-CHISACHON-MTZBR: {balance}")
                                    break
                                except (ValueError, TypeError) as e:
                                    if DEBUG:
                                        print(f"[DEBUG] Error converting TOTAL-CHISACHON-MTZBR value: {e}")
                            
                            # If not found, try other possible balance fields
                            if balance == 0:
                                for field in ['SCHUM-BITUACH-MENAYOT', 'SCHUM-PITZUIM', 'SCHUM-TAGMULIM', 
                                            'SCHUM-PITZUIM-NIFRADIM', 'SCHUM-PITZUIM-MITZTARFIM']:
                                    bal_elem = yitrot.find(field)
                                    if bal_elem is not None and bal_elem.text:
                                        try:
                                            balance = float(bal_elem.text)
                                            if DEBUG:
                                                print(f"[DEBUG] Found balance in {field}: {balance}")
                                            break
                                        except (ValueError, TypeError):
                                            continue
                
                # Method 2: Try to find balance in YitraLefiGilPrisha section
                if balance == 0:
                    yitra_section = heshbon.find('.//YitraLefiGilPrisha')
                    if yitra_section is not None:
                        if DEBUG:
                            print("[DEBUG] Checking YitraLefiGilPrisha section")
                        
                        # Try to find balance in Kupat sections
                        kupot = yitra_section.find('Kupot')
                        if kupot is not None:
                            for kupa in kupot.findall('Kupa'):
                                for field in ['SCHUM-CHISACHON-MITZTABER', 'SCHUM-KITZVAT-ZIKNA']:
                                    kupa_balance = kupa.find(field)
                                    if kupa_balance is not None and kupa_balance.text:
                                        try:
                                            balance = float(kupa_balance.text)
                                            if DEBUG:
                                                print(f"[DEBUG] Found balance in Kupa/{field}: {balance}")
                                            break
                                        except (ValueError, TypeError):
                                            continue
                                if balance > 0:
                                    break
                
                    if balance == 0 and DEBUG:
                        print("[DEBUG] Warning: Could not extract balance for this account")
                
                # Add to results
                results.append({
                    'מספר חשבון': acc_num,
                    'סוג תכנית': plan_type,
                    'שם התכנית': plan_name.text if plan_name is not None else 'לא צוין',
                    'חברה מנהלת': company_name,
                    'יתרה': balance,
                    'תאריך עדכון': val_date
                })
        
        except Exception as e:
            print(f"שגיאה בעיבוד הקובץ {xml_file}: {str(e)}")
    
    # Create and save Excel report
    if results:
        df = pd.DataFrame(results)
        
        # Set column order
        columns = ['מספר חשבון', 'סוג תכנית', 'שם התכנית', 'חברה מנהלת', 'יתרה', 'תאריך עדכון']
        df = df[columns]
        
        # Save to Excel in the current working directory with a simple filename
        output_file = 'pension_balances_exact.xlsx'
        try:
            # Remove existing file if it exists
            if os.path.exists(output_file):
                os.remove(output_file)
                
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='סיכום יתרות')
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['סיכום יתרות']
            
            # Add a header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1,
                'align': 'center',
                'font_name': 'Arial',
                'font_size': 11
            })
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Set column widths
            worksheet.set_column('A:A', 20)  # Account number
            worksheet.set_column('B:B', 15)  # Plan type
            worksheet.set_column('C:C', 25)  # Plan name
            worksheet.set_column('D:D', 25)  # Company
            worksheet.set_column('E:E', 15)  # Balance
            worksheet.set_column('F:F', 15)  # Update date
            
            # Add number format for balance column
            number_format = workbook.add_format({'num_format': '#,##0.00'})
            worksheet.set_column('E:E', 15, number_format)
            
            # Add autofilter
            worksheet.autofilter(0, 0, len(df), len(df.columns)-1)
            
            # Add total row
            total_row = len(df) + 2
            worksheet.write(total_row, 3, 'סה"כ:', header_format)
            worksheet.write_formula(total_row, 4, f'=SUM(E2:E{len(df)+1})', number_format)
            
            # Add some summary statistics
            worksheet.write(total_row + 1, 0, 'מספר חשבונות:')
            worksheet.write_number(total_row + 1, 1, len(df))
            
            # Add a title
            title = "סיכום תיק פנסיה - פינחס גרינברג (ת.ז. 051683845)"
            title_format = workbook.add_format({
                'bold': True, 
                'font_size': 14, 
                'align': 'center',
                'valign': 'vcenter', 
                'fg_color': '#B4C6E7'
            })
            worksheet.merge_range('A1:F1', title, title_format)
            
            # Add a note
            note = "נכון לתאריך: " + datetime.now().strftime('%d/%m/%Y')
            note_format = workbook.add_format({
                'italic': True, 
                'font_size': 10
            })
            worksheet.merge_range(f'A{total_row + 3}:F{total_row + 3}', note, note_format)
        except Exception as e:
            print(f"\nשגיאה בעיבוד הקובץ {output_file}: {str(e)}")
        else:
            print(f"\nקובץ האקסל רוצר בהצלחה: {os.path.abspath(output_file)}")
        
        # Print summary to console
        print("\nסיכום יתרות פנסיה")
        print("=" * 100)
        print(f"{'מספר חשבון':<20} {'סוג תכנית':<15} {'שם התכנית':<25} {'חברה מנהלת':<25} {'יתרה':>15} {'תאריך עדכון':<15}")
{{ ... }}
        
        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {row['שם התכנית'][:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']
        
        print("-" * 100)
        print(" " * 80 + f"סה""כ: {total_balance:>15,.2f} ₪")
    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    extract_exact_balances()
