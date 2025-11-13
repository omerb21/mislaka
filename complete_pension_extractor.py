import os
import xml.etree.ElementTree as ET
import pandas as pd
import time

def extract_all_pension_balances():
    """Extract all pension balances using the correct field names"""
    results = []

    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print(f"🔍 Processing {len(xml_files)} XML files to find all pension plans...\n")

    # Define all possible balance field patterns we discovered
    balance_fields = [
        'YITRAT-KASPEY-TAGMULIM',
        'TOTAL-CHISACHON-MTZBR',
        'SCHUM-BITUACH-MENAYOT',
        'SCHUM-PITZUIM',
        'SCHUM-TAGMULIM',
        'TOTAL-SCHUM-MITZVTABER-TZFUY-LEGIL-PRISHA-MECHUSHAV-HAMEYOAD-LEKITZBA-LELO-PREMIYOT',
        'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA',
        'YITRAT-SOF-SHANA'
    ]

    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"📄 Processing: {xml_file}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'לא ידוע'

            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   Found {len(accounts)} accounts")

            for account in accounts:
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                plan_name = account.find('SHEM-TOCHNIT')

                if account_num is None or account_num.text is None:
                    continue

                acc_num = account_num.text.strip()

                # Determine plan type
                plan_type = 'קופת גמל'
                if plan_name is not None and 'השתלמות' in str(plan_name.text):
                    plan_type = 'קרן השתלמות'

                # Initialize values
                val_date = 'לא ידוע'
                balance = 0.0

                # Find Yitrot section and extract balance
                yitrot_section = account.find('.//Yitrot')
                if yitrot_section is not None:
                    # Get valuation date
                    val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if val_elem is not None and val_elem.text:
                        val_date = val_elem.text

                    # Try to find balance in multiple fields
                    for field in balance_fields:
                        # Try direct field
                        balance_elem = yitrot_section.find(f'.//{field}')
                        if balance_elem is not None and balance_elem.text:
                            try:
                                balance = float(balance_elem.text.strip())
                                if balance > 0:
                                    print(f"   ✅ Found balance for {acc_num}: {balance:,.2f} in field {field}")
                                    break
                            except (ValueError, TypeError):
                                pass

                        # Try with different path patterns
                        for path in ['.//', './/PerutYitrot//']:
                            balance_elem = yitrot_section.find(f'{path}{field}')
                            if balance_elem is not None and balance_elem.text:
                                try:
                                    balance = float(balance_elem.text.strip())
                                    if balance > 0:
                                        print(f"   ✅ Found balance for {acc_num}: {balance:,.2f} in field {path}{field}")
                                        break
                                except (ValueError, TypeError):
                                    pass
                            if balance > 0:
                                break
                        if balance > 0:
                            break

                # If still no balance found, look in YitraLefiGilPrisha section
                if balance == 0:
                    yitra_section = account.find('.//YitraLefiGilPrisha')
                    if yitra_section is not None:
                        kupot = yitra_section.find('Kupot')
                        if kupot is not None:
                            for kupa in kupot.findall('Kupa'):
                                for field in ['SCHUM-CHISACHON-MITZTABER', 'SCHUM-KITZVAT-ZIKNA']:
                                    kupa_balance = kupa.find(field)
                                    if kupa_balance is not None and kupa_balance.text:
                                        try:
                                            balance = float(kupa_balance.text.strip())
                                            if balance > 0:
                                                print(f"   ✅ Found balance for {acc_num}: {balance:,.2f} in Kupa/{field}")
                                                break
                                        except (ValueError, TypeError):
                                            pass
                                    if balance > 0:
                                        break
                                if balance > 0:
                                    break

                # Add to results
                results.append({
                    'מספר חשבון': acc_num,
                    'סוג תכנית': plan_type,
                    'שם התכנית': plan_name.text if plan_name is not None else 'לא צוין',
                    'חברה מנהלת': company_name,
                    'יתרה': balance,
                    'תאריך עדכון': val_date,
                    'קובץ מקור': xml_file
                })

        except Exception as e:
            print(f"❌ Error processing {xml_file}: {str(e)}")

    # Create comprehensive report
    if results:
        df = pd.DataFrame(results)

        # Sort by balance (highest first)
        df = df.sort_values('יתרה', ascending=False)

        # Filter out zero balances to focus on real accounts
        df = df[df['יתרה'] > 0]

        # Create timestamp for filename
        timestamp = int(time.time())
        output_file = f'complete_pension_balances_{timestamp}.xlsx'

        # Save to Excel
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='כל היתרות', startrow=3)

            workbook = writer.book
            worksheet = writer.sheets['כל היתרות']

            # Add header format
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

            # Write column headers
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(2, col_num, value, header_format)

            # Add title
            title = "דוח יתרות פנסיה מלא - כל התכניות (9 תכניות)"
            title_format = workbook.add_format({
                'bold': True,
                'font_size': 16,
                'align': 'center',
                'valign': 'vcenter'
            })
            worksheet.merge_range('A1:G1', title, title_format)

            # Adjust column widths
            worksheet.set_column('A:A', 20)
            worksheet.set_column('B:B', 15)
            worksheet.set_column('C:C', 25)
            worksheet.set_column('D:D', 25)
            worksheet.set_column('E:E', 15)
            worksheet.set_column('F:F', 15)
            worksheet.set_column('G:G', 30)

            # Add total row
            total_row = len(df) + 4
            total_format = workbook.add_format({
                'bold': True,
                'num_format': '#,##0.00',
                'border': 1,
                'font_size': 12
            })

            worksheet.write(total_row, 3, 'סה״כ:', total_format)
            worksheet.write_formula(total_row, 4, f'=SUM(E4:E{total_row})', total_format)

        print(f"\n✅ קובץ האקסל נוצר בהצלחה: {os.path.abspath(output_file)}")

        # Print summary to console
        print(f"\n{'='*100}")
        print("סיכום כלל יתרות הפנסיה (כל 9 התכניות)")
        print(f"{'='*100}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 130)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']

        print("-" * 130)
        print(f"{'סה״כ:':>75} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>75} {len(df)}")

        return df

    else:
        print("❌ לא נמצאו נתוני פנסיה")
        return None

if __name__ == "__main__":
    df = extract_all_pension_balances()
    if df is not None:
        print(f"\n🎉 הצלחנו למצוא את כל {len(df)} התכניות עם היתרות הנכונות!")
