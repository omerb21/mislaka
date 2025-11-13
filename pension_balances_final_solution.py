import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import time

def extract_pension_balances_comprehensive():
    """Extract all pension balances from XML files with comprehensive analysis"""
    results = []

    # Get all XML files
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    if not xml_files:
        print("לא נמצאו קבצי XML")
        return

    print(f"נמצאו {len(xml_files)} קבצי XML לעיבוד")

    # Process each file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"\nמעבד: {xml_file}")

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

                # Determine plan type
                plan_type = 'קופת גמל'
                if plan_name is not None and 'השתלמות' in str(plan_name.text):
                    plan_type = 'קרן השתלמות'

                # Initialize values
                val_date = 'לא ידוע'
                balance = 0.0

                # Find Yitrot section and extract balance
                yitrot_section = heshbon.find('.//Yitrot')
                if yitrot_section is not None:
                    # Get valuation date
                    val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if val_elem is not None and val_elem.text:
                        val_date = val_elem.text

                    # Extract balance from YITRAT-KASPEY-TAGMULIM (this is the main balance field)
                    balance_elem = yitrot_section.find('.//YITRAT-KASPEY-TAGMULIM')
                    if balance_elem is not None and balance_elem.text:
                        try:
                            balance = float(balance_elem.text.strip())
                        except (ValueError, TypeError):
                            balance = 0.0

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
            print(f"שגיאה בעיבוד הקובץ {xml_file}: {str(e)}")

    # Create and save Excel report
    if results:
        df = pd.DataFrame(results)

        # Sort by balance (highest first)
        df = df.sort_values('יתרה', ascending=False)

        # Set column order
        columns = ['מספר חשבון', 'סוג תכנית', 'שם התכנית', 'חברה מנהלת', 'יתרה', 'תאריך עדכון', 'קובץ מקור']
        df = df[columns]

        # Create timestamp for filename
        timestamp = int(time.time())
        output_file = f'pension_balances_comprehensive_{timestamp}.xlsx'

        try:
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                # Write main data
                df.to_excel(writer, index=False, sheet_name='סיכום יתרות', startrow=3)

                # Get workbook and worksheet objects
                workbook = writer.book
                worksheet = writer.sheets['סיכום יתרות']

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
                title = "דוח יתרות פנסיה מקיף - פנחס גרינברג (ת.ז. 051683845)"
                title_format = workbook.add_format({
                    'bold': True,
                    'font_size': 16,
                    'align': 'center',
                    'valign': 'vcenter'
                })
                worksheet.merge_range('A1:G1', title, title_format)

                # Adjust column widths
                worksheet.set_column('A:A', 20)  # Account number
                worksheet.set_column('B:B', 15)  # Plan type
                worksheet.set_column('C:C', 25)  # Plan name
                worksheet.set_column('D:D', 25)  # Company
                worksheet.set_column('E:E', 15)  # Balance
                worksheet.set_column('F:F', 15)  # Date
                worksheet.set_column('G:G', 30)  # Source file

                # Add total row
                total_row = len(df) + 4
                total_format = workbook.add_format({
                    'bold': True,
                    'num_format': '#,##0.00',
                    'border': 1,
                    'font_size': 12
                })

                worksheet.write(total_row, 3, 'סה"כ:', total_format)
                worksheet.write_formula(total_row, 4, f'=SUM(E4:E{total_row})', total_format)

                # Add summary info
                summary_start_row = total_row + 2
                worksheet.write(summary_start_row, 0, 'סיכום:')
                worksheet.write(summary_start_row + 1, 0, f'סה"כ תכניות: {len(df)}')
                worksheet.write(summary_start_row + 2, 0, f'סה"כ יתרה: {df["יתרה"].sum():,.2f} ₪')
                worksheet.write(summary_start_row + 3, 0, f'תאריך יצירה: {datetime.now().strftime("%d/%m/%Y %H:%M")}')

                print(f"\n✅ קובץ האקסל נוצר בהצלחה: {os.path.abspath(output_file)}")

        except Exception as e:
            print(f"שגיאה בשמירת הקובץ: {str(e)}")

        # Print summary to console
        print(f"\n{'='*80}")
        print("סיכום יתרות פנסיה מקיף")
        print(f"{'='*80}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 120)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']

        print("-" * 120)
        print(f"{'סה\"כ:':>75} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>75} {len(df)}")

    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    extract_pension_balances_comprehensive()
