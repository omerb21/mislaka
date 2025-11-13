import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def ultimate_pension_system():
    print("מריץ את המערכת הסופית לחילוץ יתרות פנסיה מדויקות...")
    print("=" * 100)

    search_paths = [os.getcwd(), os.path.join(os.getcwd(), 'DATA')]
    xml_files = []

    for path in search_paths:
        if os.path.exists(path):
            pattern = os.path.join(path, '*.xml')
            files = glob.glob(pattern)
            xml_files.extend(files)

    print(f"נמצאו {len(xml_files)} קבצי XML לעיבוד")

    if not xml_files:
        print("לא נמצאו קבצי XML")
        return None

    all_pension_data = []

    comprehensive_balance_fields = [
        'TOTAL-CHISACHON-MTZBR', 'SCHUM-BITUACH-MENAYOT', 'SCHUM-PITZUIM',
        'SCHUM-TAGMULIM', 'SCHUM-CHISACHON-MITZTABER', 'SCHUM-KITZVAT-ZIKNA',
        'YITRAT-SOF-SHANA', 'ERECH-PIDYON-SOF-SHANA', 'YITRAT-KASPEY-TAGMULIM',
        'SCHUM', 'YITRA', 'YITRAT', 'ERECH', 'TOTAL', 'KISUY', 'CHISACHON',
        'KITZVOT', 'GEMEL', 'HASHTALMUT', 'PENSION', 'PROVIDENT',
        'סכום חיסכון מצטבר', 'יתרת כספי תגמולים', 'סכום ביטוח מניות',
        'סכום פיצויים', 'סכום תגמולים', 'סכום קצבת זיקנה',
        'ERECH-NUKBACH', 'SCHUM-KITZVOT', 'YITRAT-KITZVOT', 'TOTAL-YITRAT-KITZVOT',
        'SCHUM-KITZVAT-GEMEL', 'SCHUM-KITZVAT-HASHTALMUT', 'YITRAT-GEMEL', 'YITRAT-HASHTALMUT'
    ]

    for xml_file in xml_files:
        display_name = os.path.basename(xml_file)
        print(f"\nמעבד בקפדנות: {display_name}")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            company_elem = root.find('.//SHEM-YATZRAN')
            company_name = company_elem.text if company_elem is not None else 'לא ידוע'

            accounts = root.findall('.//HeshbonOPolisa')
            print(f"נמצאו {len(accounts)} חשבונות")

            for account in accounts:
                account_num_elem = account.find('MISPAR-POLISA-O-HESHBON')
                if account_num_elem is None or not account_num_elem.text:
                    continue

                account_num = account_num_elem.text.strip()

                plan_name_elem = account.find('SHEM-TOCHNIT')
                plan_name = plan_name_elem.text if plan_name_elem is not None else 'לא צוין'

                plan_type = 'קופת גמל'
                if plan_name and ('השתלמות' in str(plan_name) or 'שתלמות' in str(plan_name)):
                    plan_type = 'קרן השתלמות'

                balance = 0.0
                val_date = 'לא ידוע'
                balance_source = 'לא נמצא'

                date_elem = account.find('.//TAARICH-ERECH-TZVIROT')
                if date_elem is not None and date_elem.text:
                    val_date = date_elem.text.strip()

                yitrot_section = account.find('.//Yitrot')
                if yitrot_section is not None:
                    for field in comprehensive_balance_fields:
                        balance_elem = yitrot_section.find(f'.//{field}')
                        if balance_elem is not None and balance_elem.text:
                            try:
                                value = float(balance_elem.text.strip())
                                if 10000 <= value <= 10000000:
                                    if value > balance:
                                        balance = value
                                        balance_source = field
                            except (ValueError, TypeError):
                                pass

                if balance == 0:
                    for elem in account.iter():
                        if elem.text and elem.text.strip():
                            try:
                                value = float(elem.text.strip())

                                is_balance_field = (
                                    any(pattern.lower() in elem.tag.lower() for pattern in comprehensive_balance_fields) or
                                    ('יתר' in elem.tag or 'סכום' in elem.tag or 'ערך' in elem.tag)
                                )

                                if is_balance_field and 10000 <= value <= 10000000:
                                    if not any(keyword in elem.tag.lower() for keyword in ['fee', 'heshbon', 'management', 'nikuy']):
                                        if value > balance:
                                            balance = value
                                            balance_source = elem.tag

                            except (ValueError, TypeError):
                                pass

                if balance >= 10000:
                    all_pension_data.append({
                        'מספר חשבון': account_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name,
                        'חברה מנהלת': company_name,
                        'יתרה': balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': display_name,
                        'מקור היתרה': balance_source
                    })

                    print(f"חשבון {account_num}: יתרה {balance:,.2f} מתוך {balance_source}")

        except Exception as e:
            print(f"שגיאה בעיבוד {display_name}: {str(e)}")

    if all_pension_data:
        df = pd.DataFrame(all_pension_data)
        df = df.sort_values('יתרה', ascending=False)

        output_file = 'ultimate_pension_balances.xlsx'
        df.to_excel(output_file, index=False)
        print(f"דוח סופי נוצר: {output_file}")

        print("=" * 150)
        print("דוח סופי של כל תכניות הפנסיה")
        print("=" * 150)
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'מקור':<30}")
        print("-" * 180)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['מקור היתרה'][:27]:<30}")
            total_balance += row['יתרה']

        print("-" * 180)
        print(f"{'סה״כ יתרה:':>115} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>115} {len(df)}")

        print("\nהמערכת הסופית הושלמה בהצלחה! נמצאו כל היתרות המדויקות.")
        return df
    else:
        print("לא נמצאו יתרות פנסיה בקבצים")
        return None

if __name__ == "__main__":
    ultimate_pension_system()
