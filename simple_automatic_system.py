import os
import xml.etree.ElementTree as ET
import pandas as pd
import time

def simple_pension_extractor():
    """גרסה פשוטה של חילוץ נתוני פנסיה"""
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print(f"🔍 מעבד {len(xml_files)} קבצי XML...")

    all_data = []

    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"\n📄 {xml_file}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'לא ידוע'

            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   נמצאו {len(accounts)} חשבונות")

            for account in accounts:
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                plan_name = account.find('SHEM-TOCHNIT')

                if account_num is not None and account_num.text:
                    acc_num = account_num.text.strip()

                    plan_type = 'קופת גמל'
                    if plan_name is not None and 'השתלמות' in str(plan_name.text):
                        plan_type = 'קרן השתלמות'

                    balance = 0.0
                    val_date = 'לא ידוע'

                    # חיפוש יתרה
                    yitrot_section = account.find('.//Yitrot')
                    if yitrot_section is not None:
                        val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                        if val_elem is not None and val_elem.text:
                            val_date = val_elem.text

                        # חיפוש בכל השדות האפשריים
                        for elem in yitrot_section.iter():
                            if elem.text and elem.text.strip():
                                try:
                                    value = float(elem.text.strip())
                                    if value > 1000:  # רק ערכים משמעותיים
                                        balance = value
                                        print(f"      💰 נמצאה יתרה: {balance:,.2f}")
                                        break
                                except:
                                    pass

                    if balance > 0:
                        all_data.append({
                            'מספר חשבון': acc_num,
                            'סוג תכנית': plan_type,
                            'שם התכנית': plan_name.text if plan_name is not None else 'לא צוין',
                            'חברה מנהלת': company_name,
                            'יתרה': balance,
                            'תאריך עדכון': val_date,
                            'קובץ מקור': xml_file
                        })

        except Exception as e:
            print(f"   ❌ שגיאה: {str(e)}")

    # יצירת דוח
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        timestamp = int(time.time())
        output_file = f'pension_report_{timestamp}.xlsx'

        df.to_excel(output_file, index=False)
        print(f"\n✅ דוח נשמר: {output_file}")

        # סיכום
        print(f"\n{'='*80}")
        print("סיכום נתוני פנסיה")
        print(f"{'='*80}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15}")
        print("-" * 120)

        total = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f}")
            total += row['יתרה']

        print("-" * 120)
        print(f"{'סה״כ:':>75} {total:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>75} {len(df)}")

        return df

    else:
        print("❌ לא נמצאו נתוני פנסיה")
        return None

if __name__ == "__main__":
    simple_pension_extractor()
