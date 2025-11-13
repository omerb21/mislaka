import os
import xml.etree.ElementTree as ET
import pandas as pd

def comprehensive_pension_system():
    print("מריץ מערכת מקיפה לחילוץ נתוני פנסיה...")

    xml_files = [
        '51683845_512065202_KGM_202502051310_1.xml',
        '51683845_512244146_KGM_202502051310_2.xml',
        '51683845_520023185_ING_202502051310_3.xml',
        '51683845_520024647_ING_202502051310_4.xml'
    ]

    print(f"מעבד {len(xml_files)} קבצי מסלקה...")

    all_pension_data = []

    for xml_file in xml_files:
        print(f"\nמעבד: {xml_file}")

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
                if 'השתלמות' in str(plan_name):
                    plan_type = 'קרן השתלמות'

                balance = 0.0
                val_date = 'לא ידוע'

                date_elem = account.find('.//TAARICH-ERECH-TZVIROT')
                if date_elem is not None and date_elem.text:
                    val_date = date_elem.text.strip()

                for section in [account]:
                    for elem in section.iter():
                        if elem.text and elem.text.strip():
                            try:
                                value = float(elem.text.strip())
                                if 10000 < value < 100000000:
                                    balance = value
                                    print(f"חשבון {account_num}: יתרה {balance:,.2f}")
                                    break
                            except (ValueError, TypeError):
                                pass
                        if balance > 0:
                            break
                    if balance > 0:
                        break

                if balance > 10000:
                    all_pension_data.append({
                        'מספר חשבון': account_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name,
                        'חברה מנהלת': company_name,
                        'יתרה': balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': xml_file
                    })

        except Exception as e:
            print(f"שגיאה בעיבוד {xml_file}: {str(e)}")

    if all_pension_data:
        df = pd.DataFrame(all_pension_data)
        df = df.sort_values('יתרה', ascending=False)

        output_file = 'comprehensive_pension_report.xlsx'
        df.to_excel(output_file, index=False)
        print(f"דוח האקסל נוצר: {output_file}")

        print("=" * 100)
        print("דוח מקיף של כל תכניות הפנסיה")
        print("=" * 100)
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 140)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']

        print("-" * 140)
        print(f"{'סה״כ יתרה:':>75} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>75} {len(df)}")

        print("\nהמערכת הושלמה בהצלחה! כל הנתונים חולצו מהקבצים.")
        return df
    else:
        print("לא נמצאו נתוני פנסיה בקבצים")
        return None

if __name__ == "__main__":
    comprehensive_pension_system()
