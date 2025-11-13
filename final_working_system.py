import os
import xml.etree.ElementTree as ET
import pandas as pd

def ultra_simple_extractor():
    print("מריץ חילוץ פנסיה אולטרה-פשוט...")

    xml_files = [
        '51683845_512065202_KGM_202502051310_1.xml',
        '51683845_512244146_KGM_202502051310_2.xml',
        '51683845_520023185_ING_202502051310_3.xml',
        '51683845_520024647_ING_202502051310_4.xml'
    ]

    all_data = []

    for xml_file in xml_files:
        print(f"מעבד: {xml_file}")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            accounts = root.findall('.//HeshbonOPolisa')
            print(f"נמצאו {len(accounts)} חשבונות")

            for account in accounts:
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                if account_num is not None and account_num.text:
                    acc_num = account_num.text.strip()

                    yitrot = account.find('.//Yitrot')
                    if yitrot is not None:
                        for elem in yitrot.iter():
                            if elem.text and elem.text.strip():
                                try:
                                    value = float(elem.text.strip())
                                    if value > 1000:
                                        all_data.append({
                                            'מספר חשבון': acc_num,
                                            'יתרה': value,
                                            'קובץ': xml_file
                                        })
                                        print(f"נמצאה יתרה: {value:,.2f}")
                                        break
                                except:
                                    pass

        except Exception as e:
            print(f"שגיאה: {str(e)}")

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values('יתרה', ascending=False)

        df.to_excel('final_pension_report.xlsx', index=False)
        print("דוח נשמר: final_pension_report.xlsx"

        print("=" * 60)
        print("סיכום סופי")
        print("=" * 60)
        print(f"{'מספר חשבון':<20} {'יתרה':>15}")
        print("-" * 40)

        total = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['יתרה']:>15,.2f}")
            total += row['יתרה']

        print("-" * 40)
        print(f"{'סה״כ:':>25} {total:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>25} {len(df)}")

        print("המערכת הושלמה בהצלחה!")
        return df
    else:
        print("לא נמצאו נתוני פנסיה")
        return None

if __name__ == "__main__":
    ultra_simple_extractor()
