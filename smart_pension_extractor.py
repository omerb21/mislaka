import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def smart_pension_extractor():
    """מערכת חכמה לחילוץ נתוני פנסיה שמחפשת בכל התיקיות"""
    print("🚀 מריץ מערכת חכמה לחילוץ נתוני פנסיה...")

    # חיפוש אחר כל קבצי הXML בכל התיקיות
    search_paths = [
        os.getcwd(),  # תיקייה נוכחית
        os.path.join(os.getcwd(), 'DATA'),  # תיקיית DATA
        os.path.dirname(os.getcwd()),  # תיקיית הורה
        os.path.join(os.path.dirname(os.getcwd()), '**'),  # כל תתי התיקיות
    ]

    xml_files = []
    for path in search_paths:
        if os.path.exists(path):
            pattern = os.path.join(path, '**', '*.xml') if '**' in str(path) else os.path.join(path, '*.xml')
            files = glob.glob(pattern, recursive=('**' in str(path)))
            xml_files.extend(files)

    # הסרת כפילויות
    xml_files = list(set(xml_files))
    print(f"📄 נמצאו {len(xml_files)} קבצי XML יחודיים לעיבוד")

    if not xml_files:
        print("❌ לא נמצאו קבצי XML לעיבוד")
        print("💡 הכנס קבצי XML לאחת מהתיקיות:")
        for path in search_paths[:-1]:  # בלי ה-**
            if os.path.exists(path):
                print(f"   • {path}")
        return None

    all_data = []

    for xml_file in xml_files[:20]:  # הגבלה ל-20 קבצים ראשונים למניעת עומס
        display_name = os.path.basename(xml_file)
        print(f"\n📄 מעבד: {display_name}")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # זיהוי חברת הביטוח
            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'לא ידוע'

            # חיפוש חשבונות
            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   נמצאו {len(accounts)} חשבונות")

            for account in accounts:
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                plan_name = account.find('SHEM-TOCHNIT')

                if account_num is not None and account_num.text:
                    acc_num = account_num.text.strip()

                    # זיהוי סוג התכנית
                    plan_type = 'קופת גמל'
                    if plan_name is not None and 'השתלמות' in str(plan_name.text):
                        plan_type = 'קרן השתלמות'

                    # חיפוש יתרה
                    balance = 0.0
                    val_date = 'לא ידוע'

                    # חיפוש מקיף בכל האלמנטים
                    for elem in root.iter():
                        if elem.text and elem.text.strip():
                            try:
                                value = float(elem.text.strip())
                                if 1000 < value < 1000000000:  # טווח הגיוני ליתרות פנסיה
                                    balance = value
                                    print(f"      💰 נמצאה יתרה: {balance:,.2f}")
                                    break
                            except:
                                pass

                    # חיפוש תאריך הערכה
                    for elem in root.iter():
                        if elem.tag and 'TAARICH' in elem.tag and elem.text:
                            val_date = elem.text.strip()
                            break

                    if balance > 0:
                        all_data.append({
                            'מספר חשבון': acc_num,
                            'סוג תכנית': plan_type,
                            'שם התכנית': plan_name.text if plan_name is not None else 'לא צוין',
                            'חברה מנהלת': company_name,
                            'יתרה': balance,
                            'תאריך עדכון': val_date,
                            'קובץ מקור': display_name
                        })

        except Exception as e:
            print(f"   ❌ שגיאה בעיבוד הקובץ: {str(e)}")

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        df.to_excel('smart_pension_report.xlsx', index=False)
        print("\n✅ דוח נשמר: smart_pension_report.xlsx"
        # סיכום
        print(f"\n{'='*80}")
        print("סיכום נתוני הפנסיה מכל הקבצים")
        print(f"{'='*80}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 135)

        total = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total += row['יתרה']

        print("-" * 135)
        print(f"{'סה״כ:':>80} {total:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>80} {len(df)}")

        print("\n🎉 המערכת הושלמה בהצלחה!")
        return df
    else:
        print("❌ לא נמצאו נתוני פנסיה בקבצים")
        return None

if __name__ == "__main__":
    smart_pension_extractor()
