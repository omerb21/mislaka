import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def universal_pension_extractor():
    """מערכת אוניברסלית לחילוץ נתוני פנסיה מכל קבצי הXML"""
    print("🚀 מריץ מערכת אוניברסלית לחילוץ נתוני פנסיה...")
    print("=" * 70)

    # חיפוש אחר כל קבצי הXML בתיקיות השונות
    search_paths = [
        os.getcwd(),  # תיקייה נוכחית
        os.path.join(os.getcwd(), 'DATA'),  # תיקיית DATA
        os.path.dirname(os.getcwd()),  # תיקיית הורה
    ]

    xml_files = []
    for path in search_paths:
        if os.path.exists(path):
            # חיפוש בתיקייה ובתתי תיקיות
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.lower().endswith('.xml'):
                        full_path = os.path.join(root, file)
                        if full_path not in xml_files:  # הימנעות מכפילויות
                            xml_files.append(full_path)

    print(f"📄 נמצאו {len(xml_files)} קבצי XML בסך הכל")
    print("📋 רשימת הקבצים שנמצאו:")

    for i, xml_file in enumerate(xml_files, 1):
        display_name = os.path.basename(xml_file)
        print(f"   {i}. {display_name}")

    if not xml_files:
        print("❌ לא נמצאו קבצי XML לעיבוד")
        print("💡 וודא שקבצי הXML נמצאים באחת מהתיקיות הבאות:")
        for path in search_paths:
            if os.path.exists(path):
                print(f"   • {path}")
        return None

    print(f"\n🔄 מעבד {len(xml_files)} קבצי XML...")
    all_pension_data = []

    for xml_file in xml_files:
        display_name = os.path.basename(xml_file)
        print(f"\n📄 מעבד: {display_name}")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # ניקוי תווים מיוחדים
            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # זיהוי חברת הביטוח
            company_elem = root.find('.//SHEM-YATZRAN')
            company_name = company_elem.text if company_elem is not None else 'לא ידוע'

            # חיפוש חשבונות
            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   נמצאו {len(accounts)} חשבונות")

            for account in accounts:
                # חיפוש מספר חשבון
                account_num_elem = account.find('MISPAR-POLISA-O-HESHBON')
                if account_num_elem is None or not account_num_elem.text:
                    continue

                account_num = account_num_elem.text.strip()

                # חיפוש שם התכנית
                plan_name_elem = account.find('SHEM-TOCHNIT')
                plan_name = plan_name_elem.text if plan_name_elem is not None else 'לא צוין'

                # זיהוי סוג התכנית
                plan_type = 'קופת גמל'
                if plan_name and 'השתלמות' in str(plan_name):
                    plan_type = 'קרן השתלמות'

                # חיפוש יתרה - נסיון למצוא בכל האלמנטים
                balance = 0.0
                val_date = 'לא ידוע'

                # חיפוש תאריך הערכה
                for elem in root.iter():
                    if 'TAARICH' in elem.tag and elem.text:
                        val_date = elem.text.strip()
                        break

                # חיפוש יתרה בכל האלמנטים
                found_balance = False
                for elem in root.iter():
                    if elem.text and elem.text.strip():
                        try:
                            value = float(elem.text.strip())
                            # בדיקה שהערך הוא בטווח הגיוני ליתרת פנסיה
                            if 1000 < value < 1000000000:
                                balance = value
                                found_balance = True
                                print(f"      💰 חשבון {account_num}: יתרה {balance:,.2f}")
                                break
                        except (ValueError, TypeError):
                            pass
                    if found_balance:
                        break

                # הוספה לרשימה רק אם נמצאה יתרה משמעותית
                if balance > 1000:
                    all_pension_data.append({
                        'מספר חשבון': account_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name,
                        'חברה מנהלת': company_name,
                        'יתרה': balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': display_name
                    })

        except Exception as e:
            print(f"   ❌ שגיאה בעיבוד {display_name}: {str(e)}")

    # יצירת דוח אם נמצאו נתונים
    if all_pension_data:
        df = pd.DataFrame(all_pension_data)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        output_file = 'universal_pension_report.xlsx'
        df.to_excel(output_file, index=False)
        print(f"\n✅ דוח האקסל נוצר בהצלחה: {output_file}")

        # סיכום מפורט
        print(f"\n{'='*100}")
        print("דוח אוניברסלי של כל תכניות הפנסיה")
        print(f"{'='*100}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 140)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']

        print("-" * 140)
        print(f"{'סה״כ יתרה:':>75} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>75} {len(df)}")

        print("
🎉 המערכת הושלמה בהצלחה! נמצאו כל התכניות מכל הקבצים."
        return df
    else:
        print("❌ לא נמצאו נתוני פנסיה בקבצים")
        print("💡 ייתכן שהקבצים אינם מכילים נתוני פנסיה או שהמבנה שונה")
        return None

if __name__ == "__main__":
    universal_pension_extractor()
