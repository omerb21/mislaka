import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def generic_pension_extractor():
    """מערכת גנרית לחילוץ נתוני פנסיה מכל קבצי המסלקה"""
    print("🚀 מריץ מערכת גנרית לחילוץ נתוני פנסיה...")

    # חיפוש אחר כל קבצי הXML בתיקייה הנוכחית ובתת-תיקיות
    current_dir = os.getcwd()
    xml_files = []

    # חיפוש בקבצים ישירים בתיקייה
    for file in os.listdir(current_dir):
        if file.lower().endswith('.xml'):
            xml_files.append(file)

    # חיפוש בתיקיית DATA אם קיימת
    data_dir = os.path.join(current_dir, 'DATA')
    if os.path.exists(data_dir):
        for file in os.listdir(data_dir):
            if file.lower().endswith('.xml'):
                full_path = os.path.join(data_dir, file)
                xml_files.append(full_path)

    print(f"📄 נמצאו {len(xml_files)} קבצי XML לעיבוד")

    if not xml_files:
        print("❌ לא נמצאו קבצי XML לעיבוד")
        print("💡 הכנס קבצי XML לתיקייה הנוכחית או לתיקיית DATA")
        return None

    all_data = []

    for xml_file in xml_files:
        # אם זה נתיב מלא, השתמש בו. אחרת, הוסף את התיקייה הנוכחית
        if os.path.isabs(xml_file):
            file_path = xml_file
            display_name = os.path.basename(xml_file)
        else:
            file_path = xml_file
            display_name = xml_file

        print(f"\n📄 מעבד: {display_name}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
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

                    # חיפוש בקטע Yitrot
                    yitrot = account.find('.//Yitrot')
                    if yitrot is not None:
                        # תאריך הערכה
                        val_elem = yitrot.find('TAARICH-ERECH-TZVIROT')
                        if val_elem is not None and val_elem.text:
                            val_date = val_elem.text

                        # חיפוש יתרה בכל השדות האפשריים
                        for elem in yitrot.iter():
                            if elem.text and elem.text.strip():
                                try:
                                    value = float(elem.text.strip())
                                    if 1000 < value < 1000000000:  # טווח הגיוני ליתרות פנסיה
                                        balance = value
                                        print(f"      💰 נמצאה יתרה: {balance:,.2f}")
                                        break
                                except:
                                    pass

                    # אם לא נמצאה יתרה, חיפוש בקטעים אחרים
                    if balance == 0:
                        for section_name in ['.//YitraLefiGilPrisha', './/PerutYitrot']:
                            section = account.find(section_name)
                            if section is not None:
                                for elem in section.iter():
                                    if elem.text and elem.text.strip():
                                        try:
                                            value = float(elem.text.strip())
                                            if 1000 < value < 1000000000:
                                                balance = value
                                                print(f"      💰 נמצאה יתרה: {balance:,.2f}")
                                                break
                                        except:
                                            pass
                                    if balance > 0:
                                        break
                                if balance > 0:
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
        df.to_excel('generic_pension_report.xlsx', index=False)
        print("\n✅ דוח נשמר: generic_pension_report.xlsx"
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
    generic_pension_extractor()
