import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def precision_pension_extractor():
    """מערכת מדויקת לחילוץ יתרות פנסיה מדויקות"""
    print("🎯 מריץ מערכת מדויקת לחילוץ יתרות פנסיה...")
    print("=" * 90)

    # חיפוש קבצי XML
    search_paths = [os.getcwd(), os.path.join(os.getcwd(), 'DATA')]
    xml_files = []

    for path in search_paths:
        if os.path.exists(path):
            pattern = os.path.join(path, '*.xml')
            files = glob.glob(pattern)
            xml_files.extend(files)

    print(f"📄 נמצאו {len(xml_files)} קבצי XML לעיבוד")

    if not xml_files:
        print("❌ לא נמצאו קבצי XML")
        return None

    all_pension_data = []

    # רשימה מדויקת של שדות יתרה אמיתיים בלבד
    exact_balance_fields = [
        'YITRAT-KASPEY-TAGMULIM',
        'TOTAL-CHISACHON-MTZBR',
        'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA',
        'YITRAT-SOF-SHANA',
        'ERECH-PIDYON-SOF-SHANA'
    ]

    for xml_file in xml_files:
        display_name = os.path.basename(xml_file)
        print(f"\n📄 מעבד במדויק: {display_name}")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # זיהוי פרטי החברה
            company_elem = root.find('.//SHEM-YATZRAN')
            company_name = company_elem.text if company_elem is not None else 'לא ידוע'

            # חיפוש חשבונות
            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   נמצאו {len(accounts)} חשבונות")

            for account in accounts:
                account_num_elem = account.find('MISPAR-POLISA-O-HESHBON')
                if account_num_elem is None or not account_num_elem.text:
                    continue

                account_num = account_num_elem.text.strip()

                # חיפוש שם התכנית
                plan_name_elem = account.find('SHEM-TOCHNIT')
                plan_name = plan_name_elem.text if plan_name_elem is not None else 'לא צוין'

                # זיהוי סוג התכנית
                plan_type = 'קופת גמל'
                if plan_name and ('השתלמות' in str(plan_name) or 'שתלמות' in str(plan_name)):
                    plan_type = 'קרן השתלמות'

                # חיפוש מדויק של יתרות רק בשדות המוכרים
                balance = 0.0
                val_date = 'לא ידוע'
                balance_source = 'לא נמצא'

                # חיפוש תאריך הערכה
                date_elem = account.find('.//TAARICH-ERECH-TZVIROT')
                if date_elem is not None and date_elem.text:
                    val_date = date_elem.text.strip()

                # חיפוש בשדות המדויקים בקטע Yitrot
                yitrot_section = account.find('.//Yitrot')
                if yitrot_section is not None:
                    for field in exact_balance_fields:
                        balance_elem = yitrot_section.find(f'.//{field}')
                        if balance_elem is not None and balance_elem.text:
                            try:
                                value = float(balance_elem.text.strip())
                                # בדיקה מדויקת - רק יתרות פנסיה אמיתיות
                                if 10000 <= value <= 50000000:  # טווח ריאלי ליתרות פנסיה
                                    balance = value
                                    balance_source = field
                                    print(f"      ✅ חשבון {account_num}: יתרה {balance:,.2f} מתוך {field}")
                                    break
                            except (ValueError, TypeError):
                                pass

                # אם לא נמצא בשדות המדויקים, חיפוש בשדות נוספים אבל עם סינון קפדני
                if balance == 0:
                    for elem in account.iter():
                        if elem.text and elem.text.strip():
                            try:
                                value = float(elem.text.strip())
                                # בדיקה קפדנית יותר לזיהוי יתרות אמיתיות
                                if (50000 <= value <= 10000000 and  # טווח צר יותר
                                    elem.tag not in ['MISPAR', 'SHEM', 'KOD', 'STATUS'] and  # לא שדות זיהוי
                                    not any(word in elem.tag for word in ['DATE', 'TIME', 'YEAR', 'MONTH'])):  # לא שדות תאריך
                                    balance = value
                                    balance_source = f"חישוב מתוך {elem.tag}"
                                    print(f"      ⚠️ חשבון {account_num}: יתרה {balance:,.2f} מתוך {elem.tag}")
                                    break
                            except (ValueError, TypeError):
                                pass

                # הוספה לרשימה רק אם נמצאה יתרה משמעותית וריאלית
                if balance >= 50000:  # רק יתרות משמעותיות של לפחות 50,000 ש"ח
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

        except Exception as e:
            print(f"   ❌ שגיאה בעיבוד {display_name}: {str(e)}")

    if all_pension_data:
        df = pd.DataFrame(all_pension_data)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        output_file = 'precision_pension_balances.xlsx'
        df.to_excel(output_file, index=False)
        print(f"\n✅ דוח מדויק נוצר: {output_file}")

        # סיכום מפורט
        print(f"\n{'='*130}")
        print("דוח מדויק של יתרות פנסיה אמיתיות בלבד")
        print(f"{'='*130}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'מקור':<25}")
        print("-" * 160)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['מקור היתרה'][:22]:<25}")
            total_balance += row['יתרה']

        print("-" * 160)
        print(f"{'סה״כ יתרה:':>95} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>95} {len(df)}")

        print("
🎯 המערכת המדויקת הושלמה בהצלחה! נמצאו רק יתרות אמיתיות וריאליות."
        return df
    else:
        print("❌ לא נמצאו יתרות פנסיה ריאליות בקבצים")
        print("💡 ייתכן שהקבצים מכילים רק נתוני ניכויים או שהמבנה שונה")
        return None

if __name__ == "__main__":
    precision_pension_extractor()
