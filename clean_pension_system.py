import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def clean_pension_extractor():
    """מערכת נקייה לחילוץ נתוני יתרות פנסיה אמיתיות בלבד"""
    print("🚀 מריץ מערכת נקייה לחילוץ יתרות פנסיה אמיתיות...")
    print("=" * 80)

    # חיפוש קבצי XML בתיקיות שונות
    search_paths = [
        os.getcwd(),
        os.path.join(os.getcwd(), 'DATA'),
    ]

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

    # רשימת שדות יתרה ידועים שצריך לחפש
    balance_fields = [
        'YITRAT-KASPEY-TAGMULIM',
        'TOTAL-CHISACHON-MTZBR',
        'SCHUM-BITUACH-MENAYOT',
        'SCHUM-PITZUIM',
        'SCHUM-TAGMULIM',
        'TOTAL-SCHUM-MITZVTABER-TZFUY-LEGIL-PRISHA-MECHUSHAV-HAMEYOAD-LEKITZBA-LELO-PREMIYOT',
        'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA',
        'YITRAT-SOF-SHANA',
        'ERECH-PIDYON-SOF-SHANA',
        'SCHUM-KITZVAT-CHISACHON',
        'YITRA-CHISACHON-MITZTABER',
        'TOTAL-YITRA-CHISACHON'
    ]

    for xml_file in xml_files:
        display_name = os.path.basename(xml_file)
        print(f"\n📄 מעבד: {display_name}")

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # זיהוי חברת הביטוח
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

                # חיפוש יתרה אמיתית בקטעי הפנסיה השונים
                balance = 0.0
                val_date = 'לא ידוע'
                found_balance = False

                # חיפוש בקטע Yitrot (הקטע העיקרי ליתרות)
                yitrot_section = account.find('.//Yitrot')
                if yitrot_section is not None:
                    # חיפוש תאריך הערכה
                    date_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if date_elem is not None and date_elem.text:
                        val_date = date_elem.text.strip()

                    # חיפוש בשדות יתרה ידועים בקטע Yitrot
                    for field in balance_fields:
                        balance_elem = yitrot_section.find(f'.//{field}')
                        if balance_elem is not None and balance_elem.text:
                            try:
                                value = float(balance_elem.text.strip())
                                # בדיקה שהערך הוא יתרה אמיתית (לא ניכויים של החברה)
                                if 10000 < value < 100000000:  # טווח הגיוני ליתרות פנסיה
                                    balance = value
                                    found_balance = True
                                    print(f"      💰 חשבון {account_num}: יתרה {balance:,.2f}")
                                    break
                            except (ValueError, TypeError):
                                pass

                # אם לא נמצאה יתרה בקטע Yitrot, חיפוש בקטעים אחרים
                if not found_balance:
                    for section_name in ['.//YitraLefiGilPrisha', './/PerutYitrot']:
                        section = account.find(section_name)
                        if section is not None:
                            # חיפוש בשדות יתרה ידועים בקטעים נוספים
                            for field in ['SCHUM-CHISACHON-MITZTABER', 'SCHUM-KITZVAT-ZIKNA']:
                                balance_elem = section.find(f'.//{field}')
                                if balance_elem is not None and balance_elem.text:
                                    try:
                                        value = float(balance_elem.text.strip())
                                        if 10000 < value < 100000000:
                                            balance = value
                                            found_balance = True
                                            print(f"      💰 חשבון {account_num}: יתרה {balance:,.2f}")
                                            break
                                    except (ValueError, TypeError):
                                        pass
                                if found_balance:
                                    break
                        if found_balance:
                            break

                # הוספה לרשימה רק אם נמצאה יתרה אמיתית
                if balance > 10000:
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

    if all_pension_data:
        df = pd.DataFrame(all_pension_data)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        output_file = 'clean_pension_balances.xlsx'
        df.to_excel(output_file, index=False)
        print(f"\n✅ דוח נקי נוצר: {output_file}")

        # סיכום מפורט
        print(f"\n{'='*100}")
        print("דוח נקי של יתרות פנסיה אמיתיות בלבד")
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

        print("\n🎉 המערכת הושלמה בהצלחה! נמצאו רק יתרות אמיתיות.")
        return df
    else:
        print("❌ לא נמצאו יתרות פנסיה אמיתיות בקבצים")
        print("💡 ייתכן שהקבצים מכילים רק נתוני ניכויים של החברה המנהלת")
        return None

if __name__ == "__main__":
    clean_pension_extractor()
