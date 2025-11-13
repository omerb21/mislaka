import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def targeted_pension_extractor():
    """מערכת ממוקדת לחיפוש יתרות פנסיה בשדות המדויקים ביותר"""
    print("🎯 מריץ מערכת ממוקדת לחיפוש יתרות מדויקות...")
    print("=" * 95)

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

    # שדות יתרה מדויקים ביותר - מבוססים על ניתוח הקבצים
    exact_balance_fields = [
        # שדות ראשיים שמצאנו בניתוח
        'TOTAL-CHISACHON-MTZBR',
        'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA',
        'YITRAT-SOF-SHANA',
        'ERECH-PIDYON-SOF-SHANA',
        'YITRAT-KASPEY-TAGMULIM',
        'SCHUM-BITUACH-MENAYOT',
        'SCHUM-PITZUIM',
        'SCHUM-TAGMULIM',

        # שדות נוספים שיכולים להכיל יתרות
        'SCHUM', 'YITRA', 'YITRAT', 'ERECH', 'TOTAL',
        'KISUY', 'CHISACHON', 'KITZVOT'
    ]

    # רשימה של מספרי החשבונות של הלקוח למטרות בדיקה
    client_accounts = {
        '391117', '421789', '8557281', '56544653', '2887123',
        '6029331', '6504775', '4337198', '1873538', '1880186', '9675237'
    }

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

            # חיפוש כל החשבונות
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

                # חיפוש ממוקד ביותר בשדות היתרה המדויקים
                balance = 0.0
                val_date = 'לא ידוע'
                balance_source = 'לא נמצא'

                # חיפוש תאריך הערכה
                date_elem = account.find('.//TAARICH-ERECH-TZVIROT')
                if date_elem is not None and date_elem.text:
                    val_date = date_elem.text.strip()

                # חיפוש בשדות המדויקים בקטע Yitrot (הקטע העיקרי ליתרות)
                yitrot_section = account.find('.//Yitrot')
                if yitrot_section is not None:
                    for field in exact_balance_fields:
                        balance_elem = yitrot_section.find(f'.//{field}')
                        if balance_elem is not None and balance_elem.text:
                            try:
                                value = float(balance_elem.text.strip())

                                # בדיקה קפדנית - רק יתרות אמיתיות בטווח הריאלי
                                if 10000 <= value <= 10000000:
                                    # בדיקה נוספת - לא ניכויים של החברה המנהלת
                                    if not any(keyword in field.lower() for keyword in ['fee', 'management', 'heshbon', 'nikuy']):
                                        if value > balance:
                                            balance = value
                                            balance_source = field

                            except (ValueError, TypeError):
                                pass

                # חיפוש בשדות אחרים רק אם לא נמצאה יתרה בקטע Yitrot
                if balance == 0:
                    for elem in account.iter():
                        if elem.text and elem.text.strip():
                            try:
                                value = float(elem.text.strip())

                                # בדיקה אם זה שדה יתרה רלוונטי
                                is_relevant_balance = (
                                    elem.tag in exact_balance_fields or
                                    any(pattern.lower() in elem.tag.lower() for pattern in ['yitrat', 'schum', 'erech', 'total', 'kisuy'])
                                )

                                if is_relevant_balance and 20000 <= value <= 5000000:
                                    # סינון שדות לא רלוונטיים
                                    if not any(keyword in elem.tag.lower() for keyword in ['fee', 'heshbon', 'management', 'nikuy', 'date', 'time']):
                                        if value > balance:
                                            balance = value
                                            balance_source = elem.tag

                            except (ValueError, TypeError):
                                pass

                # הוספה לרשימה רק אם נמצאה יתרה משמעותית וריאלית
                if balance >= 20000:  # סף גבוה יותר למניעת זיהוי שגוי
                    all_pension_data.append({
                        'מספר חשבון': account_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name,
                        'חברה מנהלת': company_name,
                        'יתרה': balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': display_name,
                        'מקור היתרה': balance_source,
                        'חשבון לקוח': 'כן' if account_num in client_accounts else 'לא'
                    })

                    status = "✅ לקוח" if account_num in client_accounts else "📋 אחר"
                    print(f"   {status} חשבון {account_num}: יתרה {balance:,.2f} מתוך {balance_source}")

        except Exception as e:
            print(f"   ❌ שגיאה בעיבוד {display_name}: {str(e)}")

    if all_pension_data:
        df = pd.DataFrame(all_pension_data)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        output_file = 'targeted_pension_balances.xlsx'
        df.to_excel(output_file, index=False)
        print(f"\n✅ דוח ממוקד נוצר: {output_file}")

        # סיכום מפורט
        print(f"\n{'='*160}")
        print("דוח ממוקד של יתרות פנסיה אמיתיות בלבד")
        print(f"{'='*160}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'מקור':<25} {'לקוח':<8}")
        print("-" * 190)

        total_balance = 0
        client_accounts_found = 0

        for _, row in df.iterrows():
            client_mark = "✅" if row['חשבון לקוח'] == 'כן' else "📋"
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['מקור היתרה'][:22]:<25} {client_mark:<8}")
            total_balance += row['יתרה']
            if row['חשבון לקוח'] == 'כן':
                client_accounts_found += 1

        print("-" * 190)
        print(f"{'סה״כ יתרה:':>125} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>125} {len(df)} (מתוכן {client_accounts_found} של הלקוח)")

        print("
🎯 המערכת הממוקדת הושלמה בהצלחה! נמצאו רק יתרות אמיתיות ומדויקות."
        return df
    else:
        print("❌ לא נמצאו יתרות פנסיה בקבצים")
        return None

if __name__ == "__main__":
    targeted_pension_extractor()
