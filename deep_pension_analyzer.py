import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def deep_pension_analyzer():
    """מערכת ניתוח עמוק של כל סוגי היתרות בקבצי המסלקה"""
    print("🔍 מנתח עמוק של כל נתוני הפנסיה בקבצים...")
    print("=" * 80)

    # חיפוש קבצי XML
    search_paths = [os.getcwd(), os.path.join(os.getcwd(), 'DATA')]
    xml_files = []

    for path in search_paths:
        if os.path.exists(path):
            pattern = os.path.join(path, '*.xml')
            files = glob.glob(pattern)
            xml_files.extend(files)

    print(f"📄 נמצאו {len(xml_files)} קבצי XML לניתוח עמוק")

    if not xml_files:
        print("❌ לא נמצאו קבצי XML")
        return None

    all_analysis_results = []

    # רשימה מקיפה של כל סוגי השדות שיכולים להכיל יתרות פנסיה
    comprehensive_balance_patterns = [
        # שדות יתרה עיקריים
        'YITRAT-KASPEY-TAGMULIM', 'TOTAL-CHISACHON-MTZBR', 'SCHUM-BITUACH-MENAYOT',
        'SCHUM-PITZUIM', 'SCHUM-TAGMULIM', 'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA', 'YITRAT-SOF-SHANA', 'ERECH-PIDYON-SOF-SHANA',
        'TOTAL-SCHUM-MITZVTABER-TZFUY-LEGIL-PRISHA-MECHUSHAV-HAMEYOAD-LEKITZBA-LELO-PREMIYOT',

        # שדות נוספים שיכולים להכיל יתרות
        'SCHUM-KITZVAT-CHISACHON', 'YITRA-CHISACHON-MITZTABER', 'TOTAL-YITRA-CHISACHON',
        'SCHUM-CHISACHON-MITZBRIM', 'YITRAT-CHISACHON-MITZBRIM', 'TOTAL-CHISACHON-MITZBRIM',
        'ERECH-NUKBACH', 'SCHUM-KITZVOT', 'YITRAT-KITZVOT', 'TOTAL-YITRAT-KITZVOT',

        # שדות גמל והשתלמות
        'SCHUM-KITZVAT-GEMEL', 'SCHUM-KITZVAT-HASHTALMUT', 'YITRAT-GEMEL', 'YITRAT-HASHTALMUT',

        # שדות כלליים שיכולים להכיל יתרות
        'SCHUM', 'YITRA', 'YITRAT', 'ERECH', 'SCHUM-TOTAL', 'YITRAT-TOTAL', 'ERECH-TOTAL',

        # שדות עם תווים מיוחדים בעברית
        'יתרת כספי תגמולים', 'סכום חיסכון מצטבר', 'סכום ביטוח מניות',
        'סכום פיצויים', 'סכום תגמולים', 'סכום חיסכון מצטבר',
        'סכום קצבת זיקנה', 'יתרת סוף שנה', 'ערך פדיון סוף שנה'
    ]

    for xml_file in xml_files:
        display_name = os.path.basename(xml_file)
        print(f"\n📄 מנתח עמוק: {display_name}")

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

                # ניתוח עמוק של כל האלמנטים למציאת יתרות
                all_balances_found = {}
                val_date = 'לא ידוע'

                # חיפוש תאריך הערכה
                for elem in root.iter():
                    if 'TAARICH' in elem.tag and elem.text:
                        val_date = elem.text.strip()
                        break

                # חיפוש מקיף בכל האלמנטים של החשבון
                for elem in account.iter():
                    if elem.text and elem.text.strip():
                        try:
                            value = float(elem.text.strip())

                            # בדיקה אם זה ערך שיכול להיות יתרה פנסיה
                            if 1000 < value < 100000000:  # טווח רחב לכל סוגי היתרות
                                field_name = elem.tag

                                # בדיקה אם זה שדה יתרה רלוונטי (לא שדות זיהוי או תאריך)
                                is_relevant_balance = (
                                    any(pattern.lower() in field_name.lower() for pattern in comprehensive_balance_patterns) or
                                    not any(keyword in field_name.lower() for keyword in ['mispar', 'shem', 'kod', 'status', 'date', 'time', 'year', 'month'])
                                )

                                if is_relevant_balance:
                                    if field_name not in all_balances_found:
                                        all_balances_found[field_name] = []
                                    all_balances_found[field_name].append(value)

                        except (ValueError, TypeError):
                            pass

                # ניתוח היתרות שנמצאו ובחירת הטובה ביותר
                final_balance = 0
                best_balance_source = 'לא נמצא'

                if all_balances_found:
                    # סינון יתרות לפי קריטריונים
                    valid_balances = []

                    for field, values in all_balances_found.items():
                        # בחירת הערך הגבוה ביותר מכל שדה
                        max_value = max(values)

                        # סינון לפי סוג השדה והערך
                        if any(pattern.lower() in field.lower() for pattern in ['yitrat', 'schum', 'erech']):
                            # שדות יתרה עיקריים - נותנים עדיפות גבוהה
                            if max_value > 10000:  # רק יתרות משמעותיות
                                valid_balances.append((max_value, field, 'יתרה עיקרית'))
                        elif not any(keyword in field.lower() for keyword in ['fee', 'management', 'heshbon', 'nikuy']):
                            # שדות אחרים - נותנים עדיפות נמוכה יותר
                            if max_value > 50000:  # סף גבוה יותר לשדות משניים
                                valid_balances.append((max_value, field, 'יתרה משנית'))

                    if valid_balances:
                        # בחירת היתרה הטובה ביותר
                        valid_balances.sort(reverse=True, key=lambda x: x[0])
                        final_balance = valid_balances[0][0]
                        best_balance_source = f"{valid_balances[0][1]} ({valid_balances[0][2]})"

                if final_balance > 1000:  # רק יתרות משמעותיות
                    all_analysis_results.append({
                        'מספר חשבון': account_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name,
                        'חברה מנהלת': company_name,
                        'יתרה': final_balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': display_name,
                        'מקור היתרה': best_balance_source,
                        'כל השדות שנמצאו': ', '.join(all_balances_found.keys())
                    })

                    print(f"   ✅ חשבון {account_num}: יתרה {final_balance:,.2f} מתוך {best_balance_source}")

        except Exception as e:
            print(f"   ❌ שגיאה בניתוח {display_name}: {str(e)}")

    if all_analysis_results:
        df = pd.DataFrame(all_analysis_results)
        df = df.sort_values('יתרה', ascending=False)

        # שמירה לאקסל
        output_file = 'deep_pension_analysis.xlsx'
        df.to_excel(output_file, index=False)
        print(f"\n✅ דוח ניתוח עמוק נוצר: {output_file}")

        # סיכום מפורט
        print(f"\n{'='*140}")
        print("ניתוח עמוק של כל תכניות הפנסיה")
        print(f"{'='*140}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'מקור':<30}")
        print("-" * 170)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['מקור היתרה'][:27]:<30}")
            total_balance += row['יתרה']

        print("-" * 170)
        print(f"{'סה״כ יתרה:':>105} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>105} {len(df)}")

        print("
🎯 הניתוח העמוק הושלם בהצלחה! נמצאו כל סוגי היתרות בקבצים."
        return df
    else:
        print("❌ לא נמצאו נתוני פנסיה בקבצים")
        return None

if __name__ == "__main__":
    deep_pension_analyzer()
