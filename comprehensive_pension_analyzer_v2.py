import os
import xml.etree.ElementTree as ET
import pandas as pd
import glob

def comprehensive_pension_analyzer():
    print("מנתח מקיף של כל נתוני הפנסיה...")
    print("=" * 80)

    search_paths = [os.getcwd(), os.path.join(os.getcwd(), 'DATA')]
    xml_files = []

    for path in search_paths:
        if os.path.exists(path):
            pattern = os.path.join(path, '*.xml')
            files = glob.glob(pattern)
            xml_files.extend(files)

    print(f"נמצאו {len(xml_files)} קבצי XML לניתוח")

    if not xml_files:
        print("לא נמצאו קבצי XML")
        return None

    all_analysis = []

    all_possible_balance_fields = [
        'YITRAT-KASPEY-TAGMULIM', 'TOTAL-CHISACHON-MTZBR', 'SCHUM-BITUACH-MENAYOT',
        'SCHUM-PITZUIM', 'SCHUM-TAGMULIM', 'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA', 'YITRAT-SOF-SHANA', 'ERECH-PIDYON-SOF-SHANA',
        'TOTAL-SCHUM-MITZVTABER-TZFUY-LEGIL-PRISHA-MECHUSHAV-HAMEYOAD-LEKITZBA-LELO-PREMIYOT',
        'SCHUM-KITZVAT-CHISACHON', 'YITRA-CHISACHON-MITZTABER', 'TOTAL-YITRA-CHISACHON',
        'SCHUM-CHISACHON-MITZBRIM', 'YITRAT-CHISACHON-MITZBRIM', 'TOTAL-CHISACHON-MITZBRIM',
        'ERECH-NUKBACH', 'SCHUM-KITZVOT', 'YITRAT-KITZVOT', 'TOTAL-YITRAT-KITZVOT',
        'SCHUM-KITZVAT-GEMEL', 'SCHUM-KITZVAT-HASHTALMUT', 'YITRAT-GEMEL', 'YITRAT-HASHTALMUT',
        'SCHUM', 'YITRA', 'YITRAT', 'ERECH', 'SCHUM-TOTAL', 'YITRAT-TOTAL', 'ERECH-TOTAL'
    ]

    for xml_file in xml_files:
        display_name = os.path.basename(xml_file)
        print(f"\nמנתח: {display_name}")

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
                if plan_name and ('השתלמות' in str(plan_name) or 'שתלמות' in str(plan_name)):
                    plan_type = 'קרן השתלמות'

                balances_found = {}
                val_date = 'לא ידוע'

                for elem in root.iter():
                    if 'TAARICH' in elem.tag and elem.text:
                        val_date = elem.text.strip()
                        break

                for elem in account.iter():
                    if elem.text and elem.text.strip():
                        try:
                            value = float(elem.text.strip())
                            if 100 < value < 1000000000:
                                field_name = elem.tag
                                if field_name not in balances_found:
                                    balances_found[field_name] = []
                                balances_found[field_name].append(value)
                        except (ValueError, TypeError):
                            pass

                relevant_balances = []
                for field, values in balances_found.items():
                    if any(balance_field in field for balance_field in all_possible_balance_fields):
                        if not any(keyword in field.lower() for keyword in ['nikuy', 'nikuim', 'heshbon', 'management', 'fee']):
                            relevant_balances.extend(values)

                final_balance = 0
                if relevant_balances:
                    final_balance = max(relevant_balances)

                if final_balance > 100:
                    all_analysis.append({
                        'מספר חשבון': account_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name,
                        'חברה מנהלת': company_name,
                        'יתרה': final_balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': display_name,
                        'כל השדות שנמצאו': ', '.join(balances_found.keys())
                    })

                    print(f"חשבון {account_num}: יתרה {final_balance:,.2f} (מתוך {len(relevant_balances)} אפשרויות)")

        except Exception as e:
            print(f"שגיאה בניתוח {display_name}: {str(e)}")

    if all_analysis:
        df = pd.DataFrame(all_analysis)
        df = df.sort_values('יתרה', ascending=False)

        output_file = 'comprehensive_pension_analysis.xlsx'
        df.to_excel(output_file, index=False)
        print(f"דוח מפורט נוצר: {output_file}")

        print("=" * 120)
        print("ניתוח מקיף של כל תכניות הפנסיה")
        print("=" * 120)
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 150)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']

        print("-" * 150)
        print(f"{'סה״כ יתרה:':>85} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>85} {len(df)}")

        print("\nהניתוח המקיף הושלם בהצלחה!")
        return df
    else:
        print("לא נמצאו נתוני פנסיה בקבצים")
        return None

if __name__ == "__main__":
    comprehensive_pension_analyzer()
