import os
import xml.etree.ElementTree as ET
import pandas as pd
import time
import glob
from datetime import datetime

class PensionDataExtractor:
    """מערכת אוטומטית לחילוץ נתוני פנסיה מקבצי XML"""

    def __init__(self, data_directory=None):
        """אתחול המערכת עם תיקיית הנתונים"""
        if data_directory is None:
            # ברירת מחדל - תיקיית DATA בתיקייה הנוכחית
            self.data_dir = os.path.join(os.getcwd(), 'DATA')
        else:
            self.data_dir = data_directory

        self.balance_fields = [
            'YITRAT-KASPEY-TAGMULIM',
            'TOTAL-CHISACHON-MTZBR',
            'SCHUM-BITUACH-MENAYOT',
            'SCHUM-PITZUIM',
            'SCHUM-TAGMULIM',
            'TOTAL-SCHUM-MITZVTABER-TZFUY-LEGIL-PRISHA-MECHUSHAV-HAMEYOAD-LEKITZBA-LELO-PREMIYOT',
            'SCHUM-CHISACHON-MITZTABER',
            'SCHUM-KITZVAT-ZIKNA',
            'YITRAT-SOF-SHANA',
            'ERECH-PIDYON-SOF-SHANA'
        ]

        print("🚀 Pension Data Extractor Initialized")
        print(f"📁 Data Directory: {self.data_dir}")

    def find_xml_files(self):
        """מציאת כל קבצי הXML בתיקיית הנתונים"""
        if not os.path.exists(self.data_dir):
            print(f"❌ תיקיית הנתונים לא קיימת: {self.data_dir}")
            return []

        # חיפוש אחר כל קבצי הXML בתיקייה ובתת-תיקיות
        xml_files = []
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                if file.lower().endswith('.xml'):
                    xml_files.append(os.path.join(root, file))

        print(f"📄 נמצאו {len(xml_files)} קבצי XML")
        return xml_files

    def extract_pension_data_from_file(self, xml_file):
        """חילוץ נתוני פנסיה מקובץ XML בודד"""
        results = []

        try:
            with open(xml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # ניקוי תווים מיוחדים
            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # זיהוי חברת הביטוח
            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'לא ידוע'

            # חיפוש אחר כל החשבונות
            accounts = root.findall('.//HeshbonOPolisa')

            for account in accounts:
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                plan_name = account.find('SHEM-TOCHNIT')

                if account_num is None or account_num.text is None:
                    continue

                acc_num = account_num.text.strip()

                # זיהוי סוג התכנית
                plan_type = 'קופת גמל'
                if plan_name is not None and 'השתלמות' in str(plan_name.text):
                    plan_type = 'קרן השתלמות'

                # אתחול ערכים
                val_date = 'לא ידוע'
                balance = 0.0

                # חיפוש בקטע Yitrot
                yitrot_section = account.find('.//Yitrot')
                if yitrot_section is not None:
                    # תאריך הערכה
                    val_elem = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    if val_elem is not None and val_elem.text:
                        val_date = val_elem.text

                    # חיפוש יתרה בכל השדות האפשריים
                    for field in self.balance_fields:
                        balance_elem = yitrot_section.find(f'.//{field}')
                        if balance_elem is not None and balance_elem.text:
                            try:
                                balance = float(balance_elem.text.strip())
                                if balance > 0:
                                    break
                            except (ValueError, TypeError):
                                pass

                # אם לא נמצאה יתרה, חיפוש בקטע YitraLefiGilPrisha
                if balance == 0:
                    yitra_section = account.find('.//YitraLefiGilPrisha')
                    if yitra_section is not None:
                        kupot = yitra_section.find('Kupot')
                        if kupot is not None:
                            for kupa in kupot.findall('Kupa'):
                                for field in ['SCHUM-CHISACHON-MITZTABER', 'SCHUM-KITZVAT-ZIKNA']:
                                    kupa_balance = kupa.find(field)
                                    if kupa_balance is not None and kupa_balance.text:
                                        try:
                                            balance = float(kupa_balance.text.strip())
                                            if balance > 0:
                                                break
                                        except (ValueError, TypeError):
                                            pass
                                    if balance > 0:
                                        break
                                if balance > 0:
                                    break

                # הוספה לרשימה רק אם יש יתרה
                if balance > 0:
                    results.append({
                        'מספר חשבון': acc_num,
                        'סוג תכנית': plan_type,
                        'שם התכנית': plan_name.text if plan_name is not None else 'לא צוין',
                        'חברה מנהלת': company_name,
                        'יתרה': balance,
                        'תאריך עדכון': val_date,
                        'קובץ מקור': os.path.basename(xml_file)
                    })

        except Exception as e:
            print(f"❌ שגיאה בעיבוד {xml_file}: {str(e)}")

        return results

    def process_all_files(self):
        """עיבוד כל הקבצים ויצירת הדוח"""
        xml_files = self.find_xml_files()

        if not xml_files:
            print("❌ לא נמצאו קבצי XML לעיבוד")
            return None

        all_results = []

        for xml_file in xml_files:
            print(f"\n📄 מעבד: {os.path.basename(xml_file)}")
            file_results = self.extract_pension_data_from_file(xml_file)
            all_results.extend(file_results)
            print(f"   ✅ נמצאו {len(file_results)} תכניות פנסיה")

        if not all_results:
            print("❌ לא נמצאו נתוני פנסיה בקבצים")
            return None

        # יצירת DataFrame
        df = pd.DataFrame(all_results)

        # מיון לפי יתרה (מהגבוה לנמוך)
        df = df.sort_values('יתרה', ascending=False)

        # יצירת שם קובץ עם חותמת זמן
        timestamp = int(time.time())
        output_file = f'pension_data_report_{timestamp}.xlsx'

        # שמירה לאקסל עם עיצוב
        self.create_excel_report(df, output_file)

        # הדפסת סיכום למסך
        self.print_summary(df)

        return df

    def create_excel_report(self, df, filename):
        """יצירת דוח אקסל מעוצב"""
        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                # כתיבת הנתונים
                df.to_excel(writer, index=False, sheet_name='דוח פנסיה', startrow=3)

                workbook = writer.book
                worksheet = writer.sheets['דוח פנסיה']

                # עיצוב כותרות
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1,
                    'align': 'center',
                    'font_name': 'Arial',
                    'font_size': 11
                })

                # כתיבת כותרות העמודות
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(2, col_num, value, header_format)

                # כותרת ראשית
                title = "דוח נתוני פנסיה - חילוץ אוטומטי מקבצי מסלקה"
                title_format = workbook.add_format({
                    'bold': True,
                    'font_size': 16,
                    'align': 'center',
                    'valign': 'vcenter'
                })
                worksheet.merge_range('A1:G1', title, title_format)

                # התאמת רוחב עמודות
                worksheet.set_column('A:A', 20)  # מספר חשבון
                worksheet.set_column('B:B', 15)  # סוג תכנית
                worksheet.set_column('C:C', 25)  # שם התכנית
                worksheet.set_column('D:D', 25)  # חברה מנהלת
                worksheet.set_column('E:E', 15)  # יתרה
                worksheet.set_column('F:F', 15)  # תאריך עדכון
                worksheet.set_column('G:G', 30)  # קובץ מקור

                # שורת סה"כ
                total_row = len(df) + 4
                total_format = workbook.add_format({
                    'bold': True,
                    'num_format': '#,##0.00',
                    'border': 1,
                    'font_size': 12
                })

                worksheet.write(total_row, 3, 'סה״כ:', total_format)
                worksheet.write_formula(total_row, 4, f'=SUM(E4:E{total_row})', total_format)

                # מידע נוסף
                info_row = total_row + 2
                worksheet.write(info_row, 0, 'מידע נוסף:')
                worksheet.write(info_row + 1, 0, f'סה״כ תכניות: {len(df)}')
                worksheet.write(info_row + 2, 0, f'סה״כ יתרה: {df["יתרה"].sum():,.2f} ₪')
                worksheet.write(info_row + 3, 0, f'תאריך יצירה: {datetime.now().strftime("%d/%m/%Y %H:%M")}')

            print(f"✅ דוח האקסל נוצר בהצלחה: {os.path.abspath(filename)}")

        except Exception as e:
            print(f"❌ שגיאה ביצירת דוח האקסל: {str(e)}")

    def print_summary(self, df):
        """הדפסת סיכום למסך"""
        print(f"\n{'='*100}")
        print("סיכום דוח נתוני הפנסיה")
        print(f"{'='*100}")
        print(f"{'מספר חשבון':<20} {'סוג':<15} {'תכנית':<25} {'חברה':<25} {'יתרה':>15} {'תאריך':<15}")
        print("-" * 130)

        total_balance = 0
        for _, row in df.iterrows():
            print(f"{row['מספר חשבון']:<20} {row['סוג תכנית']:<15} {str(row['שם התכנית'])[:22]:<25} {row['חברה מנהלת'][:22]:<25} {row['יתרה']:>15,.2f} {row['תאריך עדכון']:<15}")
            total_balance += row['יתרה']

        print("-" * 130)
        print(f"{'סה״כ:':>75} {total_balance:>15,.2f} ₪")
        print(f"{'מספר תכניות:':>75} {len(df)}")

def main():
    """פונקציה ראשית להפעלת המערכת"""
    print("🎯 מערכת חילוץ נתוני פנסיה אוטומטית")
    print("=" * 60)

    # יצירת מופע של המערכת
    extractor = PensionDataExtractor()

    # בדיקה אם תיקיית DATA קיימת
    if not os.path.exists(extractor.data_dir):
        print(f"📁 תיקיית DATA לא קיימת: {extractor.data_dir}")
        print("💡 יוצר תיקיית DATA...")
        os.makedirs(extractor.data_dir, exist_ok=True)
        print(f"✅ תיקיית DATA נוצרה: {extractor.data_dir}")
        print("📋 הכנס קבצי XML לתיקייה זו והרץ שוב")
        return

    # עיבוד כל הקבצים
    df = extractor.process_all_files()

    if df is not None:
        print("
🎉 המערכת הושלמה בהצלחה!"        print(f"📊 נמצאו {len(df)} תכניות פנסיה")
        print(f"💰 סה״כ יתרה: {df['יתרה'].sum():,.2f} ₪")

if __name__ == "__main__":
    main()
