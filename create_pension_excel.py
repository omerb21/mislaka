import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

def extract_pension_data(file_path):
    """Extract pension data from XML file."""
    try:
        # Read and parse XML file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Clean content and parse
        content = content.replace('\x1a', '')
        root = ET.fromstring(content)
        
        # Get company name
        company = root.find('.//SHEM-YATZRAN')
        company_name = company.text if company is not None else 'לא ידוע'
        
        results = []
        
        # Process each account
        for heshbon in root.findall('.//HeshbonOPolisa'):
            # Basic info
            account = heshbon.find('MISPAR-POLISA-O-HESHBON')
            plan_name = heshbon.find('SHEM-TOCHNIT')
            
            # Determine plan type
            plan_type = 'קופת גמל'
            if plan_name is not None and 'השתלמות' in plan_name.text:
                plan_type = 'קרן השתלמות'
            
            # Get financial data
            yitrot = heshbon.find('.//Yitrot')
            savings = '0.00'
            
            if yitrot is not None:
                for yitra in yitrot.findall('PerutYitrot'):
                    kod = yitra.find('KOD-SUG-ITRA')
                    amount = yitra.find('SACHAR-HODASHI')
                    if kod is not None and amount is not None:
                        savings = amount.text if kod.text == '1' else savings
            
            results.append({
                'מספר חשבון': account.text if account is not None else 'לא ידוע',
                'סוג תכנית': plan_type,
                'שם התכנית': plan_name.text if plan_name is not None else 'לא צוין',
                'חברה מנהלת': company_name,
                'יתרה': savings if savings != '0.00' else '0.00',
                'תאריך עדכון': datetime.now().strftime('%d/%m/%Y')
            })
            
        return results
    
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return []

def main():
    # Get all XML files
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) 
                if f.endswith('.xml') and f.startswith('51683845')]
    
    if not xml_files:
        print("לא נמצאו קבצי XML")
        return
    
    all_plans = []
    
    # Process each file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"Processing: {xml_file}")
        plans = extract_pension_data(file_path)
        all_plans.extend(plans)
    
    # Create DataFrame
    if all_plans:
        df = pd.DataFrame(all_plans)
        
        # Set column order
        columns = ['מספר חשבון', 'סוג תכנית', 'שם התכנית', 'חברה מנהלת', 'יתרה', 'תאריך עדכון']
        df = df[columns]
        
        # Save to Excel
        output_file = 'pension_summary.xlsx'
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='סיכום פנסיה')
            
            # Get workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['סיכום פנסיה']
            
            # Add a header format
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
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Set column widths
            worksheet.set_column('A:A', 18)  # Account number
            worksheet.set_column('B:B', 15)  # Plan type
            worksheet.set_column('C:C', 25)  # Plan name
            worksheet.set_column('D:D', 30)  # Company
            worksheet.set_column('E:E', 15)  # Balance
            worksheet.set_column('F:F', 15)  # Update date
            
            # Add number format for balance column
            number_format = workbook.add_format({'num_format': '#,##0.00'})
            worksheet.set_column('E:E', 15, number_format)
            
            # Add autofilter
            worksheet.autofilter(0, 0, len(df), len(df.columns)-1)
            
            # Add total row
            total_row = len(df) + 2
            worksheet.write(total_row, 3, 'סה"כ:', header_format)
            worksheet.write_formula(total_row, 4, f'=SUM(E2:E{len(df)+1})', number_format)
            
            # Add some summary statistics
            worksheet.write(total_row + 1, 0, 'מספר חשבונות:')
            worksheet.write_number(total_row + 1, 1, len(df))
            
            # Add a title
            title = "סיכום תיק פנסיה - פינחס גרינברג (ת.ז. 051683845)"
            worksheet.merge_range('A1:F1', title, workbook.add_format({
                'bold': True, 'font_size': 14, 'align': 'center',
                'valign': 'vcenter', 'fg_color': '#B4C6E7'
            }))
            
            # Add a note
            note = "נכון לתאריך: " + datetime.now().strftime('%d/%m/%Y')
            worksheet.merge_range(f'A{total_row + 3}:F{total_row + 3}', note, 
                               workbook.add_format({'italic': True, 'font_size': 10}))
            
        print(f"\nקובץ האקסל נוצר בהצלחה: {output_file}")
        print("מיקום הקובץ:", os.path.abspath(output_file))
    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    main()
