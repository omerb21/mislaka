#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import re
import sys

# Namespace handling for XML parsing
namespaces = {
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

def safe_float(value, default=0.0):
    """Safely convert a string to float, return default if conversion fails."""
    if value is None:
        return default
    try:
        # Remove any non-numeric characters except decimal point and minus
        clean_value = re.sub(r'[^\d.-]', '', str(value).strip())
        return float(clean_value) if clean_value else default
    except (ValueError, TypeError):
        return default

def format_amount(value):
    """Format amount with 2 decimal places and no thousands separator."""
    try:
        return f"{float(value):.2f}"
    except (ValueError, TypeError):
        return "0.00"

def format_percentage(value):
    """Format percentage (0-100) with 2 decimal places and no % sign."""
    try:
        return f"{float(value):.2f}"
    except (ValueError, TypeError):
        return "0.00"

def parse_date(date_str):
    """Parse date from YYYYMMDD format to DD/MM/YYYY."""
    if not date_str or len(date_str) != 8 or not date_str.isdigit():
        return ""
    try:
        return f"{date_str[6:8]}/{date_str[4:6]}/{date_str[0:4]}"
    except:
        return ""

def get_text(element, path, default=''):
    """Safely get text from XML element."""
    elem = element.find(path)
    return elem.text.strip() if elem is not None and elem.text else default

def get_float(element, path, default=0.0):
    """Safely get float from XML element."""
    return safe_float(get_text(element, path), default)

def parse_xml_file(file_path):
    """Parse a single XML file and extract pension plan information."""
    try:
        # Parse with explicit UTF-8 encoding
        with open(file_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        # Clean XML content if needed
        xml_content = xml_content.replace('\x1a', '')  # Remove EOF character if present
        
        # Parse the XML
        root = ET.fromstring(xml_content)
        
        # Extract company information
        yatzran = root.find('.//YeshutYatzran')
        company_name = get_text(yatzran, 'SHEM-YATZRAN') if yatzran is not None else 'לא ידוע'
        
        plans = []
        
        # Process each account/policy
        for heshbon in root.findall('.//HeshbonOPolisa'):
            plan_info = {
                'מספר ת.ז. לקוח': '051683845',
                'שם לקוח': 'פנחס גרינברג',
                'מספר פוליסה/חשבון': get_text(heshbon, 'MISPAR-POLISA-O-HESHBON'),
                'שם תכנית': get_text(heshbon, 'SHEM-TOCHNIT'),
                'חברה מנהלת': company_name,
                'סטטוס': get_text(heshbon, 'STATUS-TOCHNIT', 'פעיל'),
                'תאריך פתיחה': parse_date(get_text(heshbon, 'TAARICH-HITZTARFUT-MUTZAR')),
                'יתרת צבירה': '0.00',
                'צבירת פיצויים': '0.00',
                'צבירת תגמולים': '0.00',
                'סכום הלוואות': '0.00',
                'סכום כולל': '0.00',
                'תאריך עדכון': datetime.now().strftime('%d/%m/%Y'),
                'מסלול השקעה': get_text(heshbon, 'SHEM-MESLUL-HASHKA-A', 'לא צוין')
            }
            
            # Determine plan type
            if 'השתלמות' in plan_info['שם תכנית']:
                plan_info['סוג תכנית'] = 'קרן השתלמות'
            elif 'פנסיה' in plan_info['שם תכנית']:
                plan_info['סוג תכנית'] = 'קרן פנסיה'
            else:
                plan_info['סוג תכנית'] = 'קופת גמל'
            
            # Extract financial information
            yitrot = heshbon.find('.//Yitrot')
            if yitrot is not None:
                for yitra in yitrot.findall('PerutYitrot'):
                    kod_sug = get_text(yitra, 'KOD-SUG-ITRA')
                    amount = get_float(yitra, 'SACHAR-HODASHI')
                    
                    if kod_sug == '1':  # יתרת צבירה
                        plan_info['יתרת צבירה'] = format_amount(amount)
                    elif kod_sug == '2':  # פיצויים
                        plan_info['צבירת פיצויים'] = format_amount(amount)
                    elif kod_sug == '3':  # תגמולים
                        plan_info['צבירת תגמולים'] = format_amount(amount)
            
            # Extract loan information
            halvaot = heshbon.find('.//Halvaot')
            if halvaot is not None:
                total_loans = sum(
                    safe_float(halva.find('SCHUM-HALVAH').text)
                    for halva in halvaot.findall('Halvaah')
                    if halva.find('SCHUM-HALVAH') is not None
                )
                plan_info['סכום הלוואות'] = format_amount(total_loans)
            
            # Calculate total amount (savings + benefits - loans)
            try:
                total = (
                    safe_float(plan_info['יתרת צבירה']) +
                    safe_float(plan_info['צבירת פיצויים']) +
                    safe_float(plan_info['צבירת תגמולים']) -
                    safe_float(plan_info['סכום הלוואות'])
                )
                plan_info['סכום כולל'] = format_amount(max(0, total))
            except (ValueError, AttributeError):
                plan_info['סכום כולל'] = '0.00'
            
            plans.append(plan_info)
            
        return plans
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return []

def main():
    # Set default encoding to UTF-8
    if sys.version_info[0] < 3:
        reload(sys)
        sys.setdefaultencoding('utf-8')
    
    # Directory containing XML files
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]
    
    if not xml_files:
        print("No XML files found for the specified ID.")
        return
    
    all_plans = []
    
    # Process each XML file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(u"Processing file: {}".format(xml_file))
        try:
            plans = parse_xml_file(file_path)
            if plans:
                all_plans.extend(plans)
        except Exception as e:
            print(u"Error processing file {}: {}".format(xml_file, str(e)))
    
    # Create DataFrame and save to Excel
    if all_plans:
        # Define column order for better readability
        columns_order = [
            'מספר ת.ז. לקוח', 'שם לקוח', 'מספר פוליסה/חשבון', 'שם תכנית',
            'סוג תכנית', 'חברה מנהלת', 'סטטוס', 'תאריך פתיחה',
            'יתרת צבירה', 'צבירת פיצויים', 'צבירת תגמולים',
            'סכום הלוואות', 'סכום כולל', 'מסלול השקעה', 'תאריך עדכון'
        ]
        
        df = pd.DataFrame(all_plans)
        
        # Reorder columns
        df = df[columns_order]
        
        # Save to Excel with proper formatting
        excel_file = os.path.join(xml_dir, 'pension_summary_51683845.xlsx')
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            # Main summary sheet
            df.to_excel(writer, index=False, sheet_name='סיכום פנסיוני')
            
            # Get the workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['סיכום פנסיוני']
            
            # Add a header format
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#D7E4BC',
                'border': 1,
                'align': 'center',
                'font_name': 'Arial',
                'font_size': 10
            })
            
            # Add a format for numbers
            number_format = workbook.add_format({'num_format': '#,##0.00', 'font_name': 'Arial'})
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Set column widths and formats
            for i, col in enumerate(df.columns):
                # Set column width
                max_length = max(
                    df[col].astype(str).apply(len).max(),
                    len(str(col))
                ) + 2
                worksheet.set_column(i, i, min(max_length, 20))
                
                # Apply number format to numeric columns
                if any(keyword in col for keyword in ['יתרה', 'סכום', 'צבירה', 'פיצויים', 'תגמולים']):
                    worksheet.set_column(i, i, 15, number_format)
            
            # Add a total row
            total_row = len(df) + 2
            worksheet.write(total_row, 7, 'סה"כ:', header_format)
            for i, col in enumerate(df.columns):
                if any(keyword in col for keyword in ['יתרה', 'סכום', 'צבירה', 'פיצויים', 'תגמולים']):
                    if col == 'סכום הלוואות':
                        # Sum of loans (should be negative)
                        worksheet.write_formula(
                            total_row, i,
                            f'=SUBTOTAL(9,{chr(65+i)}2:{chr(65+i)}{len(df)+1})',
                            number_format
                        )
                    else:
                        # Sum of other amounts
                        worksheet.write_formula(
                            total_row, i,
                            f'=SUBTOTAL(9,{chr(65+i)}2:{chr(65+i)}{len(df)+1})',
                            number_format
                        )
            
            # Add some summary statistics
            stats_row = total_row + 2
            worksheet.write(stats_row, 0, 'סטטיסטיקות:', header_format)
            worksheet.write(stats_row + 1, 0, 'מספר חשבונות')
            worksheet.write_formula(stats_row + 1, 1, f'=COUNTA(A2:A{len(df)+1})')
            
            worksheet.write(stats_row + 2, 0, 'סה"כ חיסכון')
            worksheet.write_formula(
                stats_row + 2, 1,
                f'=SUM(I{total_row+1}:K{total_row+1})-L{total_row+1}',
                number_format
            )
            
            # Add a chart
            if len(df) > 0:
                chart = workbook.add_chart({'type': 'column'})
                
                # Configure the chart
                chart.add_series({
                    'name': '=סיכום פנסיוני!$I$1',
                    'categories': f'=סיכום פנסיוני!$D$2:$D${len(df)+1}',
                    'values': f'=סיכום פנסיוני!$I$2:$I${len(df)+1}',
                })
                
                chart.set_title({'name': 'יתרת צבירה לפי חשבון'})
                chart.set_x_axis({'name': 'חשבון'})
                chart.set_y_axis({'name': 'שקלים'})
                
                # Insert the chart into the worksheet
                worksheet.insert_chart(stats_row + 5, 0, chart)
        
        print(u"\nExcel file created successfully: {}".format(excel_file))
        print(u"\nסיכום תכניות פנסיה וחסכונות:")
        print(u"-" * 80)
        
        if not df.empty:
            total_savings = df['סכום כולל'].astype(float).sum()
            print(u"סך כל החיסכון הפנסיוני: {:,.2f} שקלים".format(total_savings))
            
            # Print a summary by plan type
            print(u"\nפירוט לפי סוג תכנית:")
            for plan_type, group in df.groupby('סוג תכנית'):
                total = group['סכום כולל'].astype(float).sum()
                print(u"- {}: {:,.2f} שקלים".format(plan_type, total))
    else:
        print("No valid pension plan data found in the XML files.")

if __name__ == "__main__":
    main()
