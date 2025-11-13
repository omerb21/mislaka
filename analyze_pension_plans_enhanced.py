import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

def get_text(element, path, default='0.00'):
    """Helper function to safely get text from XML element."""
    elem = element.find(f'.//{path}')
    return elem.text if elem is not None and elem.text else default

def parse_xml_file(file_path):
    """Parse a single XML file and extract pension plan information."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract company information
        company_name = get_text(root, 'SHEM-YATZRAN', 'לא ידוע')
        
        plans = []
        
        # Find all policy/account sections
        for heshbon in root.findall('.//HeshbonOPolisa'):
            plan_info = {
                'מספר ת.ז. לקוח': '051683845',
                'שם לקוח': 'פנחס גרינברג',
                'מספר פוליסה/חשבון': get_text(heshbon, 'MISPAR-POLISA-O-HESHBON', 'לא ידוע'),
                'שם תכנית': get_text(heshbon, 'SHEM-TOCHNIT', 'לא ידוע'),
                'חברה מנהלת': company_name,
                'סוג תכנית': determine_plan_type(heshbon),
                'סטטוס': get_text(heshbon, 'STATUS-TOCHNIT', 'פעיל'),
                'תאריך פתיחה': format_date(get_text(heshbon, 'TAARICH-HITZTARFUT-MUTZAR', '')),
                'יתרת צבירה': '0.00',
                'צבירת פיצויים': '0.00',
                'צבירת תגמולים': '0.00',
                'סכום הלוואות': '0.00',
                'סכום כולל': '0.00',
                'תאריך עדכון': datetime.now().strftime('%d/%m/%Y'),
                'מסלול השקעה': get_text(heshbon, 'SHEM-MESLUL-HASHKA-A', 'לא צוין')
            }
            
            # Extract financial information from Yitrot (returns and balances)
            yitrot = heshbon.find('.//Yitrot')
            if yitrot is not None:
                for yitra in yitrot.findall('PerutYitrot'):
                    kod_sug = get_text(yitra, 'KOD-SUG-ITRA')
                    amount = get_text(yitra, 'SACHAR-HODASHI')
                    
                    if kod_sug == '1':  # Total savings
                        plan_info['יתרת צבירה'] = format_amount(amount)
                    elif kod_sug == '2':  # Compensation
                        plan_info['צבירת פיצויים'] = format_amount(amount)
                    elif kod_sug == '3':  # Benefits
                        plan_info['צבירת תגמולים'] = format_amount(amount)
            
            # Extract loan information
            halvaot = heshbon.find('.//Halvaot')
            if halvaot is not None:
                total_loans = sum(
                    float(halva.find('SCHUM-HALVAH').text or 0)
                    for halva in halvaot.findall('Halvaah')
                    if halva.find('SCHUM-HALVAH') is not None and halva.find('SCHUM-HALVAH').text
                )
                plan_info['סכום הלוואות'] = format_amount(str(total_loans))
            
            # Calculate total amount (savings + benefits - loans)
            try:
                total = (
                    float(plan_info['יתרת צבירה'].replace(',', '')) +
                    float(plan_info['צבירת פיצויים'].replace(',', '')) +
                    float(plan_info['צבירת תגמולים'].replace(',', '')) -
                    float(plan_info['סכום הלוואות'].replace(',', ''))
                )
                plan_info['סכום כולל'] = format_amount(str(max(0, total)))
            except (ValueError, AttributeError):
                plan_info['סכום כולל'] = '0.00'
            
            plans.append(plan_info)
            
        return plans
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return []

def determine_plan_type(heshbon_element):
    """Determine the type of pension plan based on the XML structure."""
    # Check for קרן השתלמות
    if 'השתלמות' in get_text(heshbon_element, 'SHEM-TOCHNIT', ''):
        return 'קרן השתלמות'
    
    # Check for ביטוח מנהלים
    if heshbon_element.find('.//BITUAH-MENAHALIM') is not None:
        return 'ביטוח מנהלים'
    
    # Check for קופת גמל
    if heshbon_element.find('.//KUPAT-GMEL') is not None:
        return 'קופת גמל'
    
    # Check for קרן פנסיה
    if heshbon_element.find('.//KEREN-PENSIA') is not None:
        return 'קרן פנסיה'
    
    # Default to קופת גמל if no specific type is found
    return 'קופת גמל'

def format_amount(amount_str):
    """Format amount string with thousands separators and 2 decimal places."""
    try:
        # Remove any non-numeric characters except decimal point
        clean_str = ''.join(c for c in amount_str if c.isdigit() or c == '.')
        amount = float(clean_str)
        return f"{amount:,.2f}"
    except (ValueError, AttributeError):
        return '0.00'

def format_date(date_str):
    """Format date from YYYYMMDD to DD/MM/YYYY."""
    if not date_str or len(date_str) != 8 or not date_str.isdigit():
        return 'לא ידוע'
    try:
        return f"{date_str[6:8]}/{date_str[4:6]}/{date_str[0:4]}"
    except:
        return 'לא ידוע'

def main():
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
        print(f"Processing file: {xml_file}")
        plans = parse_xml_file(file_path)
        all_plans.extend(plans)
    
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
        excel_file = os.path.join(xml_dir, 'pension_summary_51683845_detailed.xlsx')
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
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
            
            # Add a format for dates
            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy', 'font_name': 'Arial'})
            
            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Set column widths and formats
            for i, col in enumerate(df.columns):
                # Set column width
                max_length = max(df[col].astype(str).apply(len).max(), len(col)) + 2
                worksheet.set_column(i, i, min(max_length, 20))
                
                # Apply number format to numeric columns
                if any(term in col for term in ['סכום', 'יתרה', 'צבירה', 'פיצויים', 'תגמולים']):
                    worksheet.set_column(i, i, 15, number_format)
                # Apply date format to date columns
                elif 'תאריך' in col:
                    worksheet.set_column(i, i, 12, date_format)
        
        print(f"\nExcel file created successfully: {excel_file}")
        
        # Display the summary in the console
        print("\nסיכום תכניות פנסיה וחסכונות:")
        print("-" * 150)
        print(df.to_string(index=False, justify='right'))
        
        # Calculate and display totals
        total_savings = sum(float(x.replace(',', '')) for x in df['סכום כולל'] if x.replace('.', '').isdigit())
        print(f"\nסך כל החיסכון הפנסיוני: {total_savings:,.2f} ש\"ח")
        
    else:
        print("No pension plan information found in the XML files.")

if __name__ == "__main__":
    main()
