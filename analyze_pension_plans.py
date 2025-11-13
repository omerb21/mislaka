import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

def parse_xml_file(file_path):
    """Parse a single XML file and extract pension plan information."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract company information
        company_name = root.find('.//SHEM-YATZRAN')
        company_name = company_name.text if company_name is not None else 'לא ידוע'
        
        plans = []
        
        # Find all policy/account sections
        for heshbon in root.findall('.//HeshbonOPolisa'):
            plan_info = {
                'שם תכנית': heshbon.find('SHEM-TOCHNIT').text if heshbon.find('SHEM-TOCHNIT') is not None else 'לא ידוע',
                'מספר פוליסה/חשבון': heshbon.find('MISPAR-POLISA-O-HESHBON').text if heshbon.find('MISPAR-POLISA-O-HESHBON') is not None else 'לא ידוע',
                'חברה מנהלת': company_name,
                'סוג תכנית': 'השתלמות' if 'השתלמות' in (heshbon.find('SHEM-TOCHNIT').text if heshbon.find('SHEM-TOCHNIT') is not None else '') else 'פנסיה',
                'צבירת פיצויים': '0.00',
                'צבירת תגמולים': '0.00',
                'סכום כולל': '0.00',
                'תאריך עדכון': datetime.now().strftime('%d/%m/%Y')
            }
            
            # Extract financial information
            yitrot = heshbon.find('.//Yitrot')
            if yitrot is not None:
                for yitra in yitrot.findall('PerutYitrot'):
                    kod_sug = yitra.find('KOD-SUG-ITRA')
                    if kod_sug is not None and kod_sug.text == '1':  # Total savings
                        amount = yitra.find('SACHAR-HODASHI')
                        if amount is not None and amount.text:
                            plan_info['סכום כולל'] = f"{float(amount.text):,.2f}"
                    elif kod_sug is not None and kod_sug.text == '2':  # Compensation
                        amount = yitra.find('SACHAR-HODASHI')
                        if amount is not None and amount.text:
                            plan_info['צבירת פיצויים'] = f"{float(amount.text):,.2f}"
                    elif kod_sug is not None and kod_sug.text == '3':  # Benefits
                        amount = yitra.find('SACHAR-HODASHI')
                        if amount is not None and amount.text:
                            plan_info['צבירת תגמולים'] = f"{float(amount.text):,.2f}"
            
            plans.append(plan_info)
            
        return plans
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return []

def main():
    # Directory containing XML files
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]
    
    all_plans = []
    
    # Process each XML file
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        plans = parse_xml_file(file_path)
        all_plans.extend(plans)
    
    # Create DataFrame and save to Excel
    if all_plans:
        df = pd.DataFrame(all_plans)
        excel_file = os.path.join(xml_dir, 'pension_summary_51683845.xlsx')
        df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Excel file created successfully: {excel_file}")
        
        # Display the summary
        print("\nסיכום תכניות פנסיה וחסכונות:")
        print("-" * 100)
        print(df.to_string(index=False))
        
    else:
        print("No pension plan information found in the XML files.")

if __name__ == "__main__":
    main()
