import os
import xml.etree.ElementTree as ET
from datetime import datetime

def format_amount(amount):
    """Format amount with 2 decimal places and no thousands separator."""
    try:
        return "{:.2f}".format(float(amount))
    except (ValueError, TypeError):
        return "0.00"

def extract_pension_info(xml_file):
    """Extract pension information from XML file."""
    try:
        # Read and parse the XML file
        with open(xml_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        # Clean XML content
        xml_content = xml_content.replace('\x1a', '')
        root = ET.fromstring(xml_content)
        
        # Extract company information
        company = root.find('.//SHEM-YATZRAN')
        company_name = company.text if company is not None else 'לא ידוע'
        
        # Extract client information
        client_id = root.find('.//MISPAR-ZIHUY-LAKOACH')
        client_name = root.find('.//SHEM-PRATI')
        
        # Initialize results list
        results = []
        
        # Process each account/policy
        for heshbon in root.findall('.//HeshbonOPolisa'):
            # Basic account info
            account_number = heshbon.find('MISPAR-POLISA-O-HESHBON')
            plan_name = heshbon.find('SHEM-TOCHNIT')
            
            # Determine plan type
            plan_type = 'קופת גמל'  # Default
            if plan_name is not None and 'השתלמות' in plan_name.text:
                plan_type = 'קרן השתלמות'
            
            # Extract financial information
            yitrot = heshbon.find('.//Yitrot')
            savings = '0.00'
            compensation = '0.00'
            benefits = '0.00'
            
            if yitrot is not None:
                for yitra in yitrot.findall('PerutYitrot'):
                    kod_sug = yitra.find('KOD-SUG-ITRA')
                    amount = yitra.find('SACHAR-HODASHI')
                    
                    if kod_sug is not None and amount is not None:
                        if kod_sug.text == '1':  # Total savings
                            savings = format_amount(amount.text)
                        elif kod_sug.text == '2':  # Compensation
                            compensation = format_amount(amount.text)
                        elif kod_sug.text == '3':  # Benefits
                            benefits = format_amount(amount.text)
            
            # Calculate total
            total = float(savings) + float(compensation) + float(benefits)
            
            # Add to results
            results.append({
                'מספר ת.ז. לקוח': client_id.text if client_id is not None else '051683845',
                'שם לקוח': 'פנחס גרינברג',
                'מספר פוליסה/חשבון': account_number.text if account_number is not None else 'לא ידוע',
                'שם תכנית': plan_name.text if plan_name is not None else 'לא צוין',
                'סוג תכנית': plan_type,
                'חברה מנהלת': company_name,
                'יתרת צבירה': savings,
                'צבירת פיצויים': compensation,
                'צבירת תגמולים': benefits,
                'סכום כולל': format_amount(total),
                'תאריך עדכון': datetime.now().strftime('%d/%m/%Y')
            })
        
        return results
    
    except Exception as e:
        print(f"Error processing {xml_file}: {str(e)}")
        return []

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
        plans = extract_pension_info(file_path)
        all_plans.extend(plans)
    
    # Print summary table
    if all_plans:
        print("\nסיכום תכניות פנסיה וחסכונות:")
        print("-" * 120)
        print("{:<15} {:<15} {:<20} {:<20} {:<15} {:<15} {:<15} {:<15}".format(
            "מספר חשבון", "סוג תכנית", "שם תכנית", "חברה מנהלת", 
            "יתרת צבירה", "פיצויים", "תגמולים", "סכום כולל"
        ))
        print("-" * 120)
        
        total_savings = 0
        total_compensation = 0
        total_benefits = 0
        
        for plan in all_plans:
            print("{:<15} {:<15} {:<20} {:<20} {:>15} {:>15} {:>15} {:>15}".format(
                plan['מספר פוליסה/חשבון'][:12],
                plan['סוג תכנית'],
                plan['שם תכנית'][:18],
                plan['חברה מנהלת'][:18],
                plan['יתרת צבירה'],
                plan['צבירת פיצויים'],
                plan['צבירת תגמולים'],
                plan['סכום כולל']
            ))
            
            total_savings += float(plan['יתרת צבירה'])
            total_compensation += float(plan['צבירת פיצויים'])
            total_benefits += float(plan['צבירת תגמולים'])
        
        print("-" * 120)
        print("{:>80} {:>15} {:>15} {:>15}".format(
            "סה""כ:",
            format_amount(total_savings),
            format_amount(total_compensation),
            format_amount(total_benefits)
        ))
    else:
        print("No valid pension plan data found in the XML files.")

if __name__ == "__main__":
    main()
