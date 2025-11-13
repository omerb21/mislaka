import os
import xml.etree.ElementTree as ET

def extract_pension_data(xml_file):
    """Extract pension data from XML file."""
    try:
        with open(xml_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        # Clean XML content
        xml_content = xml_content.replace('\x1a', '')
        root = ET.fromstring(xml_content)
        
        # Get company name
        company = root.find('.//SHEM-YATZRAN')
        company_name = company.text if company is not None else 'לא ידוע'
        
        # Initialize results list
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
                'שם תכנית': plan_name.text if plan_name is not None else 'לא צוין',
                'חברה מנהלת': company_name,
                'יתרה': savings if savings != '0.00' else '0.00'
            })
            
        return results
    
    except Exception as e:
        print(f"Error processing {xml_file}: {str(e)}")
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
        plans = extract_pension_data(file_path)
        all_plans.extend(plans)
    
    # Print results
    if all_plans:
        print("\nסיכום תכניות פנסיה")
        print("-" * 80)
        print(f"{'חשבון':<15} {'סוג תכנית':<15} {'שם תכנית':<20} {'חברה':<20} {'יתרה':>10}")
        print("-" * 80)
        
        total = 0
        for plan in all_plans:
            print(f"{plan['מספר חשבון'][:12]:<15} {plan['סוג תכנית']:<15} {plan['שם תכנית'][:18]:<20} {plan['חברה מנהלת'][:18]:<20} {plan['יתרה']:>10}")
            try:
                total += float(plan['יתרה'])
            except (ValueError, TypeError):
                pass
        
        print("-" * 80)
        print(f"{'סה""כ:':>65} {total:>10.2f} ₪")
    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    main()
