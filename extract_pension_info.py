import os
import xml.etree.ElementTree as ET

def get_pension_info(xml_file):
    """Extract pension information from XML file."""
    try:
        # Read and parse XML file
        with open(xml_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Clean content and parse
        content = content.replace('\x1a', '')
        root = ET.fromstring(content)
        
        # Get company name
        company = root.find('.//SHEM-YATZRAN')
        company_name = company.text if company is not None else "לא ידוע"
        
        results = []
        
        # Process each account
        for account in root.findall('.//HeshbonOPolisa'):
            # Get account number
            acc_num = account.find('MISPAR-POLISA-O-HESHBON')
            acc_num = acc_num.text if acc_num is not None else "לא ידוע"
            
            # Get plan name
            plan = account.find('SHEM-TOCHNIT')
            plan_name = plan.text if plan is not None else "לא צוין"
            
            # Determine plan type
            plan_type = "קופת גמל"
            if 'השתלמות' in plan_name:
                plan_type = "קרן השתלמות"
            
            # Get balance
            balance = "0.00"
            yitrot = account.find('.//Yitrot')
            if yitrot is not None:
                for yitra in yitrot.findall('PerutYitrot'):
                    kod = yitra.find('KOD-SUG-ITRA')
                    amount = yitra.find('SACHAR-HODASHI')
                    if kod is not None and amount is not None and kod.text == '1':
                        balance = amount.text
                        break
            
            results.append({
                'account': acc_num,
                'type': plan_type,
                'name': plan_name,
                'company': company_name,
                'balance': balance
            })
        
        return results
    
    except Exception as e:
        print(f"Error processing {xml_file}: {str(e)}")
        return []

def main():
    # Get all XML files
    xml_files = [f for f in os.listdir() if f.startswith('51683845') and f.endswith('.xml')]
    
    if not xml_files:
        print("לא נמצאו קבצי XML")
        return
    
    all_plans = []
    
    # Process each file
    for xml_file in xml_files:
        plans = get_pension_info(xml_file)
        all_plans.extend(plans)
    
    # Print results
    if all_plans:
        print("\nסיכום תכניות פנסיה")
        print("=" * 80)
        print(f"{'חשבון':<15} {'סוג':<15} {'שם תכנית':<20} {'חברה':<20} {'יתרה':>10}")
        print("-" * 80)
        
        total = 0
        for plan in all_plans:
            print(f"{plan['account'][:12]:<15} {plan['type']:<15} {plan['name'][:18]:<20} {plan['company'][:18]:<20} {plan['balance']:>10}")
            try:
                total += float(plan['balance'])
            except (ValueError, TypeError):
                pass
        
        print("-" * 80)
        total_str = f"{total:,.2f}"
        print(" " * 55 + f"סה""כ: {total_str:>10} ₪")
    else:
        print("לא נמצאו נתוני פנסיה")

if __name__ == "__main__":
    main()
