import os
import xml.etree.ElementTree as ET

def debug_xml_files():
    xml_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]
    
    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"\n{'='*80}\nProcessing: {xml_file}\n{'='*80}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Clean content and parse
            content = content.replace('\x1a', '')
            root = ET.fromstring(content)
            
            # Get company name
            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'לא ידוע'
            print(f"\nחברה: {company_name}")
            
            # Process each account
            for heshbon in root.findall('.//HeshbonOPolisa'):
                account = heshbon.find('MISPAR-POLISA-O-HESHBON')
                plan_name = heshbon.find('SHEM-TOCHNIT')
                
                print(f"\n{'*'*50}")
                print(f"חשבון: {account.text if account is not None else 'לא ידוע'}")
                print(f"תכנית: {plan_name.text if plan_name is not None else 'לא צוין'}")
                
                # Find all balance-related elements
                yitrot_section = heshbon.find('.//Yitrot')
                if yitrot_section is not None:
                    # Get valuation date
                    val_date = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                    print(f"תאריך עדכון: {val_date.text if val_date is not None else 'לא ידוע'}")
                    
                    # Print all PerutYitrot sections
                    for i, yitrot in enumerate(yitrot_section.findall('PerutYitrot'), 1):
                        kod = yitrot.find('KOD-SUG-ITRA')
                        total = yitrot.find('TOTAL-CHISACHON-MTZBR')
                        print(f"\nסעיף {i}:")
                        print(f"  קוד סוג תשואה: {kod.text if kod is not None else 'לא צוין'}")
                        print(f"  סך חסכון מצטבר: {total.text if total is not None else 'לא צוין'}")
                        
                        # Print all elements in this section for debugging
                        print("  כל השדות בקטע:")
                        for elem in yitrot:
                            if elem.text and elem.text.strip():
                                print(f"    {elem.tag}: {elem.text}")
                
                # Also check for any other balance-related elements
                for balance_tag in ['TOTAL-CHISACHON-MTZBR', 'SCHUM-BITUACH-MENAYOT', 'SCHUM-PITZUIM', 
                                  'SCHUM-TAGMULIM', 'SCHUM-PITZUIM-NIFRADIM', 'SCHUM-PITZUIM-MITZTARFIM']:
                    elems = heshbon.findall(f'.//{balance_tag}')
                    for elem in elems:
                        if elem.text and elem.text.strip() and elem.text.strip() != '0':
                            print(f"  {balance_tag}: {elem.text}")
                            
        except Exception as e:
            print(f"שגיאה בעיבוד הקובץ: {str(e)}")

if __name__ == "__main__":
    debug_xml_files()
