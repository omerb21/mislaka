import os
import xml.etree.ElementTree as ET

def detailed_account_analysis():
    """Detailed analysis of each account in XML files"""
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print("🔍 Detailed analysis of all accounts...\n")

    for xml_file in xml_files:
        print(f"📄 {xml_file}:")
        file_path = os.path.join(xml_dir, xml_file)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'Unknown'
            print(f"   Company: {company_name}")

            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   Total accounts: {len(accounts)}")

            for i, account in enumerate(accounts, 1):
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                plan_name = account.find('SHEM-TOCHNIT')

                if account_num is not None and account_num.text:
                    acc_num = account_num.text.strip()
                    print(f"\n   📋 ACCOUNT {i}: {acc_num}")

                    if plan_name is not None and plan_name.text:
                        print(f"      Plan: {plan_name.text}")

                    # Analyze Yitrot section
                    yitrot_section = account.find('.//Yitrot')
                    if yitrot_section is not None:
                        print("      📊 Yitrot Section:")

                        # Show all elements in Yitrot
                        for elem in yitrot_section:
                            if elem.text and elem.text.strip():
                                try:
                                    value = float(elem.text.strip())
                                    print(f"         {elem.tag}: {value:>12,.2f}")
                                except (ValueError, TypeError):
                                    print(f"         {elem.tag}: {elem.text.strip()}")

                            # Also check sub-elements
                            for sub_elem in elem:
                                if sub_elem.text and sub_elem.text.strip():
                                    try:
                                        value = float(sub_elem.text.strip())
                                        print(f"            {sub_elem.tag}: {value:>10,.2f}")
                                    except (ValueError, TypeError):
                                        print(f"            {sub_elem.tag}: {sub_elem.text.strip()}")

                    # Also check YitraLefiGilPrisha section
                    yitra_section = account.find('.//YitraLefiGilPrisha')
                    if yitra_section is not None:
                        print("      📈 YitraLefiGilPrisha Section:")

                        for elem in yitra_section:
                            if elem.text and elem.text.strip():
                                try:
                                    value = float(elem.text.strip())
                                    print(f"         {elem.tag}: {value:>12,.2f}")
                                except (ValueError, TypeError):
                                    print(f"         {elem.tag}: {elem.text.strip()}")

        except Exception as e:
            print(f"❌ Error processing {xml_file}: {str(e)}")

        print("\n" + "="*80)

if __name__ == "__main__":
    detailed_account_analysis()
