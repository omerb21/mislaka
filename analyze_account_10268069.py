import xml.etree.ElementTree as ET

def analyze_specific_account():
    """Analyze the specific account 10268069"""
    file_path = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ\51683845_512244146_KGM_202502051310_2.xml'

    print("🔍 Analyzing account 10268069 in file 51683845_512244146_KGM_202502051310_2.xml\n")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        content = content.replace('\x1a', '')
        root = ET.fromstring(content)

        # Find the specific account
        accounts = root.findall('.//HeshbonOPolisa')
        target_account = None

        for account in accounts:
            account_num = account.find('MISPAR-POLISA-O-HESHBON')
            if account_num is not None and account_num.text and '10268069' in account_num.text:
                target_account = account
                break

        if target_account is None:
            print("❌ Account 10268069 not found!")
            return

        print("✅ Found account 10268069")

        # Show account details
        plan_name = target_account.find('SHEM-TOCHNIT')
        if plan_name is not None and plan_name.text:
            print(f"Plan: {plan_name.text}")

        # Analyze Yitrot section in detail
        yitrot_section = target_account.find('.//Yitrot')
        if yitrot_section is not None:
            print("\n📊 Yitrot Section Details:")

            def print_element_details(element, depth=0):
                indent = "  " * depth
                if element.text and element.text.strip():
                    try:
                        value = float(element.text.strip())
                        print(f"{indent}{element.tag}: {value:>15,.2f}")
                    except (ValueError, TypeError):
                        print(f"{indent}{element.tag}: {element.text.strip()}")

                for child in element:
                    print_element_details(child, depth + 1)

            print_element_details(yitrot_section)

        # Also check YitraLefiGilPrisha section
        yitra_section = target_account.find('.//YitraLefiGilPrisha')
        if yitra_section is not None:
            print("\n📈 YitraLefiGilPrisha Section Details:")
            print_element_details(yitra_section)

    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    analyze_specific_account()
