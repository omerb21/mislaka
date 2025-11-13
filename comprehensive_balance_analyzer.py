import os
import xml.etree.ElementTree as ET
import pandas as pd
import time

def analyze_all_balance_fields():
    """Analyze all possible balance fields in XML files"""
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print("🔍 Analyzing all balance fields in XML files...\n")

    # Define all possible balance-related fields
    balance_fields = [
        'YITRAT-KASPEY-TAGMULIM',
        'TOTAL-CHISACHON-MTZBR',
        'SCHUM-BITUACH-MENAYOT',
        'SCHUM-PITZUIM',
        'SCHUM-TAGMULIM',
        'SCHUM-PITZUIM-NIFRADIM',
        'SCHUM-PITZUIM-MITZTARFIM',
        'SCHUM-CHISACHON-MITZTABER',
        'SCHUM-KITZVAT-ZIKNA',
        'YITRAT-SOF-SHANA',
        'ERECH-PIDYON-SOF-SHANA'
    ]

    all_accounts = []

    for xml_file in xml_files:
        file_path = os.path.join(xml_dir, xml_file)
        print(f"📄 {xml_file}:")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            company = root.find('.//SHEM-YATZRAN')
            company_name = company.text if company is not None else 'Unknown'

            accounts = root.findall('.//HeshbonOPolisa')
            print(f"   Found {len(accounts)} accounts")

            for i, account in enumerate(accounts, 1):
                account_num = account.find('MISPAR-POLISA-O-HESHBON')
                plan_name = account.find('SHEM-TOCHNIT')

                if account_num is not None and account_num.text:
                    acc_num = account_num.text.strip()
                    print(f"\n   📋 Account {i}: {acc_num}")

                    if plan_name is not None and plan_name.text:
                        print(f"      Plan: {plan_name.text}")

                    # Check Yitrot section
                    yitrot_section = account.find('.//Yitrot')
                    if yitrot_section is not None:
                        print("      ✅ Found Yitrot section")

                        # Check valuation date
                        val_date = yitrot_section.find('TAARICH-ERECH-TZVIROT')
                        if val_date is not None and val_date.text:
                            print(f"      📅 Valuation Date: {val_date.text}")

                        # Check all balance fields
                        for field in balance_fields:
                            elements = yitrot_section.findall(f'.//{field}')
                            for elem in elements:
                                if elem.text and elem.text.strip():
                                    try:
                                        value = float(elem.text.strip())
                                        print(f"      💰 {field}: {value}")
                                    except (ValueError, TypeError):
                                        print(f"      📝 {field}: {elem.text.strip()}")

                    # Also check YitraLefiGilPrisha section
                    yitra_section = account.find('.//YitraLefiGilPrisha')
                    if yitra_section is not None:
                        print("      ✅ Found YitraLefiGilPrisha section")

                        kupot = yitra_section.find('Kupot')
                        if kupot is not None:
                            for kupa in kupot.findall('Kupa'):
                                for field in ['SCHUM-CHISACHON-MITZTABER', 'SCHUM-KITZVAT-ZIKNA']:
                                    balance_elem = kupa.find(field)
                                    if balance_elem is not None and balance_elem.text:
                                        try:
                                            value = float(balance_elem.text.strip())
                                            print(f"      💰 Kupa/{field}: {value}")
                                        except (ValueError, TypeError):
                                            print(f"      📝 Kupa/{field}: {balance_elem.text.strip()}")

        except Exception as e:
            print(f"❌ Error processing {xml_file}: {str(e)}")

        print("\n" + "="*60)

if __name__ == "__main__":
    analyze_all_balance_fields()
