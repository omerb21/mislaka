import os
import xml.etree.ElementTree as ET
import re

def find_all_numeric_values():
    """Find all numeric values in XML files to locate balances"""
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print("🔍 Searching for all numeric values in XML files...\n")

    for xml_file in xml_files:
        print(f"📄 {xml_file}:")
        file_path = os.path.join(xml_dir, xml_file)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Remove control characters
            content = content.replace('\x1a', '')

            # Find all numeric patterns (including decimals)
            numeric_pattern = r'\b\d+\.?\d*\b'
            matches = re.findall(numeric_pattern, content)

            # Filter for significant amounts (greater than 1000)
            significant_amounts = []
            for match in matches:
                try:
                    value = float(match)
                    if value > 1000:  # Only show significant amounts
                        significant_amounts.append((value, match))
                except:
                    pass

            # Sort by value and show top amounts
            significant_amounts.sort(reverse=True)

            print(f"   Found {len(significant_amounts)} significant amounts:")
            for value, original in significant_amounts[:10]:  # Show top 10
                print(f"   💰 {value:>12,.2f} ({original})")

        except Exception as e:
            print(f"❌ Error: {str(e)}")

        print()

if __name__ == "__main__":
    find_all_numeric_values()
