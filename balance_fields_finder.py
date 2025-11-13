import os
import xml.etree.ElementTree as ET

def find_balance_fields():
    """Find all balance-related fields in XML files"""
    xml_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    xml_files = [f for f in os.listdir(xml_dir) if f.endswith('.xml') and f.startswith('51683845')]

    print("🔍 Searching for balance-related fields in XML files...\n")

    # Balance-related field patterns
    balance_patterns = [
        'CHISACHON', 'PITZUIM', 'TAGMULIM', 'KITZVA', 'ZIKNA',
        'YITRA', 'ERECH', 'PIDION', 'SCHUM'
    ]

    for xml_file in xml_files:
        print(f"📄 {xml_file}:")
        file_path = os.path.join(xml_dir, xml_file)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            content = content.replace('\x1a', '')
            root = ET.fromstring(content)

            # Find all elements
            all_elements = []
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    tag = elem.tag
                    text = elem.text.strip()

                    # Check if tag contains balance-related patterns
                    for pattern in balance_patterns:
                        if pattern in tag.upper():
                            try:
                                value = float(text)
                                all_elements.append((tag, value, text))
                                break
                            except:
                                pass

            # Sort by value and show significant amounts
            all_elements.sort(key=lambda x: x[1], reverse=True)

            print(f"   Found {len(all_elements)} balance-related fields:")
            for tag, value, original in all_elements[:20]:  # Show top 20
                print(f"   💰 {tag}: {value:>15,.2f} ({original})")

        except Exception as e:
            print(f"❌ Error: {str(e)}")

        print("\n" + "="*60)

if __name__ == "__main__":
    find_balance_fields()
