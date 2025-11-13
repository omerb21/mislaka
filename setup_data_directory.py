import os
import shutil
import glob

def setup_data_directory():
    """הכנת תיקיית DATA עם קבצי הXML"""
    current_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    data_dir = os.path.join(current_dir, 'DATA')

    print("🔧 מכין תיקיית DATA...")

    # יצירת תיקיית DATA אם לא קיימת
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"✅ נוצרה תיקיית DATA: {data_dir}")
    else:
        print(f"✅ תיקיית DATA קיימת: {data_dir}")

    # העתקת קבצי XML לתיקיית DATA
    xml_files = glob.glob(os.path.join(current_dir, '*.xml'))
    copied_count = 0

    for xml_file in xml_files:
        filename = os.path.basename(xml_file)
        dest_file = os.path.join(data_dir, filename)

        if not os.path.exists(dest_file):
            shutil.copy2(xml_file, dest_file)
            copied_count += 1
            print(f"   📄 הועתק: {filename}")

    if copied_count == 0:
        print("   📋 כל הקבצים כבר קיימים בתיקיית DATA")
    else:
        print(f"✅ הועתקו {copied_count} קבצי XML לתיקיית DATA")

    # ספירת קבצים בתיקיית DATA
    data_files = glob.glob(os.path.join(data_dir, '*.xml'))
    print(f"📊 סה״כ קבצי XML בתיקיית DATA: {len(data_files)}")

    return data_dir

if __name__ == "__main__":
    setup_data_directory()
