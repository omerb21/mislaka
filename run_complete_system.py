import os
import shutil
import glob
import subprocess

def run_complete_system():
    """הרצת המערכת המלאה - העתקת קבצים והרצת המערכת"""

    current_dir = r'c:\Users\USER\OneDrive\AI PROJECTS\WINSURDF\dev\תשבצ'
    data_dir = os.path.join(current_dir, 'DATA')

    print("🔄 מכין את המערכת המלאה...")

    # יצירת תיקיית DATA אם לא קיימת
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"✅ נוצרה תיקיית DATA: {data_dir}")

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

    print(f"✅ הועתקו {copied_count} קבצי XML לתיקיית DATA")

    # בדיקת מספר הקבצים בתיקיית DATA
    data_files = glob.glob(os.path.join(data_dir, '*.xml'))
    print(f"📊 סה״כ קבצי XML בתיקיית DATA: {len(data_files)}")

    if len(data_files) == 0:
        print("❌ לא נמצאו קבצי XML בתיקיית DATA")
        return

    # הרצת המערכת האוטומטית
    print("\n🚀 מריץ את המערכת האוטומטית...")
    system_script = os.path.join(current_dir, 'automatic_pension_system_v3.py')

    try:
        result = subprocess.run(['python', system_script],
                              cwd=current_dir,
                              capture_output=True,
                              text=True,
                              encoding='utf-8')

        print("📋 פלט המערכת:")
        print(result.stdout)

        if result.stderr:
            print("❌ שגיאות:")
            print(result.stderr)

        if result.returncode == 0:
            print("✅ המערכת הושלמה בהצלחה!")
        else:
            print(f"❌ המערכת נכשלה עם קוד שגיאה: {result.returncode}")

    except Exception as e:
        print(f"❌ שגיאה בהרצת המערכת: {str(e)}")

if __name__ == "__main__":
    run_complete_system()
