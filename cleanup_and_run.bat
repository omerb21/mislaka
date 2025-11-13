@echo off
echo ניקוי קבצים ישנים והשארת הקובץ הנקי בלבד...
echo ===========================================

REM מחיקת כל קבצי האקסל הישנים
del *.xlsx 2>nul

echo.
echo הרצת המערכת הנקייה...
python clean_pension_system.py

echo.
echo הקובץ הנקי נוצר: clean_pension_balances.xlsx
echo כל הקבצים הישנים נמחקו.
echo.
pause
