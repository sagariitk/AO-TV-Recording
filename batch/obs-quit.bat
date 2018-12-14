set /p channel_value=<C:\Users\user\Desktop\scripts\channel_value.txt
set /p device_id=<C:\Users\user\Desktop\scripts\device_id.txt
taskkill/F /IM obs64.exe
echo %channel_value% %date% %time% "recording stopped" >> C:\Users\User\Desktop\scripts\logs\logs.txt
cd "C:\Users\user\Desktop\scripts\python"
python barc_database_new.py 6.0 %channel_value% %device_id%