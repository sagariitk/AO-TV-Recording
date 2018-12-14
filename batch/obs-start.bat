taskkill/F /IM obs64.exe
set /p channel_value=<C:\Users\user\Desktop\scripts\channel_value.txt
set /p device_id=<C:\Users\user\Desktop\scripts\device_id.txt
echo %ChannelName%
cd "C:\Program Files\obs-studio\bin\64bit"
start "" obs64.exe --startrecording
echo %channel_value% %date% %time% "recording started" >> C:\Users\user\Desktop\scripts\logs\logs.txt
cd "C:\Users\user\Desktop\scripts\python"
python request_id.py %channel_value%
python barc_database_new.py 1.0 %channel_value% %device_id%
