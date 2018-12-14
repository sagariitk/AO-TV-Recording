set /p channel_value=<C:\Users\user\Desktop\scripts\channel_value.txt
set /p device_id=<C:\Users\user\Desktop\scripts\device_id.txt
cd "C:\Users\user\Desktop\scripts\python"
QPROCESS "obs64.exe">NUL
if "%ERRORLEVEL%"=="0" ( 
	echo Program is running
) else ( 
	python barc_database_new.py 7.0 %channel_value% %device_id% && cd "C:\Program Files\obs-studio\bin\64bit" && start "" obs64.exe --startrecording && echo %channel_value% %date% %time% "recording retried" >> C:\Users\user\Desktop\scripts\logs\logs.txt
)
