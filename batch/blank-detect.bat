set /p channel_value=<C:\Users\user\Desktop\scripts\channel_value.txt
set /p device_id=<C:\Users\user\Desktop\scripts\device_id.txt
del "C:\Users\User\Desktop\scripts\logs\blackDetect*"
cd "C:\Users\user\Desktop\scripts\python"
python blackFramesDetect.py %channel_value% %device_id%