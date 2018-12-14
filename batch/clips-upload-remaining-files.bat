set /p channel_value=<C:\Users\user\Desktop\scripts\channel_value.txt
set /p device_id=<C:\Users\user\Desktop\scripts\device_id.txt
cd "C:\Users\user\Desktop\scripts\python"
python clips_upload_remaining_files.py 1.0 %channel_value% %device_id%
