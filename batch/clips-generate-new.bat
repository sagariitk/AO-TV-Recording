set /p channel_value=<C:\Users\user\Desktop\scripts\channel_value.txt
set /p device_id=<C:\Users\user\Desktop\scripts\device_id.txt
del "D:\1_clips\temp*"
cd "C:\Users\user\Desktop\scripts\python"
python clips_generate.py 1.0 %channel_value% %device_id%
move "D:\1\*" "D:\1_mkv"

