import os
import datetime
from os import listdir
from os.path import isfile, join
import sys
import uuid
import time
from subprocess import check_output
import barc_database_new as barc_database

#input parameters
stage_number = sys.argv[1]
channel_name = sys.argv[2]
device_id = sys.argv[3]
channel_value = channel_name
channel_mkv_name=channel_name + "_mkv"
channel_clips_name=channel_name + "_clips"
channel_clips_temp_name=channel_name + "_temp"

#path of the folder in local pc
file_path="D:\\" + channel_name
file_mkv_path="D:\\" + channel_mkv_name
file_clips_path="D:\\" + channel_clips_name
# file_clips_temp_path="D:\\" + channel_clips_temp_name

#list of videos in ChannelName folder
video_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

#loop for interating through the videos in ChannelName folder
for video in video_list:
    print(video)
    #rename of the file
    if(video[0]=='2'):
        start_time = 0
        clip_no = 0
        previous_clip_length = 0

        clip_name_date = video.split('_')[0].split('.')[0]
        clip_name_start_time = (time.ctime(os.path.getctime(file_path + os.path.sep + video))).split(' ')[3]
        
        try:
            if(int(clip_name_start_time) >=1 and int(clip_name_start_time) <=9):
                clip_name_start_time = (time.ctime(os.path.getctime(file_path + os.path.sep + video))).split(' ')[4]
        except:
            clip_name_start_time = (time.ctime(os.path.getctime(file_path + os.path.sep + video))).split(' ')[3]
        # print(clip_name_start_time)
        clip_name_date = datetime.datetime.strptime(clip_name_date, '%Y-%m-%d').strftime('%Y%m%d')
        clip_name_start_time = datetime.datetime.strptime(clip_name_start_time, '%H:%M:%S').strftime('%H%M%S.%f')[0:-3]
        clip_name_start_time_seconds = datetime.datetime.strptime(clip_name_start_time, '%H%M%S.%f')
        clip_length = datetime.timedelta(seconds=0)
        clip_name_end_time_seconds = clip_name_start_time_seconds + clip_length
        clip_name_end_time = clip_name_end_time_seconds.strftime('%H%M%S.%f')[0:-3]

        print(clip_name_start_time)
        print(clip_name_end_time)
        unique_day_request_id_prefix = clip_name_date + "_" + channel_value + "_"
        try:
            while clip_no < 12:       
                print(clip_no)
                clip_name_temp = "temp%d.mp4"
                
                # device_id = 'primary'
                request_id = clip_name_date + "_" + channel_value + "_" + clip_name_end_time # "_clip" + str(clip_no + 1)
                print("here")
                timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                start_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                end_timestamp = 'NULL'
                error_message = 'NULL'
                clip_path = 'NULL'
                clip_number = clip_no + 1
                clip_slot = 'NULL'
                blank_flag = 'NULL'
                corrupt_flag = 'NULL'
                clip_size = 0
                #TODO : here
                sql_row_count_operation = """ select * from recording_tracking where request_id like '%{0}%' and device_id = '{1}' and clip_number = {2} and clip_slot != 'NULL' """ .format(unique_day_request_id_prefix, device_id, clip_number)
                is_rows_present = barc_database.db_operation_return_integer(sql_row_count_operation)
                print("rows: %d" %is_rows_present)
                print(request_id)
                if not is_rows_present:
                    #inserting 2nd stage in recording table
                    barc_database.insert_into_recording(device_id, request_id, channel_value, 2.0, 'Clipping Started', timestamp, clip_path, clip_number, clip_slot)
                print(device_id, request_id, channel_value, start_timestamp, blank_flag)
                # insert stage 1 in recording_tracking table
                sql_row_count_operation = """ select * from recording_tracking where request_id like '{0}%' and device_id = '{1}' and clip_number = {2} and clip_slot = 'NULL' """ .format(unique_day_request_id_prefix, device_id, clip_number)
                if not is_rows_present:
                    if not barc_database.db_operation_return_integer(sql_row_count_operation):           # this condition is for avoiding re-entry for any tuple present with NULL slot
                        barc_database.insert_into_recording_tracking(device_id, request_id, channel_value, clip_path, 'InProgress', start_timestamp, end_timestamp, error_message, clip_slot, clip_number)                
                # clipping of 30 min
                command_for_ffmpeg = "ffmpeg -i " + file_path + os.path.sep + video + " -c copy -map 0 -segment_time " + "1800" + " -f segment -reset_timestamps 1 " + file_clips_path + os.path.sep + clip_name_temp
                print(command_for_ffmpeg)
                os.system(command_for_ffmpeg)

                clip_old_name = "temp" + str(clip_no) + ".mp4"

                #getting the length of current clip
                current_clip_length = float(str(check_output('powershell.exe ffprobe -i \"' + file_clips_path + os.path.sep + clip_old_name + '\" -show_entries format=duration -v quiet -of csv=\'p=0\'')).split('\'')[1].split('\\')[0])
                print(current_clip_length)
                
                #updating clip_name_start_time and clip_name_end_time
                clip_name_start_time_seconds = datetime.datetime.strptime(clip_name_start_time, '%H%M%S.%f')
                clip_length = datetime.timedelta(seconds = previous_clip_length)
                clip_name_start_time_seconds = clip_name_start_time_seconds + clip_length
                clip_name_start_time = clip_name_start_time_seconds.strftime('%H%M%S.%f')[0:-3]

                clip_name_end_time_seconds = datetime.datetime.strptime(clip_name_end_time, '%H%M%S.%f')
                clip_length = datetime.timedelta(seconds=current_clip_length)
                clip_name_end_time_seconds = clip_name_end_time_seconds + clip_length
                clip_name_end_time = clip_name_end_time_seconds.strftime('%H%M%S.%f')[0:-3]

                #renaming the file
                rename_file_name = clip_name_date + "_" + channel_name + "_" + clip_name_start_time + "_" + clip_name_end_time + ".mp4"
                command_for_rename = "ren \"" + file_clips_path + os.path.sep + clip_old_name + "\" \"" + rename_file_name + "\""
                rename_status = os.system(command_for_rename)

                #doing next step only when renaming is done properly
                if(rename_status == 0):
                    
                    clip_slot = clip_name_start_time + "_" + clip_name_end_time

                    #inserting 3rd stage in recording table
                    barc_database.insert_into_recording(device_id, request_id, channel_value, 3.0, "Clipping Done", timestamp, clip_path, clip_number, clip_slot)
                    
                    clip_size = 0    
                    clip_duration = 'NULL'
                    status = 'InProgress'
                    # update clip_slot in recording_tracking table
                    barc_database.update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)


                    clip_duration = current_clip_length/60
                    print(clip_duration)
                    clip_size = 0
                    # update clip_duration in recording_tracking table 
                    barc_database.update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)

                previous_clip_length = current_clip_length  
                print("clip_generated")
                clip_no += 1
                time.sleep(1800)

        except:
            print("video is not of 6 hours")
    else:
       continue
    #moving of mkv file when all the clips are done
    command_for_move="move \"" + file_path + os.path.sep + video + "\" \"" + file_mkv_path + "\""
    os.system(command_for_move)
    print("video_completed")


            # print(start_time)
            # file1 = open("start_time.txt","w+")
            # file1.writelines(str(start_time))
            # file1.close()

            # file1 = open("start_time.txt", "r")
            # start_time = int(file1.readline())
            # file1.close()


