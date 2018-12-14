#!/usr/bin/python
import os
import datetime
from os import listdir
from os.path import isfile, join
import sys
import uuid
from subprocess import check_output
import time
import calendar
from datetime import timedelta

import barc_database_new

# input parameters
stage_number = sys.argv[1]
channel_name = sys.argv[2]
device_id = sys.argv[3]
channel_value = channel_name
channel_mkv_name = channel_name + "_mkv"
# channel_clips_name = channel_name + "_clips"
channel_clips_name = "unuploaded"

project="athenas-owl-barc"
bucket_name="gs://b-ao-recording-data"

# path of the folder in local pc
file_path = "D:\\" + channel_name
file_mkv_path = "D:\\" + channel_mkv_name
file_clips_path = "D:\\" + channel_clips_name

# list of videos in ChannelName folder
clip_list = [f for f in listdir(file_clips_path) if isfile(join(file_clips_path, f))]

# loop for interating through the videos in ChannelName folder
count = 0
while count < 60 :

	for clip in clip_list:

		if (clip.split('.')[0] == 'uploading'):
			continue
		try:
			index = clip.split('_')[1][0]
			mp4_index = clip.split('.')[3]
			# print("not a temp file")
		except:
			index = " "
			mp4_index = " "
			# print("it is a temp file")

		# only upload of the video files with .mp4 extension
		clip_uploaded = False
		if(mp4_index =='mp4' and index == channel_name[0]):

			clip_date = clip.split('_')[0].split('.')[0]
			clip_date = datetime.datetime.strptime(clip_date, '%Y%m%d').strftime('%Y-%m-%d')
			clip_date = datetime.datetime.strptime(clip_date, '%Y-%m-%d').date()
			week_day = calendar.day_name[clip_date.weekday()]
			if week_day == 'Monday':
				week_start = clip_date + timedelta(days=-2)
				week_end = clip_date + timedelta(days=4)
			elif week_day == 'Tuesday':
				week_start = clip_date + timedelta(days=-3)
				week_end = clip_date + timedelta(days=3)
			elif week_day == 'Wednesday':
				week_start = clip_date + timedelta(days=-4)
				week_end = clip_date + timedelta(days=2)
			elif week_day == 'Thursday':
				week_start = clip_date + timedelta(days=-5)
				week_end = clip_date + timedelta(days=1)
			elif week_day == 'Friday':
				week_start = clip_date + timedelta(days=-6)
				week_end = clip_date + timedelta(days=0)
			elif week_day == 'Saturday':
				week_start = clip_date + timedelta(days=0)
				week_end = clip_date + timedelta(days=6)
			else:
				week_start = clip_date + timedelta(days=-1)
				week_end = clip_date + timedelta(days=5)

			clip_date = str(datetime.datetime.strptime(str(clip_date), '%Y-%m-%d').strftime('%Y%m%d'))
			week_start = str(datetime.datetime.strptime(str(week_start), '%Y-%m-%d').strftime('%Y%m%d'))
			week_end = str(datetime.datetime.strptime(str(week_end), '%Y-%m-%d').strftime('%Y%m%d'))
		

			clip_old_name = clip
			# calculating video length
			clip_length = int(float(str(check_output('powershell.exe ffprobe -i \"' + file_clips_path + os.path.sep + clip_old_name + '\" -show_entries format=duration -v quiet -of csv=\'p=0\'')).split('\'')[1].split('\\')[0]))

			# rename to uploading
			clip_temp_name = "uploading." + clip
			command_for_rename="ren \"" + file_clips_path + os.path.sep + clip_old_name + "\" \"" + clip_temp_name + "\""
			os.system(command_for_rename)

			# path for video uploading
			upload_path = bucket_name + "/" + week_start + "_" + week_end + "/" + clip_date + "/" + channel_name + "/" + device_id + "/" + clip.split('_')[2] + "_" + clip.split('_')[3].split('.')[0] + "." + clip.split('_')[3].split('.')[1] + "/video/" + clip_old_name.split('.mp4')[0] + "/"
	   
			request_id = clip.split('_')[0] + "_" + clip.split('_')[1] + "_" + clip.split('_')[2]
			timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
			start_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
			error_message='NULL'
			migration_status='NULL'
			prediction_status='NULL'
			end_timestamp='NULL'
			status = 'InProgress'
			clip_path = "NULL"
			corrupt_flag='NULL'
			# clip_number = 0
			clip_slot = clip.split('_')[2] + "_" + clip.split('_')[3].split('.mp4')[0]
			
			clip_old_status = barc_database_new.get_status_from_recording_tracking(request_id)
			clip_status_db = False
			print(clip_old_status)
			for i in clip_old_status:
				print(i[0])
				if(i[0]=='DONE'):
					clip_status_db = True

					# break
			if(clip_status_db == True):
				continue
			print(clip)
			print(clip_status_db)
			time.sleep(5)
			
				
			clip_number = barc_database_new.get_clip_number_from_recording(request_id)[0][0]
			print(clip_number)

			#checking blank frames
			# cmd = """ffmpeg -i file_clips_path + os.path.sep + clip_temp_name -vf "blackdetect=d=2:pix_th=0.00" -an -f null - 2>&1 -hide_banner -nostats | findstr black_start > out.txt"""
			cmd = "ffmpeg -i " + file_clips_path + os.path.sep + clip_temp_name + " -vf \"blackdetect=d=2:pix_th=0.1\" -an -f null - 2>&1 -hide_banner -nostats | findstr black_start > out.txt"
			os.system(cmd)
			with open('out.txt') as myfile:
				list1 = myfile.read()
			print(list1)
			if list1:
				list1 = list1.split(" ")
				start = list1[3].split(":")
				end = list1[4].split(":")
				black_start = start[1]
				black_end = end[1]
			#inserting 4th stage in recording table
			barc_database_new.insert_into_recording(device_id, request_id, channel_value, 4.0, "Uploading start", timestamp, clip_path, clip_number, clip_slot)
			
			print("uploading started")
			# uploading of file to gcp bucket
			command_for_upload="gsutil -o GSUtil:parallel_composite_upload_threshold=1M cp " + "\"" + file_clips_path + os.path.sep + clip_temp_name + "\" \"" + upload_path + clip_old_name + "\""
			os.system(command_for_upload)
				
			# to check the file in gcp bucket
			command_for_status="gsutil stat " + upload_path + clip_old_name
			status=os.system(command_for_status)

			if(status == 0):

				video_path = upload_path + clip_old_name

				clip_duration = 'NULL'
				clip_size = 0

				import subprocess
				command_for_clip_size = "gsutil du " + video_path
				clip_size = float(str(subprocess.check_output(command_for_clip_size, shell=True)).split('\'')[1].split(' ')[0])
				print(clip_size)
				clip_size /= 1073741824
				print(clip_size)

				#updating blank frames in recording_tracking table
				if list1:
					blank_flag = 1
					invalid_frame_from = black_start
					invalid_frame_to = black_end
					barc_database_new.insert_into_invalid_frame_tracking(device_id, request_id, invalid_frame_from, invalid_frame_to)
				else:
					blank_flag = 'NULL'
				
				# update clip_size in recording_tracking table    
				barc_database_new.update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)

				clip_path = upload_path + clip_old_name
				#inserting 5th stage in recording table
				barc_database_new.insert_into_recording(device_id, request_id, channel_value, 5.0, "Uploading done", timestamp, clip_path, clip_number, clip_slot)
				
				# update clip_path in recording_tracking table    
				barc_database_new.update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)
				
				# update end_timestamp in recording_tracking table    
				end_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
				barc_database_new.update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)

				# update status in recording_tracking table  
				status = 'DONE'
				barc_database_new.update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)

				# reanme back to original name before moving to uploaded
				command_for_rename="ren \"" + file_clips_path + os.path.sep + clip_temp_name + "\" \"" + clip_old_name + "\""
				os.system(command_for_rename)

				# after uploading move to uploaded folder
				command_for_move="move \"" + file_clips_path + os.path.sep + clip_old_name + "\" D:\\" + "uploaded"
				os.system(command_for_move)
			else:
				# reanme back to original name
				command_for_rename="ren \"" + file_clips_path + os.path.sep + clip_temp_name + "\" \"" + clip_old_name + "\""
				os.system(command_for_rename)
		#break

	print("one clip uploaded")
	# time.sleep(600)	
	clip_list = [f for f in listdir(file_clips_path) if isfile(join(file_clips_path, f))]	
	count += 1
	break		
				
