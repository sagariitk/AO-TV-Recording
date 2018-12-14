#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "Sagar Yadav"
__copyright__ = "Copyright (©) 2018. Athenas Owl. All rights reserved."
__credits__ = ["Quantiphi Analytics"]

# python dependencies
import sys
import time
import socket
import datetime
import configparser
import pymysql



file1 = open("request_id.txt", "r") 
request_id = file1.readline()
file1.close()

stage_number = sys.argv[1] # e.g 1.0
channel_value = sys.argv[2] #e.g zeetv
print(channel_value)
device_id = sys.argv[3]


config = configparser.ConfigParser()
config.read('C:/Users/user/Desktop/scripts/parameters.ini')
stage = 'dev'
db = pymysql.connect(host=config.get(stage,'DB_HOST'),  # your host
						user=config.get(stage,'DB_USERNAME'),  # username
						passwd=config.get(stage,'DB_PASSWORD'),  # password
						db=config.get(stage,'DB_NAME'))  # database name     

def sys1():


	clip_path = 'NULL' # e.g gs://b-ao-recording-data//////barc_week1_zeetv_2018-07-12_12-00-05
	video_path = 'NULL'
	clip_number = 0
	clip_slot = 'NULL'
	timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	end_timestamp = 'NULL'
	error_message = 'NULL'
	blank_flag = 'NULL'
	corrupt_flag = 'NULL'
	clip_size = 'NULL'
	clip_duration = 'NULL'
	start_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	count = 0
	# condition for stage 1.0
	if (stage_number == '1.0'):

		status = 'Start' # e.g DONE or InProgress
		# insert stage 1 in recording table
		insert_into_recording(device_id, request_id, channel_value, stage_number, "Start Recording", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), video_path, clip_number, clip_slot)
		
		# insert stage 1 in recording_tracking table
		#insert_into_recording_tracking(device_id, request_id, channel_value, clip_path, "InProgress", start_timestamp, 'NULL', error_message, clip_slot, clip_number)
		insert_into_recording_tracking(device_id, request_id, channel_value, clip_path, "InProgress", start_timestamp, 'NULL', error_message, clip_slot, clip_number)
		# mail_status = 'NULL'
		insert_into_recording_limit(request_id, count, device_id)
		# get_clip_number_from_recording(request_id)
	
	if (stage_number == '7.0'):

		mail_status = 'NULL'

		count = get_count_from_recording_limit(request_id)
		print(count)
		count += 1
		update_into_recording_limit(request_id, count, mail_status, device_id)

	# condition for stage 6.0
	if (stage_number == '6.0'):

		# insert stage 6 in recording table
		insert_into_recording(device_id, request_id, channel_value, stage_number, "Stop Recording", datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), video_path, clip_number, clip_slot)

		clip_duration = 'NULL'
		clip_size = 'NULL'
		status = 'InProgress'
		# update end_timestamp in recording_tracking table 
		end_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
		update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)
		#updating status in recording _tracking table
		status = 'DONE'

		update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag)
		

def insert_into_recording(device_id, request_id, channel_value, stage_number, stage_message, timestamp, video_path, clip_number, clip_slot):

	# Prepare SQL query to INSERT a record into the database.
	sql = """INSERT INTO recording (device_id, request_id, channel_value, stage_number, stage_message, timestamp, video_path, clip_number, clip_slot) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
		device_id, request_id, channel_value, stage_number, stage_message, timestamp, video_path, clip_number, clip_slot)
	print(sql)
	db_operation(sql)

def insert_into_invalid_frame_tracking(device_id, request_id, invalid_frame_from, invalid_frame_to):
	# Prepare SQL query to Insert a record into the database
	sql = """INSERT INTO invalid_frame_tracking (device_id, request_id, invalid_frame_from, invalid_frame_to) VALUES ('%s','%s','%s','%s')""" % (device_id,request_id,invalid_frame_from,invalid_frame_to)
	print(sql)
	db_operation(sql)
	
def insert_into_recording_limit(request_id, count, device_id):

	# Prepare SQL query to INSERT a record into the database.
	sql = """INSERT INTO recording_limit(request_id, count, device_id) VALUES ('%s', '%s', '%s')""" % (request_id, count, device_id)
	print(sql)
	db_operation(sql)

def get_clip_number_from_recording(request_id):

	# Prepare SQL query to INSERT a record into the database.
	sql = """SELECT clip_number from recording where request_id ='%s'""" % (request_id)
	print(sql)
	return db_operation(sql)

def get_count_from_recording_limit(request_id):

	# Prepare SQL query to INSERT a record into the database.
	sql = """SELECT count from recording_limit where request_id ='%s'""" % (request_id)
	print(sql)
	print(type(db_operation(sql)))
	return db_operation(sql)

def update_into_recording_limit(request_id, count, mail_status, device_id):
	# Prepare SQL query to Update a video path 
	sql = """UPDATE recording_limit SET count= '%s' WHERE request_id ='%s' and device_id = '%s' """ %(count, request_id, device_id)
	# db_operation(sql)
	# sql = """UPDATE recording_limit SET mail_status= '%s' WHERE request_id ='%s' """ %(mail_status, request_id)
	print("mail_status changed")
	print(sql)
	db_operation(sql)


def insert_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number):
	if( start_timestamp == 'NULL'):
		# Prepare SQL query to INSERT a record into the database.
		sql = """INSERT INTO recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')""" % (
			device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number)
		print(sql)
		print('Query Executed')
	elif( end_timestamp == 'NULL'):
		# Prepare SQL query to INSERT a record into the database.
		sql = """INSERT INTO recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')""" % (
			device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number)
		print(sql)
		print('Query Executed')
	elif( start_timestamp == 'NULL' and end_timestamp == 'NULL'):
		# Prepare SQL query to INSERT a record into the database.
		sql = """INSERT INTO recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s')""" % (
			device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number)
		print(sql)
		print('Query Executed')
	else:
		# Prepare SQL query to INSERT a record into the database.
		sql = """INSERT INTO recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %s '%s', '%s', '%s')""" % (
			device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number)
		print(sql)
		print('Query Executed')
	db_operation(sql)

def update_into_recording_tracking(device_id, request_id, channel_value, clip_path, status, start_timestamp, end_timestamp, error_message, clip_slot, clip_number, clip_size, clip_duration, blank_flag, corrupt_flag):

		
	if(end_timestamp == 'NULL' and clip_path != 'NULL'):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET clip_path= '%s' WHERE request_id ='%s' and device_id = '%s' """ %(clip_path, request_id, device_id)
		print("clip path changed")
		print(sql)
		db_operation(sql)

	elif(end_timestamp == 'NULL' and clip_duration != 'NULL'):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET clip_duration=%s WHERE request_id ='%s' and device_id ='%s' """ %(clip_duration, request_id, device_id)
		print("clip duration changed")

		print(sql)
		db_operation(sql)
	elif(end_timestamp == 'NULL' and blank_flag != 'NULL'):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET blank_flag=%s WHERE request_id ='%s' and device_id ='%s' """ %(blank_flag, request_id, device_id)
		print("clip duration changed")

		print(sql)
		db_operation(sql)

	elif(end_timestamp == 'NULL' and clip_size != 0):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET clip_size=%s WHERE request_id ='%s' and device_id = '%s'""" %(clip_size, request_id, device_id)
		print("clip size changed")
		
		print(sql)
		db_operation(sql)

	elif(end_timestamp == 'NULL' and clip_slot != 'NULL'):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET clip_slot='%s' WHERE request_id ='%s' and device_id = '%s'""" %(clip_slot, request_id, device_id)
		print("clip slot changed")
		print(sql)
		db_operation(sql)

	elif(end_timestamp != 'NULL' and status != 'DONE'):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET end_timestamp= '%s' WHERE request_id ='%s' and device_id = '%s' """ %(end_timestamp, request_id, device_id)
		print("end timestamp changed")
		
		print(sql)
		db_operation(sql)

	elif(status == 'DONE'):
		# Prepare SQL query to Update a video path 
		sql = """UPDATE recording_tracking SET status='%s' WHERE request_id ='%s' and device_id = '%s'""" %(status, request_id, device_id)
		db_operation(sql)

	else :
		print("nothing to change")

def get_status_from_recording_tracking(request_id):

	# Prepare SQL query to INSERT a record into the database.
	sql = """SELECT status from recording_tracking where request_id ='%s'""" % (request_id)
	print(sql)
	# print(type(db_operation(sql)))
	return db_operation(sql)

def db_operation(sql):
	# prepare a cursor object using cursor() method
	cursor = db.cursor()

	try:
		# Execute the SQL command
		cursor.execute(sql)
		# Commit your changes in the database
		db.commit()
		return cursor.fetchall()
		#return cursor.fetchall()[0][0]
	except Exception as e :
		print(e)
		# Rollback in case there is any error
		db.rollback()
def db_operation_return_integer(sql):
	# prepare a cursor object using cursor() method
	cursor = db.cursor()
	try:
		# Execute the SQL command
		temp = cursor.execute(sql)
		# Commit your changes in the database
		return temp
		#return cursor.fetchall()[0][0]
	except Exception as e :
		print(e)
		# Rollback in case there is any error
		db.rollback()	

if __name__ == '__main__':
	sys1()
