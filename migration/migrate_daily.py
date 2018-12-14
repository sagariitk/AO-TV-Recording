from __future__ import print_function
__author__ = "Darshit Suratwala"

#__credits__ = ["Quantiphi Analytics"]

# create columns containing decimals with 2 floating points
# change line 203 for deployment

# python dependencies
import sys
import json
import time
import logging
import os
import socket
import time
import ConfigParser
from datetime import datetime
import ast
import datetime
from decimal import *
# database dependencies
import MySQLdb
#email specific dependencies
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
# Install using pip install --upgrade google-api-python-client
import googleapiclient.discovery


class ClientMethods:
    """
    ClientMethods class contains collection of methods which are used for migration
    """
    cf_endpoint = "https://us-central1-athenas-owl-dev.cloudfunctions.net/cf-send-mail-generic"
    receiver = "darshitkumar.suratwala@quantiphi.com,bhavesh.patel@quantiphi.com"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # create a file handler
    handler = logging.FileHandler('/var/.ao/logfiles/migrater.log')
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    config = ConfigParser.ConfigParser()

    # variables
    number_of_rows_invalid_frame_tracking = 0
    number_of_rows_recording = 0
    number_of_rows_recording_limit = 0
    number_of_rows_recording_tracking = 0
    count_number_of_rows_invalid_frame_tracking = 0
    count_number_of_rows_recording = 0
    count_number_of_rows_recording_limit = 0
    count_number_of_rows_recording_tracking = 0

    def db_operation(self, sql_query, cursor,db):
        """
        this function will be used to do any operation on a database
        inputs required.....
        sql_query is your query string 
        cursor is the db.cursor() object
        db is your db connection object
        """
        # prepare a cursor object using cursor() method
        #cursor = self.db_online.cursor()
        try:
            # Execute the SQL command
            cursor.execute(sql_query)
            # Commit your changes in the database
            db.commit()
            return cursor.fetchall()

        except:
            # Rollback in case there is any error
            db.rollback()
            return False

    #its used to get today's and tomorrow's date
    def get_date(self):
        ts = time.time() #today's time
        date_today_utc = datetime.datetime.utcfromtimestamp(ts).date()    # time to utc datetime format
        date_tomorrow_utc = datetime.date.today() + datetime.timedelta(days=1)    # calculating tomorrow's date
        date_today_ist = datetime.date.today()  # time to utc datetime format
        #today = str(today).split(" ")[0]
        #tomorrow = str(tomorrow).split(" ")[0]
        if date_today_ist == date_today_utc:  # condition for changing date back to previous day when day changes at 5:30AM IST
            date_tomorrow_utc = date_today_utc
            date_today_utc = date_today_ist - datetime.timedelta(days=1)
        print("Today: %s \nTomorrow: %s\n %s" %(date_today_utc, date_tomorrow_utc,date_today_ist))
        self.logger.info("IST Today: %s\n UTC Today: %s\nUTC Tomorrow: %s" %(date_today_ist, date_today_utc, date_tomorrow_utc))
        #return format 2018-12-31
        # date_today_utc = '2018-11-28'
        # date_tomorrow_utc = '2018-11-29'
        date_today_utc= str(date_today_utc).split(" ")[0]
        date_tomorrow_utc= str(date_tomorrow_utc).split(" ")[0]
        return date_today_utc,date_tomorrow_utc

    def get_timestamp(self,timestamp):
        """
        takes in timestamp as an input and 
        returns the timestamp in string datatype
        """
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')

    def get_row_column(self, row_values, db_columns):
        """
        input params.....  
        db_columns -> colums present in that table
        row_values -> all the column values on that row
        return.....
        values -> only values which are to be inserted (removing null values)
        columns -> the column names in which 'values' to be inserted
        """
        columns = []
        values = []
        for i in range(len(row_values)):
            if row_values[i] is not None:
                if 'datetime.datetime' in str(type(row_values[i])): #convert timestamp to str
                    values.append(self.get_timestamp(row_values[i]) )
                elif 'long' in str(type(row_values[i])): #convert long to int (reason: when we use long variable it gets 'L' appended in the query string )
                    values.append(int(row_values[i]) )
                elif 'decimal' in str(type(row_values[i])): # convert decimal to float
                    values.append(float(row_values[i]))
                #elif columns[i] == 'request_id':
                    #values.append(listvar[tuplevar][i].replace(' ', '_'))
                else:   
                    values.append(row_values[i]) # works for varchar and text
                columns.append(db_columns[i])   #lastly appending the required columns
        return tuple(values),tuple(columns)

    def send_email(self, email_output):
        if len(email_output) < 1:
            print("No email to be sent")
            self.logger.info("No email to be sent")
        else:
            self.logger.info("Trying to send email.....")
            data = {}
            data["message"] = self.generate_email_message(email_output)
            data["receiver"] = self.receiver
            data["subject"] = "BARC : %d ROWS WITH ISSUE DURING MIGRATION" %len(email_output)
            data["key"] = "AOPlatform"
            headers = {
            'content-type': "application/json"
            }
            response = requests.request("POST", self.cf_endpoint, data=json.dumps(data), headers=headers)
            self.logger.info("Email sucessfully sent, yay!")

    def send_status_email(self):
        self.logger.info("Trying to send status email.....")
        data = {}
        data["message"] = self.generate_status_email_message()
        data["receiver"] = self.receiver
        data["subject"] = "BARC : MIGRATION STATUS"
        data["key"] = "AOPlatform"
        headers = {
        'content-type': "application/json"
        }
        response = requests.request("POST", self.cf_endpoint, data=json.dumps(data), headers=headers)
        self.logger.info("Email sucessfully sent, yay!")

    def generate_email_message(self, email_output):
        message_body = "<div style='overflow-x:auto, text-align:center'><table border=8>"
        columns_to_display =['device_id', 'request_id', 'channel_name', 'status', 'start_timestamp', 'end_timestamp', 'error_message', 'clip_number']
        for header in columns_to_display: #adding column names
            message_body += "<th>%s</th>" %header
        for tuple_number in range(len(email_output)):
            message_body += "<tr>"
            for iter_column in range(len(email_output[1])):
                message_body += "<th>%s</th>" %email_output[tuple_number][iter_column]
            message_body += "</tr>"
        message_body += "</table></div>"
        return message_body

    def generate_status_email_message(self):
        message_body = "<div style='overflow-x:auto, text-align:center'><table border=8>"
        message_body += "<th>Table Name</th><th>rows to be migrated</th><th>migrated</th>"
        message_body += "<tr><td>%s</td><td>%d</td><td>%d</td></tr>" %("recording_tracking", self.number_of_rows_recording_tracking, self.count_number_of_rows_recording_tracking)
        message_body += "<tr><td>%s</td><td>%d</td><td>%d</td></tr>" %("invalid_frame_tracking", self.number_of_rows_invalid_frame_tracking, self.count_number_of_rows_invalid_frame_tracking)
        message_body += "<tr><td>%s</td><td>%d</td><td>%d</td></tr>" %("recording_limit", self.number_of_rows_recording_limit, self.count_number_of_rows_recording_limit)
        message_body += "<tr><td>%s</td><td>%d</td><td>%d</td></tr>" %("recording", self.number_of_rows_recording, self.count_number_of_rows_recording)
        return message_body
    
    def generate_logs(self, cursor, db, today):
        req_id = today.replace("-", "")
        for device in ['a','b']:
            for channels in range(1,6):            
                sql_query = """ select * from recording_tracking where request_id like '{0}_{2}%' AND device_id = '{1}'; """.format(req_id,device, channels)
                migration_output = self.db_operation(sql_query, cursor,db)
                number_of_rows = len(migration_output)
                self.logger.info("device %d%s: %d" % (channels,device,number_of_rows))
    def get_daily_report(self, cursor_online,db_online, date):
        sql_query = "select * from daily_report where date = '%s' " %(date)
        query_response = self.db_operation(sql_query, cursor_online, db_online)
        if len(query_response) == 0:
            self.logger.info("no report entries found: %s" %len(query_response))
            return 1
        else:
            self.logger.info("report entries found: %s" %len(query_response))
            return 0

    def update_daily_report(self, cursor_online,db_online, date):
        db_columns = ["date", "status", "ack"]
        # insert into daily_report (date,status) values ('2018-5-17',1);
        sql_query = "insert into daily_report (%s,%s) values ('%s',%s) ;" %(db_columns[0], db_columns[1], date, '1')
        self.db_operation(sql_query, cursor_online,db_online)
        self.logger.info("report updated for the date: %s" %date)
        print("report updated for the date: %s" %date)

    def operation_recording_tracking(self, cursor, db, today, tomorrow):
        # selecting data which is to be migrated
        req_id = today.replace("-", "")
        sql_query = """ select * from recording_tracking where request_id like '{}_%' AND migration_status is NULL; """.format(req_id)
        migration_output = self.db_operation(sql_query, cursor,db)
        number_of_rows = len(migration_output)
        print("Number of rows to be migrated for recording_tracking: %d" % number_of_rows)
        self.logger.info("Number of rows to be migrated recording_tracking: %d" % number_of_rows)
        #selecting data which is to be mailed
        # sql_query = """ select recording_tracking.device_id, recording_tracking.request_id, channel_info.channel_name, recording_tracking.STATUS, recording_tracking.start_timestamp, recording_tracking.end_timestamp, recording_tracking.error_message, recording_tracking.clip_number FROM recording_tracking inner join channel_info on recording_tracking.channel_value = channel_info.channel_value WHERE device_id = 'primary' AND clip_path = 'NULL' AND status !='DONE' AND cast(start_timestamp as date) >= '{0}' AND `start_timestamp` < '{1}' AND clip_duration is NULL AND migration_status is NULL AND clip_size is NULL """.format(today, tomorrow)
        # print(sql_query)
        # email_output = self.db_operation(sql_query, cursor,db)
        email_output = ""
        print("Number of rows to be emailed: %d" % len(email_output))
        self.logger.info("Number of rows to be emailed: %d" % len(email_output))
        return migration_output, email_output, number_of_rows
    def migrate_recording_tracking(self, cursor_online, db_online, migration_output,cursor_local, db_local):
        #TODO add exception handling
        db_columns = ['device_id', 'request_id', 'channel_value', 'clip_path', 'status', 'start_timestamp', 'end_timestamp', 'error_message', 'clip_slot', 'clip_number','clip_size', 'clip_duration', 'migration_status', 'prediction_status', 'blank_flag', 'corrupt_flag']
        for row_number in range(len(migration_output)):     #iterate each tuple
            row,column = self.get_row_column(migration_output[row_number], db_columns)
            sql_query = "INSERT INTO recording_tracking (%s) VALUES %s " %(",".join(column),row)
            temp = self.db_operation(sql_query, cursor_online,db_online)
            #update migrate status using clip_path (which will be the unique value throughout the table)
            if temp == False or temp < 1:
                client_obj.logger.info("Row #%d failed to migrate into recording_tracking" %(row_number+1))
            else:
                my_primary_key = migration_output[row_number][0] # fetching device_id
                my_primary_key_1 = migration_output[row_number][1] # fetching request_id
                my_primary_key_2 = migration_output[row_number][5] # fetching start_timestamp
                my_primary_key_3 = migration_output[row_number][4] # fetching status
                sql_query = """UPDATE recording_tracking SET migration_status = 1 WHERE device_id = '{0}' and request_id = '{1}' and start_timestamp = '{2}' and status = '{3}' """ .format(my_primary_key, my_primary_key_1, my_primary_key_2, my_primary_key_3)
                temp = self.db_operation(sql_query, cursor_local, db_local)
                self.count_number_of_rows_recording_tracking += 1
                client_obj.logger.info("Row #%d migrated to recording_tracking" %(row_number+1))
                print(sql_query)
    
    def operation_recording_limit(self,cursor, db, today):
        # selecting data which is to be migrated
        req_id = today.replace("-", "")
        sql_query = """ select * from recording_limit where  migration_status is NULL and request_id like '{}'; """.format(req_id)
        migration_output = self.db_operation(sql_query, cursor,db)
        number_of_rows = len(migration_output)
        print("Number of rows to be migrated for recording_limit: %d" % number_of_rows)
        self.logger.info("Number of rows to be migrated recording_limit: %d" % number_of_rows)
        return migration_output, number_of_rows
    def migrate_recording_limit(self, cursor_online, db_online, migration_output,cursor_local, db_local):
        #TODO add exception handling
        db_columns = ['device_id', 'request_id', 'count', 'mail_status', 'migration_status']
        for row_number in range(len(migration_output)):     #iterate each tuple
            row,column = self.get_row_column(migration_output[row_number], db_columns)
            sql_query = "INSERT INTO recording_limit (%s) VALUES %s " %(",".join(column),row)
            temp = self.db_operation(sql_query, cursor_online,db_online)
            #update migrate status using request_id and device_id (which will be the unique value throughout the table)
            if temp == False or temp < 1:
                client_obj.logger.info("Row #%d failed to migrate into recording_limit" %(row_number+1))
            else:
                my_primary_key = migration_output[row_number][0] # fetching device_id
                my_primary_key_1 = migration_output[row_number][1] # fetching request_id
                sql_query = """UPDATE recording_limit SET migration_status = 1 WHERE device_id = '{0}' and request_id = '{1}' """ .format(my_primary_key, my_primary_key_1)
                temp = self.db_operation(sql_query, cursor_local, db_local)
                self.count_number_of_rows_recording_limit += 1
                client_obj.logger.info("Row #%d migrated to recording_limit" %(row_number+1))
    
    def operation_invalid_frame_tracking(self, cursor, db, today):
        req_id = today.replace("-", "")
        sql_query = """ select * from invalid_frame_tracking where  migration_status is NULL and request_id like '{}'; """ .format(req_id)
        migration_output = self.db_operation(sql_query, cursor,db)
        number_of_rows = len(migration_output)
        print("Number of rows to be migrated for invalid_frame_tracking: %d" % number_of_rows)
        self.logger.info("Number of rows to be migrated invalid_frame_tracking: %d" % number_of_rows)
        return migration_output, number_of_rows
    def migrate_invalid_frame_tracking(self, cursor_online, db_online, migration_output,cursor_local, db_local):
        #TODO add exception handling
        db_columns = ['device_id', 'request_id', 'invalid_frame_from', 'invalid_frame_to', 'migration_status']
        for row_number in range(len(migration_output)):     #iterate each tuple
            row,column = self.get_row_column(migration_output[row_number], db_columns)
            sql_query = "INSERT INTO invalid_frame_tracking (%s) VALUES %s " %(",".join(column),row)
            temp = self.db_operation(sql_query, cursor_online,db_online)
            #update migrate status using all tuple entries(which will be the unique value throughout the table)
            if temp == False or temp < 1:
                client_obj.logger.info("Row #%d failed to migrate into invalid_frame_tracking" %(row_number+1))
            else:
                my_primary_key = migration_output[row_number][0] # fetching device_id
            my_primary_key_1 = migration_output[row_number][1] # fetching request_id
            my_primary_key_2 = migration_output[row_number][2] # fetching invalid_frame_from
            my_primary_key_3 = migration_output[row_number][3] # fetching invalid_frame_to
            sql_query = """UPDATE invalid_frame_tracking SET migration_status = 1 WHERE device_id = '{0}'and request_id = '{1}'and invalid_frame_from = '{2}'and invalid_frame_to = '{3}' """ .format(my_primary_key, my_primary_key_1, my_primary_key_2, my_primary_key_3)
            temp = self.db_operation(sql_query, cursor_local, db_local)
            self.count_number_of_rows_invalid_frame_tracking += 1
            client_obj.logger.info("Row #%d migrated to invalid_frame_tracking" %(row_number+1))
    
    def operation_recording(self, cursor, db, today):
        req_id = today.replace("-", "")
        sql_query = """ select * from recording where  migration_status is NULL and request_id like '{}'; """ .format(req_id)
        migration_output = self.db_operation(sql_query, cursor,db)
        number_of_rows = len(migration_output)
        print("Number of rows to be migrated for recording: %d" % number_of_rows)
        self.logger.info("Number of rows to be migrated recording: %d" % number_of_rows)
        return migration_output, number_of_rows
    def migrate_recording(self, cursor_online, db_online, migration_output,cursor_local, db_local):
        #TODO add exception handling
        db_columns = ['device_id', 'request_id', 'channel_value', 'stage_number', 'stage_message', 'timestamp', 'video_path', 'clip_number', 'clip_slot', 'migration_status']
        for row_number in range(len(migration_output)):     #iterate each tuple
            row,column = self.get_row_column(migration_output[row_number], db_columns)
            sql_query = "INSERT INTO archive_recording (%s) VALUES %s " %(",".join(column),row)
            temp = self.db_operation(sql_query, cursor_online,db_online)
            #update migrate status using device_id and request_id(which will be the unique value throughout the table)
            if temp == False or temp < 1:
                client_obj.logger.info("Row #%d failed to migrate into archive_recording" %(row_number+1))
            else:
                my_primary_key = migration_output[row_number][0] # fetching device_id
                my_primary_key_1 = migration_output[row_number][1] # fetching request_id
                my_primary_key_2 = migration_output[row_number][5] # fetching timestamp
                sql_query = """UPDATE recording SET migration_status = 1 WHERE device_id = '{0}' and request_id = '{1}' and timestamp = '{2}'""" .format(my_primary_key, my_primary_key_1, my_primary_key_2)
                temp = self.db_operation(sql_query, cursor_local, db_local)
                self.count_number_of_rows_recording += 1
                client_obj.logger.info("Row #%d migrated to recording" %(row_number+1))

    # for future use (when everything is easy to update)
    def generic_data_migration(self, cursor, db, migration_output, table_name, db_columns, my_primary_key, my_primary_key_index):
        for row_number in range(len(migration_output)):     #iterate each tuple
            row,column = self.get_row_column(migration_output[row_number], db_columns)
            sql_query = "INSERT INTO %s (%s) VALUES %s " %(table_name, ",".join(column),row)
            temp = self.db_operation(sql_query, cursor,db)
            #update migrate status using request_id(which will be the unique value throughout the table)
            my_primary_key = migration_output[row_number][1] # fetching request_id
            sql_query = """UPDATE recording_limit SET migration_status = 1 WHERE request_id = '{0}' """ .format(my_primary_key)
            temp = self.db_operation(sql_query, cursor, db)
            client_obj.logger.info("Row #%d migrated to recording_limit" %(row_number+1))

if __name__ == '__main__':
    client_obj = ClientMethods()

    # get today and tomorrow  
    today,tomorrow = client_obj.get_date()
    
    # setup a connection to local DB
    config = ConfigParser.ConfigParser()
    config.read('/var/.ao/parameters.ini')
    stage = 'LOCAL'
    
    try:
        db_local = MySQLdb.connect(host=config.get(stage, 'DB_HOST'),  # your host
                            user=config.get(stage, 'DB_USERNAME'),  # username
                            passwd=config.get(stage, 'DB_PASSWORD'),  # password
                            db=config.get(stage, 'DB_NAME'))
        cursor_local = db_local.cursor()
        
        ### quering local db ###
        # recording_tracking
        migration_output_recording_tracking, email_output_recording_tracking, client_obj.number_of_rows_recording_tracking = client_obj.operation_recording_tracking(cursor_local, db_local, today, tomorrow)
        # recording_limit
        migration_output_recording_limit, client_obj.number_of_rows_recording_limit = client_obj.operation_recording_limit(cursor_local, db_local, today)
        # invalid_frame_tracking
        migration_output_invalid_frame_tracking, client_obj.number_of_rows_invalid_frame_tracking = client_obj.operation_invalid_frame_tracking(cursor_local, db_local, today)
        # recording
        migration_output_recording, client_obj.number_of_rows_recording = client_obj.operation_recording(cursor_local, db_local, today)
        client_obj.generate_logs(cursor_local, db_local, today)
    except:
        print("local database connection error")
        client_obj.logger.info("Unable to connect to local Database")
    
    else: # when no error thrown in try block
        ##### Migration Logic is here #####
        # condition is true if there is any data to be migrated.
        if client_obj.number_of_rows_recording_tracking or client_obj.number_of_rows_recording_limit or client_obj.number_of_rows_invalid_frame_tracking or client_obj.number_of_rows_recording:
            try:
                # connect online DB for migration
                stage = 'BARC-PROD'
                db_online = MySQLdb.connect(host=config.get(stage, 'DB_HOST'),  # your host
                            user=config.get(stage, 'DB_USERNAME'),  # username
                            passwd=config.get(stage, 'DB_PASSWORD'),  # password
                            db=config.get(stage, 'DB_NAME'))
                cursor_online = db_online.cursor()
                client_obj.logger.info("DB CONNECTED")
            except:
                client_obj.logger.info("ERROR IN DB CONNECTION")
                print("connection error")
            else: # when connection to cloud is sucessful, start migrating.....
                #client_obj.logger.info("Data sucessfully migrated")
                # initiate migration if email is not sent (in other words when connection with Cloud DB is established)
                if client_obj.get_daily_report(cursor_online,db_local, today): # checking if entry of migration is already present
                    # for recording_tracking table
                    try:
                        if client_obj.number_of_rows_recording_tracking:
                            client_obj.migrate_recording_tracking(cursor_online, db_online, migration_output_recording_tracking,cursor_local, db_local)
                        else:
                            print("no migration required for recording_tracking")
                    except:
                        client_obj.logger.info("ERROR while migrating recording_tracking")
                        print("connection error while migrating recording_tracking")
                        
                    # for invalid_frame_tracking table
                    try:
                        if client_obj.number_of_rows_invalid_frame_tracking:
                            client_obj.migrate_invalid_frame_tracking(cursor_online, db_online, migration_output_invalid_frame_tracking,cursor_local, db_local)
                        else:
                            print("no migration required for invalid_frame_tracking")
                    except:
                        client_obj.logger.info("ERROR while migrating invalid_frame_tracking")
                        print("connection error while migrating invalid_frame_tracking")
                    
                    # for recording table 
                    try:
                        if client_obj.number_of_rows_recording:
                            client_obj.migrate_recording(cursor_online, db_online, migration_output_recording,cursor_local, db_local)
                        else:
                            print("no migration required for recording")
                    except:
                        client_obj.logger.info("ERROR while migrating recording")
                        print("connection error while migrating recording")

                    # for recording_limit table
                    try:
                        if client_obj.number_of_rows_recording_limit:
                            client_obj.migrate_recording_limit(cursor_online, db_online, migration_output_recording_limit, cursor_local, db_local)
                        else:
                            print("no migration required for recording_limit")
                            client_obj.logger.info("no migration required for recording_limit")
                    except:
                        client_obj.logger.info("ERROR while migrating recording_limit")
                        print("connection error while migrating recording_limit")                
                    
                    # for updating status to daily_report to cloud database
                    if (client_obj.number_of_rows_invalid_frame_tracking == client_obj.count_number_of_rows_invalid_frame_tracking) and (client_obj.number_of_rows_recording == client_obj.count_number_of_rows_recording) and (client_obj.number_of_rows_recording_limit == client_obj.count_number_of_rows_recording_limit) and (client_obj.number_of_rows_recording_tracking == client_obj.count_number_of_rows_recording_tracking) :
                        print("DONE MIGRATION")
                        client_obj.logger.info("Migration done for the day!")
                        client_obj.update_daily_report(cursor_online, db_online, today)  
                    else : 
                        print("Migration is still incomplete")
                        client_obj.logger.info("Migration is still incomplete for %s" %today)
        else:
            client_obj.logger.info("No tuples to be migrated")
        # close connection
        db_local.close()
        #db_online.close()
    
        ##### Email Logic is here #####
    try :
        client_obj.send_email(email_output_recording_tracking)
    except:
        client_obj.logger.info("Email sending failed")
    
    try :
        client_obj.send_status_email()
    except:
        client_obj.logger.info("Status Email sending failed")

    
    