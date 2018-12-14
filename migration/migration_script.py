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
#from googleapiclient.errors import HttpError
#from google.cloud import pubsub_v1


#table Schema
db_columns = ['device_id', 'request_id', 'channel_value', 'clip_path', 'status', 'start_timestamp', 'end_timestamp', 'error_message', 'clip_slot', 'clip_number', 'clip_duration', 'prediction_status', 'migration_status', 'clip_size']

#email configs


class ClientMethods:
    """
    ClientMethods class contains collection of methods which are used for migration
    """
    cf_endpoint = "https://us-central1-athenas-owl-dev.cloudfunctions.net/cf-send-mail-generic"
    receiver = "darshitkumar.suratwala@quantiphi.com"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # create a file handler
    handler = logging.FileHandler('/tmp/logfiles/migrateremail.log')
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    config = ConfigParser.ConfigParser()


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

    #its used to get monday's and friday's date
    def get_date(self):
        ts = time.time()
        today = datetime.datetime.utcfromtimestamp(ts)
        #today = str(utc_dt).split(".")[0] #fetching today's date
        monday = today-datetime.timedelta( (today.weekday()) ) #calculating monday's date
        friday = monday+datetime.timedelta( (monday.weekday()+5) ) #calculating saturday's date
        if friday > today:
            today = today - datetime.timedelta(days=7)
            monday = today-datetime.timedelta( (today.weekday()) ) #calculating monday's date
            friday = monday+datetime.timedelta( (monday.weekday()+5) ) #calculating saturday's date
        monday = str(monday).split(" ")[0]
        friday = str(friday).split(" ")[0]
        return monday,friday

    def get_timestamp(self,timestamp):
        """
        takes in timestamp as an input and 
        returns the timestamp in string datatype
        """
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')

    def get_row_column(self,row_number, row_values):
        """
        input params.....  
        row_number -> its the row number starting with index 0
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

if __name__ == '__main__':
    client_obj = ClientMethods()
    #get monday and friday  
    monday,friday = client_obj.get_date()
    #connect to local DB
    #db = MySQLdb.connect(host=DB_HOST, user=DB_USERNAME, passwd=DB_PASSWORD, db= DB_NAME)
    config = ConfigParser.ConfigParser()
    config.read('/var/.ao/parameters.ini')
    stage = 'LOCAL'
    db = MySQLdb.connect(host=config.get(stage, 'DB_HOST'),  # your host
                         user=config.get(stage, 'DB_USERNAME'),  # username
                         passwd=config.get(stage, 'DB_PASSWORD'),  # password
                         db=config.get(stage, 'DB_NAME'))
    cursor = db.cursor()
    # quering local db
    # selecting data which is to be migrated
    sql_query = """ select * from recording_tracking where start_timestamp >= '{0}' AND start_timestamp <'{1}' AND device_id = 'primary' AND clip_path != 'NULL' AND status = 'DONE' AND clip_duration is not NULL AND migration_status is NULL; """.format(monday, friday)
    migration_output = client_obj.db_operation(sql_query, cursor,db)
    print("Number of rows to be migrated: %d" % len(migration_output))
    client_obj.logger.info("Number of rows to be migrated: %d" % len(migration_output))
    #selecting data which is to be mailed
    sql_query = """ select recording_tracking.device_id, recording_tracking.request_id, channel_info.channel_name, recording_tracking.STATUS, recording_tracking.start_timestamp, recording_tracking.end_timestamp, recording_tracking.error_message, recording_tracking.clip_number FROM recording_tracking inner join channel_info on recording_tracking.channel_value = channel_info.channel_value WHERE device_id = 'primary' AND clip_path = 'NULL' AND status !='DONE' AND cast(start_timestamp as date) >= '{0}' AND `start_timestamp` < '{1}' AND clip_duration is NULL AND migration_status is NULL AND clip_size is NULL """.format(monday, friday)
    email_output = client_obj.db_operation(sql_query, cursor,db)
    print("Number of rows to be emailed: %d" % len(email_output))
    client_obj.logger.info("Number of rows to be emailed: %d" % len(email_output))
    ##### Migration Logic is here #####
    if len(migration_output) < 1:
        print("no migration")
        client_obj.logger.info("No tuples to be migrated")
    else:
        try:
            # connect online DB for migration
            stage = 'CLOUD'
            db_online = MySQLdb.connect(host=config.get(stage, 'DB_HOST'),  # your host
                         user=config.get(stage, 'DB_USERNAME'),  # username
                         passwd=config.get(stage, 'DB_PASSWORD'),  # password
                         db=config.get(stage, 'DB_NAME'))
            for row_number in range(len(migration_output)):     #iterate each tuple
                row,column = client_obj.get_row_column(row_number,migration_output[row_number])
                cursor_online = db_online.cursor()
                sql_query = "INSERT INTO recording_tracking(%s) VALUES %s " %(",".join(column),row)
                temp = client_obj.db_operation(sql_query, cursor_online,db_online)
                #update migrate status using clip_path (which will be the unique value throughout the table)
                my_primary_key = migration_output[row_number][3] # fetching clip
                sql_query = """UPDATE recording_tracking SET migration_status = 1 WHERE clip_path = '{0}' """ .format(my_primary_key)
                temp = client_obj.db_operation(sql_query, cursor,db)
                client_obj.logger.info("Row #%d migrated to cloud" %(row_number+1))
        except:
            client_obj.logger.info("ERROR IN DB CONNECTION")
            print("connection error")
        else:
            client_obj.logger.info("Data sucessfully migrated")
                ###########here u write migration status
        #cursor.close()
        #del cursor
        db.close()
        db_online.close()

    ##### Email Logic is here #####
    try :
        client_obj.send_email(email_output)
    except:
        client_obj.logger.info("Email sending failed")
