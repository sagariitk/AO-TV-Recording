# -*- coding: utf-8 -*-
__author__ = "Pranita Kadge"
__copyright__ = "Copyright (©) 2018. Athenas Owl. All rights reserved."
__credits__ = ["Quantiphi Analytics"]

# python dependencies
import sys
import requests
import json
import time
import logging
import os
import socket
import time
import datetime

# database dependencies
import MySQLdb

class Clientmethods:

    """
    Send the mail if there is any error in recording the video.

    1.If count = 3 in process limit 
      then send the mail
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('/tmp/processLimitSendMail.log')
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(levelname)-8s %(asctime)s,%(msecs)d  [%(filename)s:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    now = datetime.datetime.now()
    cdate = now.strftime("%Y%m%d")
    cf_endpoint =  "https://us-central1-athenas-owl-dev.cloudfunctions.net/cf-send-mail-generic"
    receiver = "aopf_tvrecording@quantiphi.com"

    db = MySQLdb.connect(host='localhost',  # your host
                        user='root',  # username
                        passwd='12345',  # password
                        db='barc_db')   # db name
  
    #DB operation
    def db_operation_for_all(self,sql_query):
        # prepare a cursor object using cursor() method
        cursor = self.db.cursor()
        try:
            # Execute the SQL command
            cursor.execute(sql_query)
            # Commit your changes in the database
            self.db.commit()
            return cursor.fetchall()

        except:
            # Rollback in case there is any error
            self.db.rollback()

    def close_db_connection(self):
        self.db.close()

    def send_mail(self,message):
        try:
            data = {}
            data["message"] = message
            data["receiver"] = self.receiver
            data["subject"] = "BARC : RETRIED FOR 3 TIMES"
            data["key"] = "AOPlatform"
            print(data)
            headers = {
                    'content-type': "application/json"
                 }

            response = requests.request("POST", self.cf_endpoint, data=json.dumps(data), headers=headers)
            
        except:
            self.logger.info("POST request not sent")

    def update_mail_status(self,request_id):
        tables= """ UPDATE recording_limit
                    SET mail_status=True
                    where request_id='{}'
                """.format(request_id)
        self.db_operation_for_all(tables)
        self.logger.info("Mail status updated")

    def check_status(self):
        try:
            channel_list = """ SELECT channel_value, channel_name from channel_info;
                           """

            channel_result = self.db_operation_for_all(channel_list)

            for row in channel_result:
                channel_value = row[0]
                channel_name = row[1]
                request_id = self.cdate+"_"+str(channel_value)
                primary_query = """ SELECT request_id,count from recording_limit
                                    WHERE count = 3 and request_id = '{}' and mail_status IS NULL;
                                """.format(request_id)

                primary_result = self.db_operation_for_all(primary_query)
                
                if primary_result == None or len(primary_result) == 0:
                    self.logger.info("No requests available for processing")
                else:
                    message = "For request_id " +request_id+ " ,and channel name " +channel_name+ " the count has reached to 3."
                    print(message)
                    self.send_mail(message)
                    self.update_mail_status(request_id)
            
                
        finally:
            self.close_db_connection()
            self.logger.info("DB connection closed in finally")
            

if __name__ == '__main__':
    Clientmethods().check_status()