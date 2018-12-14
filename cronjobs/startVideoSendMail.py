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

    1.If status is InProgress and device  is primary 
      then send the mail

    2.If status is Fail and device is primary
      then send the mail

    3.If status is InProgress and device is secondary
      then send the mail

    4.If status is Fail and device is secondary
      then send the mail
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a file handler
    handler = logging.FileHandler('/tmp/startVideoSendMail.log')
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

    def send_mail(self,channel_list,device_list,status_list):
        try:
            message = """Channel Name</td><td style="padding: 15px;text-align: left;border-collapse: collapse;border: 1px solid #ddd;">Device ID</td><td style="padding: 15px;text-align: left;border-collapse: collapse;border: 1px solid #ddd;">Status</td>"""
            for channel,device,status in zip(channel_list,device_list,status_list):
                message = message + """ <tr style="background-color:#fff;">
                <td style="padding: 15px;text-align: left;border-collapse: collapse;border: 1px solid #ddd;">{0}</td><td style="padding: 15px;text-align: left;border-collapse: collapse;border: 1px solid #ddd;">{1}</td><td style="padding: 15px;text-align: left;border-collapse: collapse;border: 1px solid #ddd;">{2}</td></td>
                </tr>""".format(channel,device,status)

            data = {}
            data["message"] = message
            data["receiver"] = self.receiver
            data["subject"] = "BARC TV RECORDING STATUS"
            data["key"] = "AOPlatform"
            print(data)
            headers = {
                    'content-type': "application/json"
                 }

            response = requests.request("POST", self.cf_endpoint, data=json.dumps(data), headers=headers)
            
        except:
            self.logger.info("POST request not sent")

    
    def check_status(self):
        try:
            done_query= """ SELECT channel_value, channel_name from channel_info;
                        """
            
            done_result = self.db_operation_for_all(done_query)

            channel_list = []
            device_list = []
            status_list = []
            
            for row in done_result:
                channel_value = row[0]
                channel_name = row[1]
                request_id = self.cdate+"_"+str(channel_value)

                primary_query = """ SELECT channel_value from recording_tracking 
                                    WHERE channel_value = {0} and request_id = '{1}' and device_id = 'a' and status = 'InProgress' 
                                """.format(channel_value,request_id)

                primary_result = self.db_operation_for_all(primary_query)
                
                if primary_result == None or len(primary_result) == 0:
                    channel_list.append(channel_name)
                    device_list.append("Primary")
                    status_list.append("Not Started")
                    self.logger.info("When recording Failed for primary, mail sent")
                else:
                    channel_list.append(channel_name)
                    device_list.append("Primary")
                    status_list.append("Started")
                    self.logger.info("When recording started for primary, mail sent")

                secondary_query = """ SELECT channel_value from recording_tracking 
                                      WHERE channel_value = {0} and request_id = '{1}' and device_id = 'b' and status = 'InProgress' 
                                  """.format(channel_value,request_id)

                secondary_result = self.db_operation_for_all(secondary_query)
                
                if secondary_result == None or len(secondary_result) == 0:
                    channel_list.append(channel_name)
                    device_list.append("Secondary")
                    status_list.append("Not Started")
                    self.logger.info("When recording Failed for secondary, mail sent")
                else:
                    channel_list.append(channel_name)
                    device_list.append("Secondary")
                    status_list.append("Started")
                    self.logger.info("When recording started for secondary, mail sent")


            self.send_mail(channel_list,device_list,status_list)
                
        finally:
            self.close_db_connection()
            self.logger.info("DB connection closed in finally")
            

if __name__ == '__main__':
    Clientmethods().check_status()