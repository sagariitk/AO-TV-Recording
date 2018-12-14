import os
import mmap
import datetime
import time
from datetime import datetime, timedelta
from os import listdir
import sys
import json
import requests
from os.path import isfile, join
from subprocess import Popen


class Clientmethods:
    #path of the folder in local pc
    channel_name = sys.argv[1]
    device_id = sys.argv[2]
    file_path="D:\\" + channel_name
    logs_path="C:\\Users\\User\\Desktop\\scripts\\logs\\blackDetect.txt"
    receiver = "pradeep.thawani@quantiphi.com,amit.kumar@quantiphi.com"
    cf_endpoint =  "https://us-central1-athenas-owl-dev.cloudfunctions.net/cf-send-mail-generic"

    

    def send_mail(self, message,subject):
        try:
            data = {}
            data["message"] = message
            data["receiver"] = self.receiver
            data["subject"] = subject
            data["key"] = "AOPlatform"
            print(data)
            headers = {
                    'content-type': "application/json"
                 }

            response = requests.request("POST", self.cf_endpoint, data=json.dumps(data), headers=headers)
            
        except Exception as err:
            print(err)
            print("POST request not sent")

    def check_blank(self):
        #list of videos in ChannelName folder
        t = datetime.strptime("00:00:00","%H:%M:%S")
        delta = timedelta(hours=t.hour,minutes=t.minute,seconds=t.second)
        t1 = datetime.strptime("05:45:00","%H:%M:%S")
        t2 = datetime.strptime("00:15:00","%H:%M:%S")
        delta2 = timedelta(hours=t2.hour,minutes=t2.minute,seconds=t2.second)
        endTime = timedelta(hours=t1.hour,minutes=t1.minute,seconds=t1.second)
        i=1
        j=0
        video_list = [f for f in listdir(self.file_path) if isfile(join(self.file_path, f))]
        for video in video_list:
            print(video)
            while delta < endTime:
                deltaString = str(delta)
                #command_for_ffmpeg = """ffmpeg -i """ + self.file_path + os.path.sep + video + """ -ss """ + deltaString + """ -t 00:16:00 -vf "blackdetect=d=30:pix_th=0.1" -an -f null - > """+self.logs_path+ " 2>&1"
                command_for_ffmpeg = """ffmpeg -i """ + self.file_path + os.path.sep + video + """ -vf "blackdetect=d=30:pix_th=0.1" -an -f null - > """+self.logs_path+ " 2>&1"
                # " > """ + self.logs_path
                print(command_for_ffmpeg)
                os.system(command_for_ffmpeg)
                print("Completed")
                #f = open(self.logs_path)
                with open(self.logs_path) as f:
                    s = f.read()
                    print("countttt: ")
                    print(s.count("black_start"))
                    if s.count("black_start") > j:
                        print('true')
                        self.send_mail("Blank video is getting recorded", "BARC Blank recording Alert for device: "+self.device_id+", channel: " + self.channel_name)
                        j= j + s.count("black_start")
                delta = delta+delta2
                counter = str(i)
                os.rename(self.logs_path, "C:\\Users\\User\\Desktop\\scripts\\logs\\blackDetect"+counter+".txt")
                i = i+1
                time.sleep(900)
#command_for_ffmpeg = "ffmpeg -i blank.mp4 -vf blackdetect=d=50:pix_th=0.1 -f rawvideo -y nul > b.txt 2>&1"
#os.system(command_for_ffmpeg)

if __name__ == '__main__':
    Clientmethods().check_blank()
