# Twitter API Crawler
# -*- coding: utf-8 -*-
'''
Author: Junjun Yin
Penn State University
Email: yinjunjun@gmail.com
State College, 05/24/2019
'''
#!/usr/bin/env python
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime
import json
import time

# from tweepy.utils import import_simplejson
# json = import_simplejson()

class TwitterCrawler(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    """
    def __init__(self, name):
        #tweets_WorldPart1_org_2019-05-26-14-45-08.txt
        self.allN = 500000
        self.name = name
        self.mDate = datetime.datetime.now().strftime("%Y-%m-%d")
        self.mTime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.hTime = datetime.datetime.now().strftime("%Y-%m-%d-%H")
        self.orgFile = open("data/tweets_" + self.name + "_org_" + self.hTime + ".txt",'a')
        self.geoFile = open("data/tweets_" + self.name + "_geo_" + self.hTime + ".txt",'a')

        self.orgCounter = 0
        self.geoCounter = 0

    def on_data(self, data):
        self.write2Org(data)
        self.write2Geo(data)
        return True

    def on_error(self, status_code):
        mFile = open("error_log.txt","a")
        mFile.write(str(status_code))
        mFile.close()
        if status_code == 420:
            return False

    def on_limit(self, track):
        """Called when a limitation notice arrives"""
        try:
            mFile = open("limit_log.txt","w+")
            mFile.write(str(track))
            mFile.close()
            print(track)
        except Exception as e:
            print("track exception")
            pass
        return True

    def on_status(self, status):
        try:
            mFile = open("status_log.txt","a+")
            mFile.write(str(status))
            mFile.close()
            print(status.text)
        except Exception as e:
            print('Encountered Exception Tweet')
            pass
        return True

    def write2Org(self,data):
        hTime = datetime.datetime.now().strftime("%Y-%m-%d-%H")
        if hTime == self.hTime:
            if self.orgCounter < self.allN:
                self.orgFile.write(data)
                self.orgCounter +=1
            else:
                self.orgFile.close()
                # xx = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                xx = datetime.datetime.now().strftime("%Y-%m-%d-%H")
                self.orgCounter = 0
                self.orgFile = open("data/tweets_" + self.name + "_org_" + self.hTime + "-" + xx + "_break.txt",'a+')
                self.orgFile.write(data)
                # self.mTime = xx
                self.hTime = xx
                self.orgCounter +=1
        else:
            self.orgFile.close()
            self.geoFile.close()
            # self.mDate = mDate
            self.hTime = hTime
            # self.mTime = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            self.orgFile = open("data/tweets_" + self.name + "_org_" + self.hTime + ".txt",'a+')
            self.geoFile = open("data/tweets_" + self.name + "_geo_" + self.hTime + ".txt",'a+')

    
    def write2Geo(self,data):
        if self.geoCounter < self.allN:
            try:
                dataX = json.loads(data)
                if "geo" in dataX:
                    if dataX['geo'] is not None:
                    # self.write2Lite(data)
                        self.geoFile.write(data)
                        self.geoCounter +=1
            except ValueError as e:
                    pass
        else:
            self.geoFile.close()
            xx = datetime.datetime.now().strftime("%Y-%m-%d-%H")
            self.geoCounter = 0
            self.geoFile = open("data/tweets_" + self.name + "_geo_" + self.hTime + "-" + xx + "_break.txt",'a')
            self.geoFile.write(data)
            self.hTime = xx
            self.geoCounter +=1

if __name__ == '__main__':
    # mFile = open("identity.txt","rb")
    mFile = open("identity.txt","r")

    identity =[]
    for ele in mFile:
        identity.append(ele.rstrip())
    auth = OAuthHandler(identity[0], identity[1])
    auth.set_access_token(identity[2], identity[3])

    # pFile = open("parameters.txt","rb")
    # loc = pFile.next().rstrip().split('\t')
    pFile = open("parameters.txt","r")

    loc = next(pFile).rstrip().split('\t')
    locs = loc[1].split(',')
    locations = []
    for ele in locs:
        locations.append(float(ele))
    name = next(pFile).rstrip().split('\t')
    print(name[1])
    app = TwitterCrawler(name[1])


    while True:
        try:
            print("Stream starts again ...")
            stream = Stream(auth, app)
            stream.filter(locations =locations, is_async=False)
        except:
            print("Retry connection after 1 minutes")
            time.sleep(60)
            continue


