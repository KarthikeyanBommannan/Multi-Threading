#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pymongo
import requests
import json
import time


conn = pymongo.MongoClient("mongodb://karthikeyan:karthi007@ac-bc4wsub-shard-00-00.ayv02is.mongodb.net:27017,ac-bc4wsub-shard-00-01.ayv02is.mongodb.net:27017,ac-bc4wsub-shard-00-02.ayv02is.mongodb.net:27017/?ssl=true&replicaSet=atlas-j3cu4j-shard-0&authSource=admin&retryWrites=true&w=majority")
db = conn["CRYPTO-CURRENCY"]
print("Connection Established Successfully")

def get_data_from_link1():
    collec1 = db["BITCOIN"]
    for i in range(10800):
        try:   
            response1 = requests.get('https://www.bitstamp.net/api/v2/ticker/btcusd/')
            data1 = response1.json()
            collec1.insert_one(data1)
            if i == 10800:
                print("Data Limit reached")    
        except KeyboardInterrupt:
            print("human Interruption")
            break
        time.sleep(2)       
                

def get_data_from_link2():
    collec2 = db["ETHERUM"]
    for i in range(10800):
        try:   
            response2 = requests.get('https://www.bitstamp.net/api/v2/ticker/ethusd/')
            data2 = response2.json()
            collec2.insert_one(data2)
            if i == 10800:
                print("Data Limit reached")    
        except KeyboardInterrupt:
            print("human Interruption")
            break
        time.sleep(2)          
                

def get_data_from_link3():
    collec3 = db["TETHER"]
    for i in range(10800):
        try:   
            response3 = requests.get('https://www.bitstamp.net/api/v2/ticker/usdtusd/')
            data3 = response3.json()
            collec3.insert_one(data3)
            if i == 10800:
                print("Data Limit reached")    
        except KeyboardInterrupt:
            print("human Interruption")
            break
        time.sleep(2)       

     
    
def get_data_from_link4():
    collec4 = db["SOLANA"]
    for i in range(10800):
        try:   
            response4 = requests.get('https://www.bitstamp.net/api/v2/ticker/solusd/')
            data4 = response4.json()
            collec4.insert_one(data4)
            if i == 10800:
                print("Data Limit reached")    
        except KeyboardInterrupt:
            print("human Interruption")
            break
        time.sleep(2)       

def get_data_from_link5():
    collec5 = db["BITCOIN CASH"]
    for i in range(10800):
        try:   
            response5 = requests.get('https://www.bitstamp.net/api/v2/ticker/dchusd/')
            data5 = response5.json()
            collec5.insert_one(data5)
            if i == 10800:
                print("Data Limit reached")    
        except KeyboardInterrupt:
            print("human Interruption")
            break
        time.sleep(2)             
                
                
import threading

if __name__ == "__main__":
    
    start_time = time.time()
    x1 = threading.Thread(target = get_data_from_link1)
    x2 = threading.Thread(target = get_data_from_link2)
    x3 = threading.Thread(target = get_data_from_link3)
    x4 = threading.Thread(target = get_data_from_link4)
    x5 = threading.Thread(target = get_data_from_link5)
    
    x1.start()
    x2.start()
    x3.start()
    x4.start()
    x5.start()
   
    x1.join()
    x2.join()
    x3.join()
    x4.join()
    x5.join()
    end_time = time.time()
    total_time = (start_time-end_time)
    print("Total time = ",total_time)
    print("Successfully completed")
    
    

