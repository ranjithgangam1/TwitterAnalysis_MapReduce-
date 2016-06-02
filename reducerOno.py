#!/usr/bin/python
import sys
import string
import json
import datetime

input_dict = {}
count_dict ={}
tweet_count =0
count =[]
Oldtime = 25
dateList = []

for line in sys.stdin:
    line = line.strip()
    (key,count) =line.split('\t',1)
   
    dt = datetime.datetime.strptime(key,'%a %b %d %H:%M:%S +0000 %Y')
    datetest = dt.strftime('%b%d')
    if not(datetest in dateList):
      dateList.append(datetest)
          
    
    time1 = dt.hour
    if time1 in count_dict.keys():
      count_dict[time1] += 1
    else:
      count_dict[time1] = 1
      
for each in count_dict.keys():
  print each ,float(count_dict[each])/len(dateList)
   




  



