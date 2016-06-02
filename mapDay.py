#!/usr/bin/python

import sys
import json

count_ln =0
Prez_twit_count = 0
input_dict = {}
user_dict ={}
#createdtime =[]
for line in sys.stdin:
  #count_ln is to count number of tweets it processed
  input_dict = json.loads(line)
  user_dict = input_dict['user']
  if user_dict['screen_name'] == "PrezOno":
    print '%s\t%s' % (input_dict['created_at'], 1)
       
#Test to check no of twits analysed by each mapper 
#print count_ln

