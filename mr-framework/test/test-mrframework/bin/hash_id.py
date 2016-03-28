#!/usr/bin/env python
# coding=utf-8

'''
Created on 2014年9月27日

@author: phadyc@126.com
'''

import sys
import hashlib
import string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)
    
    reduce_num = int(sys.argv[1])
    if reduce_num <= 0:
        sys.exit(1)
    
    for line_str in sys.stdin.readlines():
        line_str = line_str.strip()
        str_list = string.split(line_str, "\t")
        if len(str_list) > 0:
            md5 = hashlib.md5(str_list[0]).hexdigest()
            print("%d\t%s" % (int(md5.upper(), 16) % reduce_num, line_str) )

