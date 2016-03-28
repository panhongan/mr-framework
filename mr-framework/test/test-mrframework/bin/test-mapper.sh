#!/bin/bash

chmod +x *

source ./hash_id.conf

awk '{
	print $0
}' | python ./hash_id.py ${TEST_HASH_NUM}

