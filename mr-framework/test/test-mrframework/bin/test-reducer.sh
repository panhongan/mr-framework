#!/bin/bash

chmod +x *

awk 'BEGIN{
	dict[0]="A"
	dict[1]="B"

	FS = "\t"
	OFS = "\t"
}
{
	
	print $2, "reducer#"dict[0]
}'

