#!/bin/sh

# If a timeout is explicitly requeired, could be included in the command below.

#Update the path name to the correct location
RESULT=$(python3 /etc/xrootd/cephsum/cephsum.py -x /etc/xrootd/storage.xml -d --action=inget $1)
ECODE=$(echo $?)

# Additional logging could be added here if needed

# Must return on stdout just the checksum followed by a newline 
printf "${RESULT}\n"
# Exit code from the output of the python script
exit ${ECODE)
