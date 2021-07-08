#!/bin/sh

# If a timeout is explicitly requeired, could be included in the command below.

#Update the path name to the correct location
# -d enables debug logging (logging goes to the xrootd log file)
# -r 64 implies to use 64MiB block size for each read request; see help for more info
RESULT=$(python3 /etc/xrootd/cephsum/cephsum.py -x /etc/xrootd/storage.xml -d -r 64 --action=inget $1)
ECODE=$(echo $?)

# Additional logging could be added here if needed

# Must return on stdout just the checksum followed by a newline 
printf "${RESULT}\n"
# Exit code from the output of the python script
exit ${ECODE)
