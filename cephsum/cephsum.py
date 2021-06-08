#!/usr/bin/env python3

# Main entrypoint for cephsum module


import logging,argparse
import sys, os,re
from datetime import datetime
import functools

import rados
import XrdCks
import adler32
import cephtools
import lfn2pfn
import actions

ERRCODE_OK = 0
ERRCODE_MISMATCH_SOURCE = 101
ERRCODE_NO_CHECKSUM     = 102
ERRCODE_FAILED_VERIFY   = 103


if __name__ == "__main__":
    xattr_name = "XrdCks.adler32"

    parser = argparse.ArgumentParser(description='Checksum based operations for Ceph rados system; based around XrootD requirments')

    parser.add_argument('-C','--type',type=str,default='adler32',dest='checksum_alg',
                     help='-C {adler32 | crc32 | md5 | zcrc32 | auto}[:{<value>|print|source}] like in xrdcp options:\n'\
                     """Obtains the checksum of type (i.e. adler32, crc32, or md5) from the source, computes the checksum at the destination, and verifies that they\
                          are the same. If a value is specified, it is used as the source checksum. When print is specified, the checksum at the destination is printed but #is not verified.
                    
                     Note - when in Xrootd config, -C may be added from the xrd manager
                     """)

    parser.add_argument('-d','--debug',help='Enable additional logging',action='store_true')
    parser.add_argument('-l','--log',help='Send all logging to a dedicated file',dest='logfile',default=None)


    parser.add_argument('-x','--lfn2pfnxml',default=None, dest='lfn2pfn_xmlfile', 
                        help='The storage.xml file usually provided to xrootd for lfn2pfn mapping. If not provided a simple method is used to separate the pool and object names')

    #parser.add_argument('-s','--source',default=None,help='Provide a source checksum value. Modifies behaviour of --action.')
    #parser.add_argument('-w','--forcewrite',action='store_true',help='Depending on action, allow overwriting of existing metadata')
    #parser.add_argument('-e','--fixendian',action='store_true', help='Allow correction to bad endian formatting; independent to --forcewrite')


    parser.add_argument('-a','--action',default='inget',
                        help="""<inget>|get|verify|check  Default inget: Select action to perform: depends also on -C option. 
    Possible options are:

    \ninget:       Get checksum from metadata, or calculate from file if not in metadata; adds to metadata, if needed. if --source is provided, non-zero error code if non-matching, and metadata not added.
    \nget:         Read checksum, from metadata, or from file -- if not in metadata. Don't write anything back.
    \nmetaonly:    Read checksum, only from metadata. Will fail if not existing
    \nfileonly:    Get checksum, from file only
    \nverify:      Calculate checksum from file and compares to metadata value (if not in metadata, fail). If --source is given, also compare to source value
    \ncheck:       Requires --source value; if not in metadata, calculate and insert to metadata if matches.  
                        """)

    # actual path to use, as a positional argument; only one allowed
    parser.add_argument('path', nargs=1)

    args = parser.parse_args()

    logging.basicConfig(level= logging.DEBUG if args.debug else logging.INFO,
                    filename=None if args.logfile is None else args.logfile,
                    format='CEPHSUM-%(asctime)s-%(process)d-%(levelname)s-%(message)s',                  
                    )
    #logging.debug(f'Args: {args}')
    
    lfn_path = args.path[0]

    #pool, path = split_path(args.path[0]) # extract pool and path from LFN
    if args.lfn2pfn_xmlfile is None:
        # No mapping to give, so assume the basic defaults
        lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper() 
    else:
        lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_file(args.lfn2pfn_xmlfile)
    pool, path = lfn2pfn_converter.parse(lfn_path)
    
    #logging.debug(f'{lfn2pfn_converter}')
    logging.debug(f'Converted {lfn_path} to {pool}, {path}')

    cslag = args.checksum_alg.split(':')
    checksum_alg = cslag[0].lower()
    if checksum_alg == 'auto':
        checksum_alg = 'adler32'
    if checksum_alg != 'adler32':
        raise NotImplementedError(f"Alg {checksum_alg} is not implemented")
    
    # note, could also be print or source, or the checksum ... #FIXME
    source_checksum = None if len(cslag) < 2 else cslag[1].lower()
    if args.action == 'check' and source_checksum is None:
        raise ValueError("Need --type|-C in form adler32:<checksum> for 'check' action with source checksum value")

    xrdcks,adler = None,None
    timestart = datetime.now()


    cluster = cephtools.cluster_connect()
    try:
        with cluster.open_ioctx(pool) as ioctx:
            if args.action in ['inget','check']:
                xrdcks = actions.inget(ioctx,path,xattr_name)
            elif args.action == 'verify':
                xrdcks = actions.verify(ioctx,path,xattr_name)
            elif args.action == 'get':
                xrdcks = actions.get_checksum(ioctx,path,xattr_name)
            elif args.action == 'metaonly':
                xrdcks = actions.get_from_metatdata(ioctx,path,xattr_name)
            elif args.action == 'fileonly':
                xrdcks = actions.get_from_file(ioctx,path)    
            else:
                raise NotImplementedError(f'Action {args.action} is not implemented')
    finally:
        cluster.shutdown()

    timeend = datetime.now()
    time_delta_seconds = (timeend_utc - timestart_utc).total_seconds()

    xrdcks_hex = "N/A" if xrdcks is None else xrdcks.get_cksum_as_hex()
    exit_code = ERRCODE_OK

    if args.action == 'check':
        match = False if xrdcks is None or source_checksum != xrdcks_hex else True
        if not match:
            logging.warning(f"Source checksum not matching file/stored: {source_checksum}, {xrdcks_hex}")
            exit_code = ERRCODE_MISMATCH_SOURCE
        else:
            logging.debug(f"Source checksum matches file/stored: {source_checksum}, {xrdcks_hex}")
    elif args.action == 'verify' and xrdcks is None:
        exit_code = ERRCODE_FAILED_VERIFY
    elif args.action == 'verify' and source_checksum is not None:
        match = False if source_checksum != xrdcks_hex else True
        if not match:
            logging.warning(f"Source checksum not matching file/stored: {source_checksum}, {xrdcks_hex}")
            exit_code = ERRCODE_MISMATCH_SOURCE




    # Write out for xrootd
    if xrdcks is not None:
        adler  = xrdcks.get_cksum_as_hex()
        source = xrdcks.source_type
        fbytes = xrdcks.total_size_bytes
        logging.info(f'Result:{"Failed" if exit_code !=0 else "Done"}, pool:{pool}, path:{lfn_path}, checksum:{adler}, time_s:{time_delta_seconds}, filesize_bytes:{fbytes}, source:{source}')
        sys.stdout.write(adler + '\n')
        sys.stdout.flush()
        sys.exit(exit_code)
    else:
        logging.warning(f'Result:failed, pool:{pool}, path:{lfn_path}')
        sys.exit(ERRCODE_NO_CHECKSUM)



    # parser = argparse.ArgumentParser(description='Checksum based operations for Ceph rados system; based around XrootD requirments')
    # parser.add_argument('-l','--log',type=str,help='Checksum log file',default='/tmp/jw_checksum.log')
    # parser.add_argument('-d','--debug',help='Enable additional logging',action='store_true')


    # parser.add_argument('-C','--type',type=str,help='Checksum log file',default='adler32',
    #                 help='-C {adler32 | crc32 | md5 | zcrc32 | auto}[:{<value>|print|source}] as in xrdcp options:\n'+\
    #                 """Obtains the checksum of type (i.e. adler32, crc32, or md5) from the source, computes the checksum at the destination, and verifies that they are the same. If a value is specified, it is used as the source checksum. When print is specified, the checksum at the destination is printed but is not verified.
                    
    #                 Note - when in Xrootd config, -C may be added from the xrd manager
    #                 """)


    # parser.add_argument('-m','--mode',type=str,help="""What to do with the checksum; options [onlymetadata|onlycalc|verify|get|upget]
    #   onlymetadata: Retrieve from metadata (via xattr). If not in xattr raise exception and exit with failure.
    #   onlycalc:     Ignore metadata and caclulate from file; does not write anything back.
    #   verify:       Compare metadata value to directly calculated value; If no metadata then fail.
    #   get:          Retrieve via metadata, if exists, else calculate. Does not update
    #   upget:        Get via whatever means, and update if metadata not existing.
    #   force:        Force a new update into the metadata even if existing.
    # """, default='get',)

    # parser.add_argument('-t','--tfcfile',type=str,help='use a TFC storage.xml file to map input path to ceph pool and oid')


