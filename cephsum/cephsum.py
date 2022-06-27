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


def convert_path(path, xmlfile=None):
    """
    Convert  provided path (LFN) to pool and oid (PFN), using xmlfile for mapping if provided

    Parameters:
    path : input lfn path, e.g. from xrootd

    xmlfile : the xrootd xml file used to define any lfn to pfn mapping
    """
    if xmlfile is None:
        # No mapping to give, so assume the basic defaults
        lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper() 
    else:
        lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_file(args.lfn2pfn_xmlfile)
    pool, path = lfn2pfn_converter.parse(lfn_path)

    return pool, path



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
    parser.add_argument('-e','--es',help='Send information into elastic search. See README.md for more info',dest='send_es',action='store_true')

    parser.add_argument('-r','--readsize',help='Set the readsize in MiB for each chunk of data. Should be a power of 2, and near (but not larger than) the stripe size. Smaller values wll use less memory, larger sizes may have benefits in IO performance.',
                        dest='readsize',default=64,type=int)

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

    parser.add_argument('--cephconf',default='/etc/ceph/ceph.conf', dest='conf_file', 
                        help='location of the ceph.conf file, if different from default')
    parser.add_argument('--keyring',default='/etc/ceph/ceph.client.xrootd.keyring', dest='keyring_file', 
                        help='location of the ceph keyring file, if different from default')
    parser.add_argument('--cephuser',default='client.xrootd', dest='ceph_user', 
                        help='ceph user name for the client keyring')

    parser.add_argument('--mt',default=False,help='Use multithreading',action='store_true')
    parser.add_argument('--workers',default=1,type=int,help='If multithreading, specify the max number of workers')
    


    # actual path to use, as a positional argument; only one allowed
    parser.add_argument('path', nargs=1)

    args = parser.parse_args()

    logging.basicConfig(level= logging.DEBUG if args.debug else logging.INFO,
                    filename=None if args.logfile is None else args.logfile,
                    format='CEPHSUM-%(asctime)s-%(process)d-%(levelname)s-%(message)s',                  
                    )
    #logging.debug(f'Args: {args}')
    
    if args.send_es:
        try:
            from esearch import send_data
        except Exception as e:
            # catch errors later if a problem with ES 
            logging.warning(f'Error importing the esearch module')
            pass

    # set readsize with default, or command line value, in bytes
    readsize = args.readsize*1024*1024
    logging.debug(f'Set Readsize to {readsize}')



    # obtain the pool and oid of the input object
    lfn_path = args.path[0]
    pool, path = convert_path(lfn_path, args.lfn2pfn_xmlfile)
    logging.debug(f'Converted {lfn_path} to {pool}, {path}')

    cslag = args.checksum_alg.split(':')
    checksum_alg = cslag[0].lower()
    if checksum_alg == 'auto':
        checksum_alg = 'adler32'
    if checksum_alg != 'adler32':
        if args.send_es:
            try:
                send_data({'error':"NotImplementedError",'reason':f"Alg {checksum_alg} is not implemented"})
            except Exception as e:
                logging.warning(f"ESdata send failed: {e}")
        raise NotImplementedError(f"Alg {checksum_alg} is not implemented")
    
    # note, could also be print or source, or the checksum ... #TODO
    source_checksum = None if len(cslag) < 2 else cslag[1].lower()
    if args.action == 'check' and source_checksum is None:
        raise ValueError("Need --type|-C in form adler32:<checksum> for 'check' action with source checksum value")


    use_multithreading = args.mt
    mt_workers = args.workers

    xrdcks,adler = None,None
    timestart = datetime.now()


    cluster = cephtools.cluster_connect(conffile=args.conf_file, 
                                        keyring=args.keyring_file,
                                        name=args.ceph_user)
    try:
        with cluster.open_ioctx(pool) as ioctx:
            if args.action in ['inget','check']:
                xrdcks = actions.inget(ioctx,path,readsize,xattr_name, use_multithreading=use_multithreading, mt_workers=mt_workers)
            elif args.action == 'verify':
                xrdcks = actions.verify(ioctx,path,readsize,xattr_name, use_multithreading=use_multithreading, mt_workers=mt_workers)
            elif args.action == 'get':
                xrdcks = actions.get_checksum(ioctx,path,readsize, xattr_name, use_multithreading=use_multithreading, mt_workers=mt_workers)
            elif args.action == 'metaonly':
                xrdcks = actions.get_from_metatdata(ioctx,path,xattr_name)
            elif args.action == 'fileonly':
                xrdcks = actions.get_from_file(ioctx,path, readsize, use_multithreading=use_multithreading, mt_workers=mt_workers)    
            else:
                logging.warning(f'Action {args.action} is not implemented')
                raise NotImplementedError(f'Action {args.action} is not implemented')
    finally:
        cluster.shutdown()

    timeend = datetime.now()
    time_delta_seconds = (timeend - timestart).total_seconds()

    xrdcks_hex = "N/A" if xrdcks is None else xrdcks.get_cksum_as_hex()
    exit_code = ERRCODE_OK

    if args.action == 'check':
        match = False if xrdcks is None or source_checksum != xrdcks_hex else True
        if not match:
            logging.error(f"Source checksum not matching file/stored: {source_checksum}, {xrdcks_hex}")
            exit_code = ERRCODE_MISMATCH_SOURCE
        else:
            logging.debug(f"Source checksum matches file/stored: {source_checksum}, {xrdcks_hex}")

    elif args.action == 'verify' and xrdcks is None:
        exit_code = ERRCODE_FAILED_VERIFY
    elif args.action == 'verify' and source_checksum is not None:
        match = False if source_checksum != xrdcks_hex else True
        if not match:
            logging.error(f"Source checksum not matching file/stored: {source_checksum}, {xrdcks_hex}")
            exit_code = ERRCODE_MISMATCH_SOURCE
    elif source_checksum is not None:
        # eg. could be using inget, but with source value specified; need to also fail if these don't match
        match = False if source_checksum != xrdcks_hex else True
        if not match:
            logging.error(f"Source checksum not matching file/stored: {pool}, {path}, {source_checksum}, {xrdcks_hex}")
            exit_code = ERRCODE_MISMATCH_SOURCE


    # Prepare ES ingest, if requested
    if args.send_es:
        vars={'result':"Failed" if exit_code !=0 else "Done",
              'pool':pool,
              'lfn':lfn_path,
              'exit_code':exit_code,
              'srccks':"N/A" if source_checksum is None else source_checksum,
              'timestart':timestart.timestamp(),
              'duration_s':time_delta_seconds,
              'algorithm':checksum_alg,
              'action':args.action,
            }
        if xrdcks is not None:
            vars['checksum'] = xrdcks.get_cksum_as_hex()
            vars['source']   = xrdcks.source_type
            vars['fbytes']   = xrdcks.total_size_bytes
        try:
            send_data(vars)
        except Exception as e:
            logging.warning(f"ESdata send failed: {e}")
        

    # Write out for xrootd
    if xrdcks is not None:
        adler  = xrdcks.get_cksum_as_hex()
        source = xrdcks.source_type
        fbytes = xrdcks.total_size_bytes
        logging.info(f'Result:{"Failed" if exit_code !=0 else "Done"}, pool:{pool}, path:{lfn_path}, checksum:{adler}, time_s:{time_delta_seconds}, '\
                     f' filesize_bytes:{fbytes}, source:{source}, exit_code:{exit_code}, srccks:{"N/A" if source_checksum is None else source_checksum}')
        sys.stdout.write(adler + '\n')
        sys.stdout.flush()
        sys.exit(exit_code)
    else:
        logging.warning(f'Result:failed, pool:{pool}, path:{lfn_path}')
        sys.exit(ERRCODE_NO_CHECKSUM)

