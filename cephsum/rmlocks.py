from datetime import date, datetime, timedelta
import time
import logging,argparse,math

import rados
import cephtools

chunk0=f'.{0:016x}' # Chunks are hex valued
empty_lock="0x10x10xc0x00x00x00x00x00x00x00x20x30x00x00x00x540x610x67"

def main(args):
    cluster = cephtools.cluster_connect(conffile = '/etc/ceph/ceph.conf',
                                        keyring='/etc/ceph/ceph.client.admin.keyring',
                                        name='client.admin'
                                        )
    counter_notfound=0
    counter_noxttr=0
    counter_noactivelock=0
    counter_haslock=0
    counter_rmxattr_error=0
    counter_rmxattr_ok=0
    try:
        with cluster.open_ioctx(args.pool) as ioctx:
            with open(args.filelist[0],'r') as fii:
                for line in fii:
                    path = line.strip()
                    try:
                        chunk0_size, timestamp = cephtools.stat(ioctx, path)
                    except rados.ObjectNotFound:
                        logging.info(f'NotFound: {path}')
                        counter_notfound+=1
                        continue
                    res = cephtools.retrieve_xattr(ioctx,path,args.lock_name)
                    if res == None:
                        logging.info(f'NoXttrLock:{path}')
                        counter_noxttr+=1 
                        continue
                    lock_string_hex = ''.join([str(hex(i)) for i in res])
                    if lock_string_hex == empty_lock:
                        logging.info(f'NoActiveLock:{path}')
                        counter_noactivelock+=1
                        continue

                    logging.info(f'HasLock: {path}')
                    counter_haslock+=1
                    if  not args.dry_run:
                        try:
                            cephtools.remove_xattr(ioctx,path, args.lock_name)
                            counter_rmxattr_ok+=1
                            logging.info(f'RmXattrDone: {path}')
                        except:
                            logging.info(f'Error:{path}')
                            counter_rmxattr_error+=1
    finally:
        cluster.shutdown()
    logging.info(f'Counter: NotFound:      {counter_notfound}')
    logging.info(f'Counter: NoXttr:        {counter_noxttr}')
    logging.info(f'Counter: NoActiveLock:  {counter_noactivelock}')
    logging.info(f'Counter: HasLock:       {counter_haslock}')
    logging.info(f'Counter: rmxattr_OK:    {counter_rmxattr_ok}')
    logging.info(f'Counter: rmxattr_Error: {counter_rmxattr_error}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Remove the lock xattr on a file. The file list provided on input should be just a list of the object paths (no pool name, and no chunk number')
    
    parser.add_argument('--dry-run',help='Don\'t remove the locks, but run some stats against the file',action='store_true')
    parser.add_argument('-d','--debug',help='Enable additional logging',action='store_true')
    parser.add_argument('-l','--log',help='Send all logging to a dedicated file',dest='logfile',default=None)

    parser.add_argument('--lock-name',help='Name of the xattr used to hold the lock',default='lock.striper.lock')

    parser.add_argument('-p','--pool',help='define the pool to use')
    # actual path to use, as a positional argument; only one allowed
    parser.add_argument('filelist', nargs=1)

    args = parser.parse_args()

    logging.basicConfig(level= logging.DEBUG if args.debug else logging.INFO,
                    filename=None if args.logfile is None else args.logfile,
                    format='CEPHRMLOCK-%(asctime)s-%(process)d-%(levelname)s-%(message)s',                  
                    )

    main(args)

