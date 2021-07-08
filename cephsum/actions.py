
import logging,argparse
import sys, os,re
from datetime import datetime
import functools

import rados
import XrdCks
import adler32
import cephtools
import lfn2pfn




def get_from_metatdata(ioctx, path, xattr_name = "XrdCks.adler32"):
    """Try to get checksum info from metadata only.
    """
    xrdcks = cephtools.cks_from_metadata(ioctx,path,xattr_name)
    logging.info(xrdcks)
    return xrdcks  # returns None if not existing

def get_from_file(ioctx, path, readsize):
    """Try to get checksum info from file only.
    """
    xrdcks = cephtools.cks_from_file(ioctx,path,readsize)
    logging.info(xrdcks)
    return xrdcks  # returns None if not existing

def get_checksum(ioctx, path, readsize, xattr_name = "XrdCks.adler32"):
    """Try to get checksum info from metadata; else use file.
    No data is writen to metadata, and no comparison is performed
    """
    source = 'metadata'
    xrdcks = get_from_metatdata(ioctx, path, xattr_name)
    if xrdcks is None:
        xrdcks = get_from_file(ioctx, path,readsize)
        source = 'file'
    logging.info(f'Path:{path}; From:{source}; Checksum:{xrdcks.get_cksum_as_hex()}')
    return xrdcks 



def inget(ioctx, path, readsize, xattr_name = "XrdCks.adler32",rewriteto_littleendian=True):
    """Return a checksum; if in metadata, just return that. If no metadata, obtain from file and store metadata.
    If rewriteto_littleendian and metadata was stored in big endian; write it back as little endian
    """
    source = 'metadata'
    xrdcks = get_from_metatdata(ioctx, path, xattr_name)

    if rewriteto_littleendian and xrdcks is not None and xrdcks.read_format == 'big':
        logging.debug(f'Rewriting to little endian {path}')
        cks_binary = xrdcks.to_binary()
        logging.debug(cks_binary)
        cephtools.cks_write_metadata(ioctx, path, xattr_name, cks_binary, force_overwrite=True)


    if xrdcks is None:
        source = 'file'
        xrdcks = cephtools.cks_from_file(ioctx, path,readsize)
        if xrdcks is None:
            logging.warning(f"No checksum possible for {path} from file")
            return None
        logging.debug(xrdcks)

        cks_binary = xrdcks.to_binary()
        logging.debug(cks_binary)
        cephtools.cks_write_metadata(ioctx, path, xattr_name, cks_binary, force_overwrite=False)

    cks_hex = xrdcks.get_cksum_as_hex() if xrdcks is not None else "None"
    logging.info(f'Path:{path}; From:{source}; Checksum:{cks_hex}')

    return xrdcks 


def verify(ioctx, path, readsize, xattr_name = "XrdCks.adler32", force_fileread=False):
    """compare the stored checksum against the file-computed value.
    If no stored metadata, still compute file (if requested), but compare as false.
    """

    xrdcks_stored = cephtools.cks_from_metadata(ioctx, path, xattr_name)
    if xrdcks_stored is None:
        logging.debug(f'{path} has no stored metadata')

    if xrdcks_stored is None and not force_fileread:
        xrdcks_file = None
    else:
        xrdcks_file = cephtools.cks_from_file(ioctx, path,readsize)

    if xrdcks_stored is None:
        matching = False
    else:
        matching = xrdcks_stored.get_cksum_as_binary() == xrdcks_file.get_cksum_as_binary()

    logging.info (f'{path}; Matched  : {matching}, Metadata : {"None" if xrdcks_stored is None else xrdcks_stored}, File: {"None" if xrdcks_file   is None else xrdcks_file}'  )
    return xrdcks_stored if matching else None 



# def fullchain_test(ioctx, path):
#     """More for testing; run through various set of ways of getting the checksum"""

#     xattr_name = "XrdCks.adler32"
#     test_name = "TestJW2.adler32"

#     # stat
#     fsize, tstamp = cephtools.stat(ioctx, path)
#     logging.debug(fsize, tstamp)

#     # check metadata
#     logging.debug("\n\n========= Check Metatdata ==========")
#     cks = cephtools.cks_from_metadata(ioctx, path, xattr_name)
#     if cks is None:
#         logging.debug("No metadata")
#         #sys.exit(1)
#     else:
#         logging.debug(cks)
#         logging.debug(cks.to_binary())

#     logging.debug("\n\n========= Calculate ==========")
#     # calculate if needed
#     cks2 = cephtools.cks_from_file(ioctx, path)
#     logging.debug(cks2)

#     logging.debug("\n\n========= Write Metatdata ==========")
#     # Write metadata if requested
#     cks_binary = cks2.to_binary()
#     logging.debug(cks_binary)
#     cephtools.cks_write_metadata(ioctx, path, test_name, cks_binary, force_overwrite=True)

#     logging.debug("\n\n========= ReCheck Metatdata ==========")
#     #Extra check
#     cks3 = cephtools.cks_from_metadata(ioctx, path, test_name)
#     logging.debug(cks3)
#     logging.debug(cks3.to_binary() == cks2.to_binary())

#     adler = cks2.get_cksum_as_hex()

#     return adler