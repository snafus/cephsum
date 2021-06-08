# cephsum

## Introduction
Cephsum is intended as a simple alternative for performing checksum operations in xrootd on Ceph object-store systems
using librados.
The basic command will get / caclulate the checksum via whatever means, and write the checksum into the ceph xattr if needed, 
following the XrootD XrdCks binary file format. Little-endian format is used; following the gridFTP implementation 
(which is likely different to the default xrdadler32 and XrdCks method.)

## dependencies
The package was designed to rely on a minimal set of external dependencies. This is python3 only, and needs the librados python
bindings.

## Usage with xrootd
Main motivation for this package is to replace the internal XrootD checksum implemetation with a simple external script. 
As such a few modifications to the xrootd.cfg are required. 

In addition, xrootd external checksum scripts only provide the LFN on the path, not the converted PFN. Using `-x storage.xml`, 
this functionality can be recovered. 

## Basic standalone usage
```
python3 cephsum.py  --action=inget -x storage.xml  dteam:test1/testfile.root
```
The above command should get / caclulate the checksum via whatever means, and write the checksum into the ceph xattr if needed, 
following the XrootD XrdCks binary file format. Little-endiany

## Caveats



## Examples 

Default example; read a checksum from metadata if possible, else calculate from file and insert back into metadata:
```
python3 cephsum.py  --action=inget   dteam:test1/testfile.root
```

Read cehcksum, from metadata, or file if ncessary. Don't write back any caclulated checksum
```
python3 cephsum.py  --action=get   dteam:test1/testfile.root
```


Use a storage.xml file, used in xrootd for lfn2pfn mapping
```
python3 cephsum.py  --action=inget -x storage.xml  dteam:test1/testfile.root
```

Get checksum from only metadata, or file. Don't store any checksum back however
```
python3 cephsum.py  --action=metaonly   dteam:test1/testfile.root
python3 cephsum.py  --action=fileonly   dteam:test1/testfile.root
```

Compare stored checksum to file-calculated checksum,
If -C adler32:<value> is provided, then also compare to the provided checksum
```
python3 cephsum.py  --action=verify   dteam:test1/testfile.root
python3 cephsum.py  --action=verify -C adler32:95413e91  dteam:test1/testfile.root
```

Check given checksum against source checksum provided by   -C adler32:<value>.
if not in metadata, calculate and insert to metadata if matches
```
python3 cephsum.py  --action=check -C adler32:95413e91  dteam:test1/testfile.root
```





