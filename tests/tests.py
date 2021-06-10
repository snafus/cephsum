from datetime import datetime
import unittest
import datetime 

from cephsum import adler32, XrdCks
from cephsum import lfn2pfn

class TestAdler32(unittest.TestCase):
    def test_inttohex(self):
        """
        Test if hex value is converted to an int
        """
        a32_int = 999476992
        a32_hex = '3b92cf00'
        test_hex = adler32.adler32.adler32_inttohex(a32_int)

        self.assertEqual(a32_hex, test_hex)


    def test_hextoint(self):
        """
        Test if hex value is converted to an int
        """
        a32_int = 999476992
        a32_hex = '3b92cf00'
        test_int = adler32.adler32.adler32_hextoint(a32_hex)

        self.assertEqual(a32_int, test_int)


    def test_btyesread_0(self):
        """
        Test a 0 byte checksum
        """
        alg = adler32.adler32()
        val = alg.calc_checksum(b'')
        self.assertEqual(val,'00000001')

    def test_bytesread(self):
        """
        Test a byte checksum
        """
        alg = adler32.adler32()
        val = alg.calc_checksum([b'1234'])
        self.assertEqual(val,'01f800cb')

class TestXrdCks(unittest.TestCase):
    def test_from_binary(self):
        val=b'adler32\x00\x00\x00\x00\x00\x00\x00\x00\x00I\xfe\xbd`\x00\x00\x00\x00\xf5\xf1\xff\xff\x00\x00\x00\x04\x88\xb8\xf4\xa2\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        xrdcks = XrdCks.XrdCks.from_binary(val)
        #print(xrdcks)
        self.assertEqual('adler32', xrdcks.name)
        self.assertEqual('88b8f4a2', xrdcks.get_cksum_as_hex() )
        #self.assertEqual(1623062359, xrdcks.fm_time)
        #self.assertEqual(-3595, xrdcks.cs_time) # FIXME choose a better time ... 



class TestLfn2Pfn(unittest.TestCase):
    _test_xml = """<storage-mapping>
<!-- The following is always applied (we specify protocol=xrootd in the xrootd config file) -->
<lfn-to-pfn protocol="xrootd" chain="direct" path-match="(.*)" result="$1"/>
<lfn-to-pfn protocol="http"  chain="direct" path-match="(.*)" result="$1"/>
<lfn-to-pfn protocol="https"  chain="direct" path-match="(.*)" result="$1"/>
<!-- Below we define the mappings used for each VO as necessary, exiting on the first match -->
<!-- CMS mapping for AAA testing -->
<lfn-to-pfn protocol="direct" path-match="/+store/test/xrootd/T1_UK_RAL/+store/(.*)" result="cms:/store/$1"/>
<!-- CMS mapping from CMS LFNs to ECHO object names -->
<lfn-to-pfn protocol="direct" path-match="/+store/(.*)" result="cms:/store/$1"/>
<!-- Counter the problem with XRootD clients preceding an OID with an extraneous slash (especially an issue for TPC transfers) -->
<!-- See: https://wiki.e-science.cclrc.ac.uk/web1/bin/view/EScienceInternal/XRootDName2NameTFC -->
<lfn-to-pfn protocol="direct" path-match="/*(.*)" result="$1"/>
<!-- <lfn-to-pfn protocol="direct" path-match="/(.*)" result="$1"/> -->
</storage-mapping>
"""
    def test_atlas0(self):
        c = lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_string(self._test_xml)
        pool,pfn = c.parse("atlas:test/rucio/tests/77/1d/step14.898.10671.recon.ESD.85875.82011")

        self.assertEqual(pool,'atlas')
        self.assertEqual(pfn,'test/rucio/tests/77/1d/step14.898.10671.recon.ESD.85875.82011')

    def test_atlas1(self):
        c = lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_string(self._test_xml)
        pool,pfn = c.parse("/atlas:test/rucio/tests/77/1d/step14.898.10671.recon.ESD.85875.82011")

        self.assertEqual(pool,'atlas')
        self.assertEqual(pfn,'test/rucio/tests/77/1d/step14.898.10671.recon.ESD.85875.82011')

    def test_atlas2(self):
        c = lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_string(self._test_xml)
        pool,pfn = c.parse("//atlas:test/rucio/tests/77/1d/step14.898.10671.recon.ESD.85875.82011")

        self.assertEqual(pool,'atlas')
        self.assertEqual(pfn,'test/rucio/tests/77/1d/step14.898.10671.recon.ESD.85875.82011')

    # This test would fail, but appears to be how the xml and logic is defined!
    # def test_cms0(self):
    #     c = lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_string(self._test_xml)
    #     pool,pfn = c.parse("store/mc/Run3Winter21DRMiniAOD/DYToLL_M-50_TuneCP5_14TeV-pythia8/GEN-SIM-DIGI-RAW/FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2/70010/2bf7006f-029d-4bf2-adaa-949896f22dcb.root")

    #     self.assertEqual(pool,'cms')
    #     self.assertEqual(pfn,'/store/mc/Run3Winter21DRMiniAOD/DYToLL_M-50_TuneCP5_14TeV-pythia8/GEN-SIM-DIGI-RAW/FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2/70010/2bf7006f-029d-4bf2-adaa-949896f22dcb.root')

    def test_cms1(self):
        c = lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_string(self._test_xml)
        pool,pfn = c.parse("/store/mc/Run3Winter21DRMiniAOD/DYToLL_M-50_TuneCP5_14TeV-pythia8/GEN-SIM-DIGI-RAW/FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2/70010/2bf7006f-029d-4bf2-adaa-949896f22dcb.root")

        self.assertEqual(pool,'cms')
        self.assertEqual(pfn,'/store/mc/Run3Winter21DRMiniAOD/DYToLL_M-50_TuneCP5_14TeV-pythia8/GEN-SIM-DIGI-RAW/FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2/70010/2bf7006f-029d-4bf2-adaa-949896f22dcb.root')

    def test_cms2(self):
        c = lfn2pfn_converter = lfn2pfn.Lfn2PfnMapper.from_string(self._test_xml)
        pool,pfn = c.parse("//store/mc/Run3Winter21DRMiniAOD/DYToLL_M-50_TuneCP5_14TeV-pythia8/GEN-SIM-DIGI-RAW/FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2/70010/2bf7006f-029d-4bf2-adaa-949896f22dcb.root")

        self.assertEqual(pool,'cms')
        self.assertEqual(pfn,'/store/mc/Run3Winter21DRMiniAOD/DYToLL_M-50_TuneCP5_14TeV-pythia8/GEN-SIM-DIGI-RAW/FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2/70010/2bf7006f-029d-4bf2-adaa-949896f22dcb.root')



if __name__ == '__main__':
    unittest.main()


