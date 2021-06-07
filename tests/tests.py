from datetime import datetime
import unittest
import datetime 

from cephsum import adler32, XrdCks

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





if __name__ == '__main__':
    unittest.main()


