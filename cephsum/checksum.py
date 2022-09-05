from abc import ABC, abstractmethod
from typing import Iterable, TypeVar, Optional
import logging 

T = TypeVar("T") # internal return type of the checksum algorithm (e.g. int)
U = TypeVar("U", bound=str) # external checksum type

class Checksum(ABC):
    """Abstract class to define the required behaviour for any Checksumming class.

    It should be capable of conversions from int to hex (and vice vesa),
    and perform the checksum calculation on an iterable collection of bytearrays.

    The responsibility of the checksum 
    """
    def __init__(self):
        self.name = "N/A"
        self._cksvalue: Optional[T] = None
        self._bytes_read: int = 0 
        self._number_buffers: int = 0
        self._log_each_step: bool = False 

    @property
    def bytes_read(self):
        return self._bytes_read

    @property
    def number_buffers(self):
        return self._number_buffers

    @property
    def log_each_step(self):
        return self._log_each_step

    @staticmethod
    @abstractmethod
    def to_output(val: T) -> U:
        """Convert the internal checksum value to the extrnal format"""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def to_internal(val: U) -> T:
        """Convert the external checksum value to the internal format (e.g. int)"""
        raise NotImplementedError


    @staticmethod
    @abstractmethod
    def inttohex(val: int) -> str:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def hextoint(val: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def _checksum(self,buffer: bytearray) -> T:
        """
        Method to subclass that performs the checksum calculation.
        Return should be the (checksum dependent) output of each iteration, in a string format.
        Usually this would expect to be the hex value at the current step

        For example:
        def _checksum(self,buffer: Iterable[bytearray], ) -> int:
            value = zlib.adler32( buf, value)
        """
        raise NotImplementedError

    def calc_checksum(self,buffer: Iterable[bytearray]) -> U:
        for buf in buffer:
            # need to consider intra-file chunks
            step_result = self._checksum(buf) # pass of the checksum calculation to the concrete subclass
            self._bytes_read += len(buf)
            self._number_buffers += 1
            if self.log_each_step:
                logging.debug('%s: %s %s %s' % (self.name, self.to_output(step_result), len(buf), self.bytes_read) )
        
        self.value      = self.to_output(self._cksvalue)

        return self.value


