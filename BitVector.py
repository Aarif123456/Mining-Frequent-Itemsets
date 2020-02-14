# Abdullah Arif
# Class to represent bit vector in Python 
# Used for the PCY algorithm
import math


class BitVector:
    posVector = b'\x80\x40\x20\x10\x08\x04\x02\x01'  # holds arguments to target a bit in the byte vector

    def __init__(self, size: float):
        sz = math.ceil(size / 8)  # store size of bit vector
        self.bitVector = bytearray(sz)

    # since each byte has 8 bits we have to translate position into a tuple
    @staticmethod
    def findPos(pos: int) -> tuple:
        return math.floor(pos / 8), pos % 8

    def getBit(self, pos: int) -> bool:
        pos = BitVector.findPos(pos)
        check = self.posVector[pos[1]] & self.bitVector[pos[0]]
        if check == 0:
            return False
        return True

        # flip value at position by using XOR

    def flipBit(self, pos: int):
        pos = BitVector.findPos(pos)
        self.bitVector[pos[0]] ^= self.posVector[pos[1]]

        # Set bit a position to true by using OR

    def setBit(self, pos: int):
        pos = BitVector.findPos(pos)
        self.bitVector[pos[0]] |= self.posVector[pos[1]]

        # Set bit a position to false by using AND

    def resetBit(self, pos: int):
        pos = BitVector.findPos(pos)
        self.bitVector[pos[0]] &= ~self.posVector[pos[1]]

    def printVector(self):
        i = int.from_bytes(self.bitVector, byteorder='big')
        print("hex: " + "{0:x}".format(i))
