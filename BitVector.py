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
    def find_pos(pos: int) -> tuple:
        return math.floor(pos / 8), pos % 8

    def get_bit(self, pos: int) -> bool:
        pos = BitVector.find_pos(pos)
        check = self.posVector[pos[1]] & self.bitVector[pos[0]]
        if check == 0:
            return False
        return True

        
    # Flip value at position by using XOR
    def flip_bit(self, pos: int):
        pos = BitVector.find_pos(pos)
        self.bitVector[pos[0]] ^= self.posVector[pos[1]]

        
    # Set bit a position to true by using OR
    def set_bit(self, pos: int):
        pos = BitVector.find_pos(pos)
        self.bitVector[pos[0]] |= self.posVector[pos[1]]

        
    # Set bit a position to false by using AND
    def reset_bit(self, pos: int):
        pos = BitVector.find_pos(pos)
        self.bitVector[pos[0]] &= ~self.posVector[pos[1]]

    def print_vector(self):
        i = int.from_bytes(self.bitVector, byteorder='big')
        print("hex: " + "{0:x}".format(i))
