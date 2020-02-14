# Abdullah Arif
# Feb 14, 2020
# Project 1 for COMP-4250 (Big data)
# Program will take the data from given text file, break it down into chunks 
# and find frequent item-sets using the PCY algorithm
from BitVector import BitVector
import itertools  # used to create candidate pairs efficiently
import numpy  # used to create fixed sized array
import time # used to keep track of time


class PCY:
    def __init__(self, dataFilePath: str, support: int, chunk: int, sizeOfBucket: int):
        self.sizeOfBucket = sizeOfBucket  # set size of bucket - the bigger the bucket the less false positives
        with open(dataFilePath, "r", encoding="utf-8") as file:
            self.dataFile = file.read().splitlines()
        # support threshold is used for eliminating items for the algorithm
        self.support = ((support / 100) * len(self.dataFile)) * (chunk / 100)
        self.lastLine = int(len(self.dataFile) * (chunk / 100))  # last line to process depending on chunk size

    @staticmethod
    def hashPair(pair: tuple, num=1) -> int:
        if num == 1:
            return (int(pair[0]) + int(pair[1])) % 15485863  # Picked a random large prime
        return (int(pair[0]) + int(pair[1])) % 3060427  # abusing pythons int range...

    # count frequent items same as in Apriori
    @staticmethod
    def updateFrequency(countFrequency: dict, basket: list) -> dict:
        for item in basket:
            if item not in countFrequency:  # if item not in basket then add to basket
                countFrequency[item] = 1
            else:
                countFrequency[item] += 1  # otherwise add 1 to count
        return countFrequency  # get frequency and basket

    # used for creating a basket with frequent item - used in multi-stage PCY during pass 2
    @staticmethod
    def getFrequentBasket(frequentItems: list, basket: list) -> list:
        frequentItems = set(frequentItems)
        return [value for value in basket if value in frequentItems]  # create basket with frequent items

    # update the buckets during PCY
    def updateBucket(self, basket: list, bucket: numpy.ndarray, num=1) -> numpy.ndarray:
        pairs = itertools.combinations(basket, 2)
        for pair in pairs:
            bucketNum = PCY.hashPair(pair, num) % self.sizeOfBucket  # get hashed index
            if bucket[bucketNum] < self.support:
                bucket[bucketNum] += 1  # hash pairs to buckets and add 1
        return bucket

    # for multi-hash PCY update all the buckets together
    def multiUpdateBucket(self, basket: list, buckets: list, numberOfhashes=2) -> list:
        for i in range(numberOfhashes):
            buckets[i] = self.updateBucket(basket, buckets[i], i + 1)
        return buckets

    # turn basket into bitVector to save memory in PCY
    def getBitVector(self, bucket: numpy.ndarray) -> BitVector:
        bitVector = BitVector(self.sizeOfBucket)
        for i in range(self.sizeOfBucket):
            if bucket[i] >= self.support:  # if the bucket count meets the threshold set vector
                bitVector.setBit(i)  # set the bit vector to true
        return bitVector

    # use to turn an arbitrary number of buckets to bit vectors - used in multi stage
    def multiGetVector(self, buckets: list, numberOfhashes=2) -> list:
        bitVectors = []
        for i in range(numberOfhashes):
            bitVectors.append(self.getBitVector(buckets[i]))  # for each basket create a bitVector
        return bitVectors

    # turn count of frequent items into a set of frequent items
    def getFrequentItems(self, countFrequency: dict) -> list:
        frequentItems = list()
        for item in countFrequency.keys():  # get the items that are past the threshold
            if countFrequency[item] >= self.support:
                frequentItems.append(item)
        return frequentItems

    # get candidate pair for basic PCY
    def createCandidatePairs(self, frequentItems: list, bitVector: BitVector, num=1) -> dict:
        allPairs = itertools.combinations(frequentItems, 2)  # make pairs from frequent item
        candidatePairs = dict()
        for pair in allPairs:
            if bitVector.getBit(PCY.hashPair(pair, num) % self.sizeOfBucket):
                candidatePairs[pair] = 0
        return candidatePairs

    # generalized version of the basic PCY's createCandidatePairs algorithm - look through all bitVectors 
    # before adding a pair to candidate pair
    def createMultiCandidatePair(self, frequentItems: list, bitVectors: list):
        allPairs = itertools.combinations(frequentItems, 2)  # make pairs from frequent item
        candidatePairs = dict()
        for pair in allPairs:
            inAllVectors = True  # check if pair is all of the bit vectors
            for i in range(len(bitVectors)):
                if not bitVectors[i].getBit(PCY.hashPair(pair, i + 1) % self.sizeOfBucket):
                    inAllVectors = False
                    break
            if inAllVectors:
                candidatePairs[pair] = 0
        return candidatePairs

    # update the candidate pairs during the final pass
    @staticmethod
    def updateCandidatePairs(candidatePairs, basket):
        pairs = itertools.combinations(basket, 2)
        for pair in pairs:
            if pair in candidatePairs:
                candidatePairs[pair] += 1
        return candidatePairs

    def printPairs(self, candidatePairs):
        # print("The frequent pairs using the support " + str(self.support) + " are:")
        ans = set()
        for pair in candidatePairs.keys():
            if candidatePairs[pair] >= self.support:
                ans.add(pair)
                # print(pair)
        print("Frequent pairs found:" + str(len(ans)))

    def basicPCY(self):
        bucket = numpy.zeros(self.sizeOfBucket, dtype=int)  # bucket to count reduce candidates

        # Pass 1        
        countFrequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()
            countFrequency = PCY.updateFrequency(countFrequency, basket)  # count items
            bucket = self.updateBucket(basket, bucket)

        bitVector = self.getBitVector(bucket)
        del bucket  # no longer need after we have bit vector
        frequentItems = self.getFrequentItems(countFrequency)  # get set of frequent item
        del countFrequency

        # Pass 2 - finding the pairs
        candidatePairs = self.createCandidatePairs(frequentItems, bitVector)
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()  # split basket into items
            candidatePairs = PCY.updateCandidatePairs(candidatePairs, basket)
        self.printPairs(candidatePairs)

    def multiStagePCY(self):
        # Pass 1
        bucket1 = numpy.zeros(self.sizeOfBucket, dtype=int)
        countFrequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()
            countFrequency = PCY.updateFrequency(countFrequency, basket)  # count items
            bucket1 = self.updateBucket(basket, bucket1)

        bitVector1 = self.getBitVector(bucket1)
        del bucket1  # no longer need after we have bit vector
        frequentItems = self.getFrequentItems(countFrequency)  # get set of frequent item
        del countFrequency

        # Pass 2 - next hash function
        bucket2 = numpy.zeros(self.sizeOfBucket, dtype=int)
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()
            frequentBasket = PCY.getFrequentBasket(frequentItems, basket)
            bucket2 = self.updateBucket(frequentBasket, bucket2, 2)

        bitVector2 = self.getBitVector(bucket2)
        del bucket2  # no longer need after we have bit vector

        # Pass 3 - going through file to search for candidate pairs
        candidatePairs = self.createMultiCandidatePair(frequentItems, [bitVector1, bitVector2])
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()  # split basket into items
            candidatePairs = PCY.updateCandidatePairs(candidatePairs, basket)
        self.printPairs(candidatePairs)

        buckets = [numpy.zeros(self.sizeOfBucket, dtype=int), numpy.zeros(self.sizeOfBucket, dtype=int)]

        # Pass 1        
        countFrequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()
            countFrequency = PCY.updateFrequency(countFrequency, basket)  # count items
            buckets = self.multiUpdateBucket(basket, buckets)

        bitVectors = self.multiGetVector(buckets)  # get the list of bit vectors
        del buckets  # no longer need
        frequentItems = self.getFrequentItems(countFrequency)  # get set of frequent item
        del countFrequency

        # Pass 2 - finding the pairs
        candidatePairs = self.createMultiCandidatePair(frequentItems, bitVectors)
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()  # split basket into items
            candidatePairs = PCY.updateCandidatePairs(candidatePairs, basket)
        self.printPairs(candidatePairs)


pcy = PCY("retail.txt", 5, 100, 100000)

start = time.perf_counter()
pcy.basicPCY()
end = time.perf_counter()
print(f"Basic PCY finished in {(end - start)*1000:0.3f}ms")

start = time.perf_counter()
pcy.multiStagePCY()
end = time.perf_counter()
print(f"Multistage PCY finished in {(end - start)*1000:0.3f}ms")

start = time.perf_counter()
pcy.multiHashPCY()
end = time.perf_counter()
print(f"MultiHash PCY finished in {(end - start)*1000:0.3f}ms")