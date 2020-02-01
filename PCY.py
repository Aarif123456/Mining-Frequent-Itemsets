# Abdullah Arif
# Feb 14, 2020
# Project 1 for COMP-4250 (Big data)
# Program will take the data from given text file, break it down into chunks 
# and find frequent item-sets using the PCY algorithm
from BitVector import BitVector
import itertools # used to create candidate pairs efficiently
import numpy # used to create fixed sized array

class PCY:
    def __init__(self, support : int, dataFilePath : str, maxBuckets: int, sizeOfBucket : int ):
        self.maxBuckets = maxBuckets # the maximum number of baskets we will process at a time
        self.bucket = numpy.zeros(sizeOfBucket, dtype=int)
        self.bitVector = BitVector(sizeOfBucket)
        #  file with data ** need to change so whole file isn't read at once
        with open(dataFilePath,"r", encoding="utf-8") as file: 
            self.dataFile = file.read().splitlines()
        # support threshold is used for eliminating items for the algorithm
        self.support = (support/100)*len(self.dataFile) 
        # self.processedFile = False # can check if file is done being processed

    @staticmethod
    def hashPair(pair : list) -> int:
        return ((int)(pair[0]) +(int)(pair[1])) % 15485863 # Picked a random large prime

    def basicPCY(self):
        # Pass 1
        lineNum = -1
        countFrequency = dict() # hashtable to store frequency of items
        while lineNum+1 < len(self.dataFile): # or lineNum%self.maxBuckets !=0 
            lineNum+=1
            basket = self.dataFile[lineNum]
            # if not basket: # if we reached end of file stop
            #     break
            basket = basket.split() # split basket into items
            for item in basket:
                if item not in countFrequency: # if item not in basket then add to basket
                    countFrequency[item] = 1
                else:
                    countFrequency[item] += 1 # otherwise add 1 to count
            pairs = itertools.combinations(basket, 2) # create pairs
            for pair in pairs:
                bucketNum = PCY.hashPair(pair) %len(self.bucket) # get hashed index
                if self.bucket[bucketNum]<self.support:
                    self.bucket[bucketNum] += 1# hash pairs to buckets and add 1
                # if bucket passed threshold put in set bit vector
                if self.bucket[bucketNum] >= self.support:
                    self.bitVector.setBit(bucketNum) # set the bit vector to true
            print("Processing basket number " +(str)(lineNum) )
        
        bucketSize = len(self.bucket)
        del self.bucket # no longer need bucket when we have bit vector

        # get set of frequent item
        frequentItems = set()
        for item in countFrequency.keys(): # get the items that are past the threshold
            if countFrequency[item] >= self.support:
                frequentItems.add(item)
        del countFrequency

        # Pass 2 - finding the pairs
        allPairs = itertools.combinations(frequentItems, 2) # make pairs from frequent item
        candidatePairs = dict()
        for pair in allPairs:
            if self.bitVector.getBit(PCY.hashPair(pair) %bucketSize):
                candidatePairs[pair] = 0

        lineNum = -1
        while lineNum+1 < len(self.dataFile): 
            lineNum+=1
            basket = self.dataFile[lineNum]
            basket = basket.split() # split basket into items
            pairs = itertools.combinations(basket, 2)
            for pair in pairs:
                if pair in candidatePairs:
                    candidatePairs[pair] +=1


        print("The frequent pairs using the support " + (str)(self.support) + " are:")
        for pair in candidatePairs.keys():
            if(candidatePairs[pair] >= self.support):
                print((str)(pair) +" which appear " + (str)(candidatePairs[pair]) + " times")

                    
               
                
pcy =PCY(5, "retail.txt", 100000, 100000)


pcy.basicPCY()
