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
        self.sizeOfBucket=sizeOfBucket
        #  file with data ** need to change so whole file isn't read at once
        with open(dataFilePath,"r", encoding="utf-8") as file: 
            self.dataFile = file.read().splitlines()
        # support threshold is used for eliminating items for the algorithm
        self.support = (support/100)*len(self.dataFile) 
        # self.processedFile = False # can check if file is done being processed

    @staticmethod
    def hashPair(pair : list, num=1) -> int:
        if num ==1:
            return ((int)(pair[0]) +(int)(pair[1])) % 15485863 # Picked a random large prime
        return ((int)(pair[0]) +(int)(pair[1])) % 3060427 # abusing pythons int range...
    
    # used to create pairs from basket at the start of PCY
    def getPairsFromItems(self, lineNum:int): 
        basket = self.dataFile[lineNum]
        basket = basket.split() # split basket into items
        for item in basket:
            if item not in self.countFrequency: # if item not in basket then add to basket
                self.countFrequency[item] = 1
            else:
                self.countFrequency[item] += 1 # otherwise add 1 to count
        return itertools.combinations(basket, 2) # create pairs

    # used for creating stage in multi stage during pass 2
    def getFrequentPairs(self, lineNum:int,frequentItems): 
        basket = self.dataFile[lineNum]
        basket = basket.split() # split basket into items
        returnBasket = []
        for item in basket:
            if item in frequentItems:
                returnBasket.append(item)
        return itertools.combinations(returnBasket, 2) # create frequent pairs

    # update for basic and multiStage PCY
    def updateBitVector(self, pairs, bucket, num =1): 
        for pair in pairs:
            bucketNum = PCY.hashPair(pair, num) %self.sizeOfBucket # get hashed index
            if bucket[bucketNum]<self.support:
                bucket[bucketNum] += 1# hash pairs to buckets and add 1
            # if buckets passed threshold put in set bit vector
            if bucket[bucketNum] >= self.support:
                if num==1 or self.bitVector1.getBit(PCY.hashPair(pair,1) %self.sizeOfBucket):
                    self.bitVector.setBit(bucketNum) # set the bit vector to true
        return bucket

    # update for multiHash PCY
    def multiUpdateBitVector(self, pairs, buckets): 
        for pair in pairs:
            for i in range(2):
                bucketNum = PCY.hashPair(pair, i+1) %self.sizeOfBucket# get hashed index
                if buckets[i][bucketNum]<self.support:
                    buckets[i][bucketNum] += 1# hash pairs to buckets and add 1
                # if buckets passed threshold put in set bit vector
                if buckets[i][bucketNum] >= self.support:
                    self.bitVectors[i].setBit(bucketNum) # set the bit vector to true
        return buckets
    
    # used to get count of frequent elements
    def getFrequentItems(self):
        frequentItems = set()
        for item in self.countFrequency.keys(): # get the items that are past the threshold
            if self.countFrequency[item] >= self.support:
                frequentItems.add(item)
        del self.countFrequency
        return frequentItems

    # get candidate pair for basic or multistage version
    def getCandidatePair(self, frequentItems, num=1):
        allPairs = itertools.combinations(frequentItems, 2) # make pairs from frequent item
        candidatePairs = dict()
        for pair in allPairs:
            if self.bitVector.getBit(PCY.hashPair(pair,num) %self.sizeOfBucket) :
                if num==1 or self.bitVector1.getBit(PCY.hashPair(pair,1) %self.sizeOfBucket):
                    candidatePairs[pair] = 0
        return candidatePairs

    def getMultiCandidatePair(self, frequentItems):
        allPairs = itertools.combinations(frequentItems, 2) # make pairs from frequent item
        candidatePairs = dict()
        for pair in allPairs:
            if self.bitVectors[0].getBit(PCY.hashPair(pair,1) %self.sizeOfBucket)  and self.bitVectors[1].getBit(PCY.hashPair(pair,2) %self.sizeOfBucket):
                candidatePairs[pair] = 0
        return candidatePairs

    def printPairs(self, candidatePairs):
        print("The frequent pairs using the support " + (str)(self.support) + " are:")
        for pair in candidatePairs.keys():
            if(candidatePairs[pair] >= self.support):
                print(pair)
                # print((str)(pair) +" which appear " + (str)(candidatePairs[pair]) + " times")

    def basicPCY(self):
        bucket = numpy.zeros(self.sizeOfBucket, dtype=int)
        self.bitVector = BitVector(self.sizeOfBucket)

        # Pass 1        
        self.countFrequency = dict() # hashtable to store frequency of items
        for lineNum in range(len(self.dataFile)): # or lineNum%self.maxBuckets !=0             
            bucket = self.updateBitVector(self.getPairsFromItems(lineNum), bucket)
            print("Processing basket number " +(str)(lineNum) )
        del bucket # no longer need
        frequentItems = self.getFrequentItems() # get set of frequent item

        # Pass 2 - finding the pairs
        candidatePairs = self.getCandidatePair(frequentItems)        
        for lineNum in range(len(self.dataFile)):             
            basket = self.dataFile[lineNum].split() # split basket into items
            pairs = itertools.combinations(basket, 2)
            for pair in pairs:
                if pair in candidatePairs:
                    candidatePairs[pair] +=1

        self.printPairs(candidatePairs)
        del self.bitVector


    def multiStagePCY(self):
        bucket = numpy.zeros(self.sizeOfBucket, dtype=int)
        self.bitVector = BitVector(self.sizeOfBucket)

        # Pass 1        
        self.countFrequency = dict() # hashtable to store frequency of items
        for lineNum in range(len(self.dataFile)): # or lineNum%self.maxBuckets !=0             
            bucket = self.updateBitVector(self.getPairsFromItems(lineNum), bucket)
            print("Pass 1: Processing basket number " +(str)(lineNum) )
        del bucket # no longer need
        frequentItems = self.getFrequentItems() # get set of frequent item

        # Pass 2 - next hash function
        self.bitVector1 = self.bitVector #save bitVector1 
        bucket = numpy.zeros(self.sizeOfBucket, dtype=int)
        self.bitVector = BitVector(self.sizeOfBucket) # use for second bit vector
        for lineNum in range(len(self.dataFile)):             
            bucket = self.updateBitVector(self.getFrequentPairs(lineNum, frequentItems), bucket, 2)
            print("Pass 2: Processing basket number " +(str)(lineNum) )

        candidatePairs = self.getCandidatePair(frequentItems,2)        
        for lineNum in range(len(self.dataFile)):             
            basket = self.dataFile[lineNum].split()
            pairs = itertools.combinations(basket, 2)
            for pair in pairs:
                if pair in candidatePairs:
                    candidatePairs[pair] +=1

        self.printPairs(candidatePairs)
        del self.bitVector1
        del self.bitVector

    def multiHashPCY(self):
        buckets = [numpy.zeros(self.sizeOfBucket, dtype=int),numpy.zeros(self.sizeOfBucket, dtype=int)]
        self.bitVectors = [BitVector(self.sizeOfBucket),BitVector(self.sizeOfBucket)]

        # Pass 1        
        self.countFrequency = dict() # hashtable to store frequency of items
        for lineNum in range(len(self.dataFile)): # or lineNum%self.maxBuckets !=0             
            buckets = self.multiUpdateBitVector(self.getPairsFromItems(lineNum), buckets)
            print("Processing basket number " +(str)(lineNum) )
        del buckets # no longer need
        frequentItems = self.getFrequentItems() # get set of frequent item

        # Pass 2 - finding the pairs
        candidatePairs = self.getMultiCandidatePair(frequentItems)        
        for lineNum in range(len(self.dataFile)):             
            basket = self.dataFile[lineNum].split() # split basket into items
            pairs = itertools.combinations(basket, 2)
            for pair in pairs:
                if pair in candidatePairs:
                    candidatePairs[pair] +=1

        self.printPairs(candidatePairs)
        del self.bitVectors
                    
               
                
pcy =PCY(5, "retail.txt", 100000, 100000)

# pcy.basicPCY()

# pcy.multiStagePCY()
pcy.multiHashPCY()