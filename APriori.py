# Feb 14, 2020
# Project 1 for COMP-4250 (Big data)
# Program will take the data from given text file, break it down into chunks 
# and find frequent item-sets using the APriori algorithm

import itertools # used to create candidate pairs efficiently
import time # used to keep track of time

class APriori:
    def __init__(self, dataFilePath : str, support : int, chunk: int):
        #  store file data
        with open(dataFilePath,"r", encoding="utf-8") as file: 
            self.dataFile = file.read().splitlines()
        # support threshold is used for eliminating items for the algorithm
        self.support = ((support/100)*len(self.dataFile))*(chunk/100)
        self.lastLine = int(len(self.dataFile) * (chunk / 100)) # last line to process depending on chunk size

    def runAPriori(self):
        start = time.perf_counter()
        # Pass 1
        countFrequency = dict() # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()

            for item in basket:
                if item not in countFrequency: # if item not in basket then add to basket
                    countFrequency[item] = 1
                else:
                    countFrequency[item] += 1 # otherwise add 1 to count

        # get list of frequent items
        frequentItems = list()
        for item in countFrequency.keys(): # get the items that are past the threshold
            if countFrequency[item] >= self.support:
                frequentItems.append(item)
        del countFrequency

        # Pass 2
        allPairs = itertools.combinations(frequentItems, 2) # make pairs from frequent items

        candidatePairs = dict()
        for pair in allPairs:
            candidatePairs[pair] = 0

        for lineNum in range(self.lastLine): 
            basket = self.dataFile[lineNum].split() # split basket into items
            pairs = itertools.combinations(basket, 2)
            for pair in pairs:
                if pair in candidatePairs:
                    candidatePairs[pair] +=1
        
        # get set of frequent pairs
        frequentPairs = set()
        for item in candidatePairs.keys(): # get the pairs that are past the threshold
            if candidatePairs[item] >= self.support:
                frequentPairs.add(item)
        del candidatePairs

        end = time.perf_counter()
        print(f"APriori finished in {(end - start)*1000:0.3f}ms")
        
        print("Frequent pairs found: " + str(len(frequentPairs)))
                
ap = APriori("retail.txt", 1, 100)

ap.runAPriori()
