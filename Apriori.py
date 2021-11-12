# Feb 14, 2020
# Project 1 for COMP-4250 (Big data)
# Program will take the data from given text file, break it down into chunks 
# and find frequent item-sets using the APriori algorithm

import itertools  # used to create candidate pairs efficiently
import time  # used to keep track of time


class Apriori:
    def __init__(self, dataFilePath: str, support: int, chunk: int):
        #  store file data
        with open(dataFilePath, "r", encoding="utf-8") as file:
            self.dataFile = file.read().splitlines()
        # support threshold is used for eliminating items for the algorithm
        self.support = ((support / 100) * len(self.dataFile)) * (chunk / 100)
        self.lastLine = int(len(self.dataFile) * (chunk / 100))  # last line to process depending on chunk size

    def run_apriori(self):
        start = time.perf_counter()
        # Pass 1
        count_frequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()

            for item in basket:
                if item not in count_frequency:  # if item not in basket then add to basket
                    count_frequency[item] = 1
                else:
                    count_frequency[item] += 1  # otherwise add 1 to count

        # get list of frequent items
        frequent_items = list()
        for item in count_frequency.keys():  # get the items that are past the threshold
            if count_frequency[item] >= self.support:
                frequent_items.append(item)
        del count_frequency

        # Pass 2
        all_pairs = itertools.combinations(frequent_items, 2)  # make pairs from frequent items

        candidate_pairs = dict()
        for pair in all_pairs:
            candidate_pairs[pair] = 0

        for lineNum in range(self.lastLine):
            basket = self.dataFile[lineNum].split()  # split basket into items
            pairs = itertools.combinations(basket, 2)
            for pair in pairs:
                if pair in candidate_pairs:
                    candidate_pairs[pair] += 1

        # get set of frequent pairs
        frequent_pairs = set()
        for item in candidate_pairs.keys():  # get the pairs that are past the threshold
            if candidate_pairs[item] >= self.support:
                frequent_pairs.add(item)
        del candidate_pairs

        end = time.perf_counter()
        print(f"Apriori finished in {(end - start) * 1000:0.3f}ms")

        print("Frequent pairs found: " + str(len(frequent_pairs)))


ap = Apriori("retail.txt", 1, 100)

ap.run_apriori()
