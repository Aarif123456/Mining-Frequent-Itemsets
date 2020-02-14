# Abdullah Arif
# Feb 14, 2020
# Project 1 for COMP-4250 (Big data)
# Program will take the data from given text file, break it down into chunks 
# and find frequent item-sets using the PCY algorithm
from BitVector import BitVector
import itertools  # used to create candidate pairs efficiently
import numpy  # used to create fixed sized array
import time  # used to keep track of time


class PCY:
    def __init__(self, dataFilePath: str, support: int, chunk: int, sizeOfBucket: int):
        self.size_of_bucket = sizeOfBucket  # set size of bucket - the bigger the bucket the less false positives
        with open(dataFilePath, "r", encoding="utf-8") as file:
            self.dataFile = file.read().splitlines()
        # support threshold is used for eliminating items for the algorithm
        self.support = ((support / 100) * len(self.dataFile)) * (chunk / 100)
        self.lastLine = int(len(self.dataFile) * (chunk / 100))  # last line to process depending on chunk size

    def set_chunk_and_support(self, chunk: int, support: int):
        self.support = ((support / 100) * len(self.dataFile)) * (chunk / 100)
        self.lastLine = int(len(self.dataFile) * (chunk / 100))  # last line to process depending on chunk size

    @staticmethod
    def hash_pair(pair: tuple, num=1) -> int:
        if num == 1:
            return (pair[0] + pair[1]) % 50021  # Picked a random large prime
        return (pair[0] + pair[1]) % 50993  # abusing pythons int range...

    # count frequent items same as in Apriori
    @staticmethod
    def update_frequency(countFrequency: dict, basket: list) -> dict:
        for item in basket:
            if item not in countFrequency:  # if item not in basket then add to basket
                countFrequency[item] = 1
            else:
                countFrequency[item] += 1  # otherwise add 1 to count
        return countFrequency  # get frequency and basket

    # used for creating a basket with frequent item - used in multi-stage PCY during pass 2
    @staticmethod
    def get_frequent_basket(frequent_items: list, basket: list) -> list:
        frequent_items = set(frequent_items)
        return [value for value in basket if value in frequent_items]  # create basket with frequent items

    # update the buckets during PCY
    def update_bucket(self, basket: list, bucket: numpy.ndarray, num=1) -> numpy.ndarray:
        pairs = itertools.combinations(basket, 2)
        for pair in pairs:
            bucket_num = PCY.hash_pair(pair, num) % self.size_of_bucket  # get hashed index
            if bucket[bucket_num] < self.support:
                bucket[bucket_num] += 1  # hash pairs to buckets and add 1
        return bucket

    # for multi-hash PCY update all the buckets together
    def multi_update_bucket(self, basket: list, buckets: list, num_hashes=2) -> list:
        for i in range(num_hashes):
            buckets[i] = self.update_bucket(basket, buckets[i], i + 1)
        return buckets

    # turn basket into bitVector to save memory in PCY
    def get_bit_vector(self, bucket: numpy.ndarray) -> BitVector:
        bit_vector = BitVector(self.size_of_bucket)
        for i in range(self.size_of_bucket):
            if bucket[i] >= self.support:  # if the bucket count meets the threshold set vector
                bit_vector.set_bit(i)  # set the bit vector to true
        return bit_vector

    # use to turn an arbitrary number of buckets to bit vectors - used in multi stage
    def multi_get_vector(self, buckets: list, num_hashes=2) -> list:
        bit_vectors = []
        for i in range(num_hashes):
            bit_vectors.append(self.get_bit_vector(buckets[i]))  # for each basket create a bitVector
        return bit_vectors

    # turn count of frequent items into a set of frequent items
    def get_frequent_items(self, countFrequency: dict) -> list:
        frequent_items = list()
        for item in countFrequency.keys():  # get the items that are past the threshold
            if countFrequency[item] >= self.support:
                frequent_items.append(item)
        return frequent_items

    # get candidate pair for basic PCY
    def create_candidate_pairs(self, frequentItems: list, bitVector: BitVector, num=1) -> dict:
        all_pairs = itertools.combinations(frequentItems, 2)  # make pairs from frequent item
        candidate_pairs = dict()
        for pair in all_pairs:
            if bitVector.get_bit(PCY.hash_pair(pair, num) % self.size_of_bucket):
                candidate_pairs[pair] = 0
        return candidate_pairs

    # generalized version of the basic PCY's create_candidate_pairs algorithm - look through all bitVectors
    # before adding a pair to candidate pair
    def create_multi_candidate_pairs(self, frequentItems: list, bitVectors: list):
        all_pairs = itertools.combinations(frequentItems, 2)  # make pairs from frequent item
        candidate_pairs = dict()
        for pair in all_pairs:
            in_all_vectors = True  # check if pair is all of the bit vectors
            for i in range(len(bitVectors)):
                if not bitVectors[i].get_bit(PCY.hash_pair(pair, i + 1) % self.size_of_bucket):
                    in_all_vectors = False
                    break
            if in_all_vectors:
                candidate_pairs[pair] = 0
        return candidate_pairs

    # update the candidate pairs during the final pass
    @staticmethod
    def update_candidate_pairs(candidatePairs, basket):
        pairs = itertools.combinations(basket, 2)
        for pair in pairs:
            if pair in candidatePairs:
                candidatePairs[pair] += 1
        return candidatePairs

    def print_pairs(self, candidatePairs):
        # print("The frequent pairs using the support " + str(self.support) + " are:")
        ans = set()
        for pair in candidatePairs.keys():
            if candidatePairs[pair] >= self.support:
                ans.add(pair)
                # print(pair)
        print("Frequent pairs found:" + str(len(ans)))

    def basic_pcy(self):
        bucket = numpy.zeros(self.size_of_bucket, dtype=int)  # bucket to count reduce candidates

        # Pass 1
        count_frequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]
            count_frequency = PCY.update_frequency(count_frequency, basket)  # count items
            bucket = self.update_bucket(basket, bucket)

        bit_vector = self.get_bit_vector(bucket)
        del bucket  # no longer need after we have bit vector
        frequent_items = self.get_frequent_items(count_frequency)  # get set of frequent item
        del count_frequency

        # Pass 2 - finding the pairs
        candidate_pairs = self.create_candidate_pairs(frequent_items, bit_vector)
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]  # split basket into items
            candidate_pairs = PCY.update_candidate_pairs(candidate_pairs, basket)
        self.print_pairs(candidate_pairs)

    def multi_stage_pcy(self):
        # Pass 1
        bucket1 = numpy.zeros(self.size_of_bucket, dtype=int)
        count_frequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]
            count_frequency = PCY.update_frequency(count_frequency, basket)  # count items
            bucket1 = self.update_bucket(basket, bucket1)

        bit_vector1 = self.get_bit_vector(bucket1)
        del bucket1  # no longer need after we have bit vector
        frequent_items = self.get_frequent_items(count_frequency)  # get set of frequent item
        del count_frequency

        # Pass 2 - next hash function
        bucket2 = numpy.zeros(self.size_of_bucket, dtype=int)
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]
            frequent_basket = PCY.get_frequent_basket(frequent_items, basket)
            bucket2 = self.update_bucket(frequent_basket, bucket2, 2)

        bit_vector2 = self.get_bit_vector(bucket2)
        del bucket2  # no longer need after we have bit vector

        # Pass 3 - going through file to search for candidate pairs
        candidate_pairs = self.create_multi_candidate_pairs(frequent_items, [bit_vector1, bit_vector2])
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]  # split basket into items
            candidate_pairs = PCY.update_candidate_pairs(candidate_pairs, basket)
        self.print_pairs(candidate_pairs)

    def multi_hash_pcy(self):
        buckets = [numpy.zeros(self.size_of_bucket, dtype=int), numpy.zeros(self.size_of_bucket, dtype=int)]

        # Pass 1
        count_frequency = dict()  # hashtable to store frequency of items
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]
            count_frequency = PCY.update_frequency(count_frequency, basket)  # count items
            buckets = self.multi_update_bucket(basket, buckets)

        bit_vectors = self.multi_get_vector(buckets)  # get the list of bit vectors
        del buckets  # no longer need
        frequent_items = self.get_frequent_items(count_frequency)  # get set of frequent item
        del count_frequency

        # Pass 2 - finding the pairs
        candidate_pairs = self.create_multi_candidate_pairs(frequent_items, bit_vectors)
        for lineNum in range(self.lastLine):
            basket = [int(n) for n in self.dataFile[lineNum].split()]  # split basket into items
            candidate_pairs = PCY.update_candidate_pairs(candidate_pairs, basket)
        self.print_pairs(candidate_pairs)


pcy = PCY("retail.txt", 1, 100, 50000)

start = time.perf_counter()
pcy.basic_pcy()
end = time.perf_counter()
print(f"Basic PCY finished in {(end - start) * 1000:0.3f}ms")

start = time.perf_counter()
pcy.multi_stage_pcy()
end = time.perf_counter()
print(f"Multistage PCY finished in {(end - start) * 1000:0.3f}ms")

start = time.perf_counter()
pcy.multi_hash_pcy()
end = time.perf_counter()
print(f"MultiHash PCY finished in {(end - start) * 1000:0.3f}ms")
