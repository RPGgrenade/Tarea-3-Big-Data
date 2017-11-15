import time
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
from mr3px.csvprotocol import CsvProtocol
import re
import itertools
import csv
import string
import json
import output_to_csv

categorey_stars = dict()

class StarsPerCategory(MRJob):

    def mapper_stars_category(self, _, line):
        obj = json.loads(line)
        line = obj
        review_id = None
        categories = None
        stars = 0
        try:    #business line
            categories = line['categories']
        except: #review line
            review_id = line['user_id']
            stars = line['stars']
        business_id = line['business_id']
        if categories: #if looking at the business stuff
            symbol = 'B'
            yield business_id, [symbol, categories]
        else: #review stuff
            symbol = 'A'
            yield business_id, [symbol, review_id, stars]

    def reducer_join_business_review(self, business_id, values):
        reviews = []

        for value in values:
            if value[0] == 'A':
                reviews.append(value)
            if value[0] == 'B':
                for review in reviews:
                    full_review = review[1:] + value[1:]
                    yield business_id, full_review

    def reducer_categorize_stars(self, business_id, reviews):

        for review in reviews:
            review_id = review[0]
            stars = review[1]
            categories = review[2]
            for category in categories:
                categorey_stars[review_id] = [category, stars]
                yield review_id, [category, stars]


    def steps(self):
        return [MRStep(mapper=self.mapper_stars_category),
                MRStep(reducer=self.reducer_join_business_review),
                MRStep(reducer=self.reducer_categorize_stars)]


if __name__ == '__main__':
    start = time.time()
    StarsPerCategory.run()
    output_to_csv.make_csv('stars_per_category_30000', categorey_stars)
    end = time.time()
    print("Time: " + str(end - start) + "sec")