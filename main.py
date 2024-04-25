import pandas as pd
import math
import numpy as np
import json
from multiprocessing import Pool
import time
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def findEmptyData(data):
    reviewMap = {column: 0 for column in data.columns}
    print(len(reviewMap))

    for row in data.itertuples():
        rowIndex = 0
        for value in enumerate(row[1:], start=0):
            if value[1] is None:
                reviewMap[data.columns[rowIndex]] += 1
            elif isinstance(value[1], str) and (value[1].lower() == "nan" or value[1] == ""):
                reviewMap[data.columns[rowIndex]] += 1
            elif isinstance(value[1], float) and np.isnan(value[1]):
                reviewMap[data.columns[rowIndex]] += 1
            rowIndex += 1
    return reviewMap

def getattributeinfo(data):
    attributeMap = {}
    missingAttributes = []
    for row in data.itertuples():
        if isinstance(row.attributes, float): # if its a float, then that means that it is NaN, otherwise it would be a list
            missingAttributes.append(row)
        else:
            try:
                # Replacing single quotes with double quotes
                jsonstring = (row.attributes.replace('"', '').replace("u'", "'").replace("'", '"'))
                jsonstring = jsonstring.replace(' True,', ' "True",').replace(" True}", ' "True"}').replace(' False,', ' "False",').replace(" False}", '"False"}')
                jsonattributes = json.loads(jsonstring)
                for attribute in jsonattributes:
                    if attribute not in attributeMap:
                        attributeMap[attribute] = 1
                    else:
                        attributeMap[attribute] += 1
            except json.JSONDecodeError as e:
                print("Error decoding JSON:", e)
                print("JSON string:", jsonstring)
    sorted_map = dict(sorted(attributeMap.items(), key=lambda item: item[1], reverse=True))
    return sorted_map, missingAttributes


## Input the name of a city or state and returns the businesses from that area
def getbusinessesfromarea(area):
    returned_businesses = []
    with open("./data/yelp_dataset/yelp_academic_dataset_business.json", 'r', encoding='utf-8') as f:
        for line in f:
            business = json.loads(line)
            if business['city'] == area or business['state'] == area:
                returned_businesses.append(business)
    print("there are " + str(len(returned_businesses)) + " businesses based in " + area)
    return returned_businesses


def load_reviews_chunk(filename, chunk, business_ids):
    reviews = {}
    with open(filename, 'r', encoding='utf-8') as f:
        for line in chunk:
            review = json.loads(line)
            business_id = review['business_id']
            if business_id in business_ids:
                if business_id not in reviews:
                    reviews[business_id] = []
                reviews[business_id].append(review['text']) # only append the text instead of the entire review
    return reviews

def check_reviews_for_businesses(review_chunks, business_ids, filename):
    reviews = {}
    with Pool() as pool:
        results = pool.starmap(load_reviews_chunk, [(filename, chunk, business_ids) for chunk in review_chunks])
    for result in results:
        for business_id, business_reviews in result.items():
            if business_id in reviews:
                reviews[business_id].extend(business_reviews)
            else:
                reviews[business_id] = business_reviews
    return reviews

def collect_reviews_for_businesses_in_chunks(review_filename, business_ids, chunk_size=10000):
    review_chunks = []
    with open(review_filename, 'r', encoding='utf-8') as f:
        chunk = []
        for line in f:
            chunk.append(line)
            if len(chunk) == chunk_size:
                review_chunks.append(chunk)
                chunk = []
        if chunk:
            review_chunks.append(chunk)
    return check_reviews_for_businesses(review_chunks, business_ids, review_filename)

def collect_text_from_chunk_area(area, review_filename):
    business_data = getbusinessesfromarea(area)
    business_ids = [business['business_id'] for business in business_data]

    reviews = collect_reviews_for_businesses_in_chunks(review_filename, business_ids)
    print("Reviews collected:", str(len(reviews)))
    # reviews[business_id] = [ 'text', 'text', ...]
    removed_count = 0
    for business in business_data[:]: # loop through a copy becuase we are deleting things from the original list as we are going
        print("business name: " + business['name'])
        print("Attributes: " + str(business['attributes']))
        if business['business_id'] in reviews:
            print("Number of reviews " + str(len(reviews[business['business_id']])) + "\n")
        else:
            print("No reviews for this business\n")
        if business['attributes'] is None:
            removed_count += 1
            business_data.remove(business)
    print("Businesses with no attributes - removed: " + str(removed_count))
    return business_data, reviews


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    '''
    print_hi('PyCharm')
    business_data = pd.read_csv("./data/yelp_business.csv")
    print("business data size:" + str(len(business_data)))
    print(str(findEmptyData(business_data)) + "\n")
    # 28836 of them are missing attributes altogether

    attributeMap, missingAttributes = getattributeinfo(business_data)
    print("There are " + str(len(missingAttributes)) + " missing attributes in business data")
    print("business data attribute Map:")
    print(attributeMap)

    review_data = pd.read_csv("./data/yelp_review_arizona.csv")
    print("review data size:" + str(len(review_data)))
    print(findEmptyData(review_data))
    # No missing attributes
    '''
    my_business = "AZ"
    getbusinessesfromarea(my_business)
    #getreviewsfromuser("IpLRJY4CP3fXtlEd8Y4GFQ")
    #getreviewsfrombusinesses(["tUFrWirKiKi_TAnsVWINQQ", "mWMc6_wTdE0EUBKIGXDVfA", "bBDDEgkFA1Otx9Lfe7BZUQ"])
    #collect_text_from_area(my_business)

    start_time = time.time()
    # this line works for both the tip and review datasets, all they need to have is a business_id field and a text_id field
    business_data, reviews = collect_text_from_chunk_area(my_business, "./data/yelp_dataset/yelp_academic_dataset_tip.json") # input data, map of labels
    end_time = time.time()
    print("Time taken to: " + str(end_time - start_time))

    attribute_map = {}
    for business in business_data:
        for attribute in business['attributes']:
            if attribute not in attribute_map:
                attribute_map[attribute] = 0
            attribute_map[attribute] += 1
    print("attribute map: " + json.dumps(dict(sorted(attribute_map.items(), key=lambda item: item[1], reverse=True))))


# See PyCharm help at https://www.jetbrains.com/help/pycharm/

# what needs to be done
# find businesses with at least 50 reviews + tips, and at least 3 attributes
# find the most popular attributes which has enough data to be able to test on
# get all the reviews + tips and aggregate them for each business that meets attribute and review + tip threshold
# *insert some way of learning associations between textual data and attribute classification*
