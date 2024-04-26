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

#When doing useful stuff, i think we want to have the measure of usefulness be useful/total_reviews, so we can get on average
# Or, who knows if the numbers on the user.json file are even accurate, i should check that - will do now
'''
def collect_users():
    users = []
    with open("./data/yelp_dataset/yelp_academic_dataset_user.json", 'r', encoding="utf-8") as f:
        for line in f:
            user = json.loads(line)
            if user["review_count"] > 150: # only append the yelpers who have made more than 150 yelps
                users.append(user["user_id"])
    print("Number of users: " + str(len(users)))
    return users

def get_reviews_from_users(users):
    user_review_text_map = {}
    user_review_avg_useful_map = {}
    with open("./data/yelp_dataset/yelp_academic_dataset_review.json", 'r', encoding="utf-8") as f:
        for line in f:
            review = json.loads(line)
            if review["user_id"] in users:
                usefulness = review["useful"]
                if review["user_id"] not in user_review_text_map:
                    user_review_text_map[review["user_id"]] = []
                    user_review_avg_useful_map[review["user_id"]] = []
                user_review_text_map[review["user_id"]].append(review["text"]) # append all text from each user in users to map list
                user_review_avg_useful_map[review["user_id"]].append(usefulness)

    for key, value in list(user_review_avg_useful_map.items())[:]:
        weighted_sum = 0
        for useful in value:
            weighted_sum += (useful*len(value))
        user_review_avg_useful_map[key] = weighted_sum/len(value)

    return user_review_text_map, user_review_avg_useful_map
'''

def collect_users():
    users = []
    with open("./data/yelp_dataset/yelp_academic_dataset_user.json", 'r', encoding="utf-8") as f:
        for line in f:
            user = json.loads(line)
            if user["review_count"] > 2000:
                users.append(user["user_id"])
    print("Number of users: " + str(len(users)))
    return users

def load_reviews_chunk(filename, chunk, users):
    start_time = time.time()
    user_review_text_map = {}
    user_review_avg_useful_map = {}
    with open(filename, 'r', encoding="utf-8") as f:
        for line in chunk:
            review = json.loads(line)
            if review["user_id"] in users:
                usefulness = review["useful"]
                if review["user_id"] not in user_review_text_map:
                    user_review_text_map[review["user_id"]] = []
                    user_review_avg_useful_map[review["user_id"]] = []
                user_review_text_map[review["user_id"]].append(review["text"])
                user_review_avg_useful_map[review["user_id"]].append(usefulness)
    '''
    for key, value in list(user_review_avg_useful_map.items())[:]:
        weighted_sum = 0
        for useful in value:
            weighted_sum += useful
        user_review_avg_useful_map[key] = weighted_sum/len(value)
    '''
    end_time = time.time()
    print("Time taken for chunk: ", end_time - start_time)
    print("Number reviews seen: " + str(len(user_review_text_map)))
    return [user_review_text_map, user_review_avg_useful_map]

def get_reviews_from_users(users):
    review_filename = "./data/yelp_dataset/yelp_academic_dataset_review.json"
    chunk_size = 15000
    with open(review_filename, 'r', encoding="utf-8") as f:
        chunks = []
        chunk = []
        start_time = time.time()
        for line in f:
            review = json.loads(line)
            if review["user_id"] in users:
                chunk.append(line)
                if len(chunk) == chunk_size:
                    chunks.append(chunk)
                    chunk = []
                    end_time = time.time()
                    print("Time taken to load chunk: ", end_time - start_time)
                    print("chunks at last position:" + str(chunks[-1][0]))
                    start_time = time.time()
        if chunk:
            chunks.append(chunk)

    with Pool() as pool:
        results = pool.starmap(load_reviews_chunk, [(review_filename, c, users) for c in chunks])
    #print(results)
    user_review_text_map = {}
    user_review_avg_useful_map = {}
    for text_map, avg_useful_map in results:
        for user_id, text_list in list(text_map.items())[:]:
            if user_id not in user_review_text_map:
                user_review_text_map[user_id] = []
                user_review_avg_useful_map[user_id] = []
            user_review_text_map[user_id].extend(text_list)
            if isinstance(user_review_avg_useful_map[user_id], float) or isinstance(user_review_avg_useful_map[user_id], int):
                user_review_avg_useful_map[user_id] = [user_review_avg_useful_map[user_id]]
            user_review_avg_useful_map[user_id].append(avg_useful_map[user_id])
        for user_id, usefuls in list(user_review_avg_useful_map.items())[:]:
            divlength = 1
            user_review_avg_useful_map[user_id] = 0 # set value to zero
            if isinstance(usefuls, float) or isinstance(usefuls, int):
                continue
            elif isinstance(usefuls[0], float) or isinstance(usefuls[0], int):
                usefuls = [[usefuls[0]]]
            else:
                divlength = len(usefuls[0])
            for useful in usefuls[0]: # strange, extra array outside
                print(str(useful) + "\n")
                print(user_review_avg_useful_map[user_id])
                user_review_avg_useful_map[user_id] += useful
            user_review_avg_useful_map[user_id] = user_review_avg_useful_map[user_id] / divlength

    return user_review_text_map, user_review_avg_useful_map

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
    '''

    users = collect_users()
    text_map, useful_map = get_reviews_from_users(users)
    # print(text_map)
    print(useful_map)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

# what needs to be done
# find businesses with at least 50 reviews + tips, and at least 3 attributes
# find the most popular attributes which has enough data to be able to test on
# get all the reviews + tips and aggregate them for each business that meets attribute and review + tip threshold
# *insert some way of learning associations between textual data and attribute classification*
