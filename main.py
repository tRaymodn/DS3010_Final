import pandas as pd
import math
import numpy as np
import json
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

    return attributeMap, missingAttributes


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
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


# See PyCharm help at https://www.jetbrains.com/help/pycharm/

# what needs to be done
# find businesses with at least 50 reviews + tips, and at least 3 attributes
# find the most popular attributes which has enough data to be able to test on
# get all the reviews + tips and aggregate them for each business that meets attribute and review + tip threshold
# *insert some way of learning associations between textual data and attribute classification*
