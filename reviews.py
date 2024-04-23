
# Create CSV file of reviews, user ID, and useful votes
#   Use review json file from Yelp data

import json
import csv

input_json_path = "C:/DS1310/data/reviews.json"
output_csv_path = 'reviews.csv'

data = []

# Open the JSON file with UTF-8 encoding specified
with open(input_json_path, 'r', encoding='utf-8') as json_file:
    for line in json_file:
         review = json.loads(line)
         data.append(review)


# Open a file for writing with UTF-8 encoding
with open(output_csv_path, mode='w', newline='', encoding='utf-8') as data_file:
    csv_writer = csv.writer(data_file)
    csv_writer.writerow(['user_id', 'text', 'useful'])

    for review in data:
        user_id = review.get('user_id')
        text = review.get('text')
        useful = review.get('useful')
    
        csv_writer.writerow([user_id, text, useful]) # write the row to the CSV file


print("CSV file has been created successfully.")