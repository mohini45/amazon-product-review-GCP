#publish.py

# importing required libraries
from google.cloud import pubsub
from time import sleep
import datetime

# create a publisher object to the Pub/Sub topic
project = "data228-final-project-312006"
topic = "streaming_data_in"

publisher = pubsub.PublisherClient()
topic_path = publisher.topic_path(project, topic)

# read csv file from local
file_name = 'AmazonReviews_Streaming_Data.csv'

# read csv file line by line and print line number
with open(file_name, 'r') as csvfile:
	# Iterate over each row  in the csv
	row_no = 1
	for line in csvfile.readlines():
		line = line.rstrip('\n')
		publisher.publish(topic_path, bytes(str(line), "utf-8"))
		print(f"Publishing file object {row_no} to {topic_path} at {datetime.datetime.now()}...")
		sleep(0.5)
		row_no += 1