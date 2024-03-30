import requests
import json
from kafka import KafkaProducer
import csv

#kafkasetup
# producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
# topic_name = 'user_data'

url = 'https://randomuser.me/api/'

def fetch_data_and_send():
    response = requests.get(url)

    if response.status_code == 200:
        data  = response.json()
        results = data["results"][0]

        return results
    
def create_final_json(results):
    """
    Creates the final JSON to be sent to Kafka topic only with necessary keys
    """
    kafka_data = {}

    kafka_data["full_name"] = f"{results['name']['title']}. {results['name']['first']} {results['name']['last']}"
    kafka_data["gender"] = results["gender"]
    kafka_data["location"] = f"{results['location']['street']['number']}, {results['location']['street']['name']}"
    kafka_data["city"] = results['location']['city']
    kafka_data["country"] = results['location']['country']
    kafka_data["postcode"] = int(results['location']['postcode'])
    kafka_data["latitude"] = float(results['location']['coordinates']['latitude'])
    kafka_data["longitude"] = float(results['location']['coordinates']['longitude'])
    kafka_data["email"] = results["email"]

    return kafka_data
    
def start_streaming():

    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
    topic_name = 'users_data_topic'

    results = fetch_data_and_send()
    kafka_data = create_final_json(results)
    producer.send(topic_name, json.dumps(kafka_data).encode('utf-8')) 
    producer.flush()  # Ensure the message is sent

if __name__ == '__main__':
    start_streaming()
