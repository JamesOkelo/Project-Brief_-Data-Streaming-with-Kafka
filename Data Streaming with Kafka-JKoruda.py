
#Install confluent library
!pip install confluent-kafka

#Import needed libraries
from confluent_kafka import Producer, Consumer
import json
from collections import defaultdict
from collections import OrderedDict
import random
import json
from datetime import datetime
import time
from tabulate import tabulate

import logging
#Setup logger
logging.basicConfig(filename='pipeline.log', level=logging.DEBUG)

#Using confluent cloud to host kafka instance
# Confluent Cloud configurations

bootstrap_servers = '<server_name>'
security_protocol = 'SASL_SSL'
sasl_mechanism = 'PLAIN'
sasl_plain_username = '<username>'
sasl_plain_password = '<password>'
topic = 'my_pipeline'

# Producer configuration
producer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': sasl_mechanism,
    'sasl.username': sasl_plain_username,
    'sasl.password': sasl_plain_password
}

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': sasl_mechanism,
    'sasl.username': sasl_plain_username,
    'sasl.password': sasl_plain_password,
    'group.id': 'my_consumer_group'
}

#Helper functions for the cli output table art

def border1(text):

  """
    Function used to draw the grid outline on cli output using tabulate library
  """
  data_store = [[text]]
  output = tabulate(data_store, tablefmt='grid')
  print(output)


def border2(text):

  """
    Function used to draw the fancy_grid outline upon loop exit using tabulate library
  """
  data_store = [[text]]
  output = tabulate(data_store, tablefmt='fancy_grid')
  print(output)

#Producer config

# Create the Kafka producer
producer = Producer(producer_conf)


def json_generator():

  """
  Function generates random customer data for producer to load to topic hosted on confluent cloud
  """
  x=0   #a loop counter
  dict_list = []

  try:
    while x<5:
        # Generate random transaction ID
        transaction_id = str(random.randint(10000, 99999))

        # Generate random phone numbers
        sender_phone_number = "256" + "".join(str(random.randint(0, 9)) for _ in range(9))
        receiver_phone_number = "256" + "".join(str(random.randint(0, 9)) for _ in range(9))

        # Generate random transaction amount
        transaction_amount = random.randint(1, 100000)

        # Generate random transaction time
        transaction_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Create the JSON object
        data = {
            "transaction_id": transaction_id,
            "sender_phone_number": sender_phone_number,
            "receiver_phone_number": receiver_phone_number,
            "transaction_amount": transaction_amount,
            "transaction_time": transaction_time
        }

        dict_list.append(data)
        x=x+1

        #sleep 1 second so that timestamp values vary
        time.sleep(1)
        
    
    # Produce sample CDR data to Kafka topic
    for trx in dict_list:
      # Serialize transaction data to JSON
      serialized_trx = json.dumps(trx).encode('utf-8')
      # Produce message to Kafka topic
      producer.produce(topic, key=None, value=serialized_trx)
      producer.flush()

    print('Producer posted sample transaction data to Kafka topic....')

  except Exception as e:
    err = "Producer() error - "+str(e)
    logging.debug(err)

# Create the Kafka consumer
consumer = Consumer(consumer_conf)

# Subscribe to the topic
consumer.subscribe([topic])

#The _main-pipeline_ function

def main_pipeline():

  """
  Main pipeline function that starts by producing and posting to the topic | It then enters a while loop where consumer reads messages from topic, this message data is used to analyse customer transaction spend/
  unique sender/unique recipient/ max spend/ min spend. Running stats are posted in realtime as each message is consumed
  """

  # Variables to store data
  transaction_count = 0
  total_transaction_amount = 0
  transaction_amount_histogram = defaultdict(int)
  trx_amount_tracker = {}
  unique_sender_numbers = set()
  unique_receiver_numbers = set()
  
  #produce some data first...
  json_generator()

  try:
      print("Pipeline 'While' loop started.... Use Stop[on jupyter] or Ctrl+C[in bash] to stop the loop \n")
      while True:
      
          msg = consumer.poll(1.0)

          if msg is None:
              json_generator()
              continue

          if msg.error():
              print("Consumer error: {}".format(msg.error()))
              continue

          # Process the consumed message
          message = json.loads(msg.value())
          trx_amount_tracker[message.get('transaction_id')] = int(message.get('transaction_amount'))
          transaction_amount = int(message.get('transaction_amount'))
          sender_phone_number = message.get('sender_phone_number')
          receiver_phone_number = message.get('receiver_phone_number')

          # Update tracker data
          total_transaction_amount += transaction_amount
          transaction_count += 1
          transaction_amount_histogram[transaction_amount] += 1
          unique_sender_numbers.add(sender_phone_number)
          unique_receiver_numbers.add(receiver_phone_number)

          # Print the processed/consumed message
          border1("\nProcessed Message:"+json.dumps(message, indent=4))
          print("")

          # Print the aggregated data
          print("Total of all transactions[Kshs]: ", total_transaction_amount)
          
          print("Top 5 transaction amounts table:\n")
          sorted_items = dict(sorted(trx_amount_tracker.items(),  key=lambda x: x[1], reverse=True)[:5])  
          for key,value in sorted_items.items():
                print(f"Trx amount [Kshs]: {value}: Trx ID: {key}")

          # Find the largest and smallest trx spend amount values 
          largest_value = max(trx_amount_tracker.values())
          smallest_value = min(trx_amount_tracker.values())
          # Find the corresponding keys for min, max...
          largest_keys = [key for key, value in trx_amount_tracker.items() if value == largest_value]
          smallest_keys = [key for key, value in trx_amount_tracker.items() if value == smallest_value]

          # Print the largest and smallest keys and values
          for trx in largest_keys:
            print(f"\nLargest trx amounts. Trx ID {trx} - Amount {trx_amount_tracker[trx]}")

          for trx in smallest_keys:
            print(f"Smallest trx amounts. Trx ID {trx} - Amount {trx_amount_tracker[trx]} \n")
          

          print("Number of Unique Sender Phone Numbers:", len(unique_sender_numbers))
          print("Number of Unique Receiver Phone Numbers:", len(unique_receiver_numbers))
         

  except KeyboardInterrupt:
      pass

  finally:
      border2("Loop exit - Finally...closing the consumer...")

  #close the consumer()
  consumer.close()

if __name__ == '__main__':
    # Run the data pipeline function
    main_pipeline()