#!/usr/bin/python3

'''
	Author      : Himanshu Sharma
	Producer.py : To make a connection with RabbitMQ and sending data to the Queue
				  via Producer.
	Payload 	: Payload or Message that needs to be send to Queue
	Config.py   : Contains the Configuration of the RabbitMQ URl, Exchange, Routing_key
'''

from kombu import Connection, Exchange, Producer
from config import config

class RabbitMQProducer():

	def __init__(self, config, message):
		self.rabbit_url = config.get('rabbit_url')
		self.exchange_type = config.get('exchange_type')
		self.exchange = Exchange(config.get('exchange'), type=self.exchange_type, durable=True)
		self.routing_key = config.get('routing_key')
		self.payload = message

	def make_connection(self):
		return Connection(self.rabbit_url, virtual_host='/')

	def send(self):
		with self.make_connection() as connection:
			producer = Producer(connection, exchange = self.exchange, routing_key = self.routing_key)
			producer.declare()
			producer.publish(self.payload)

if __name__ == "__main__":
	message = "First Message"
	rabbitMQObject = RabbitMQProducer(config, message)
	rabbitMQObject.send()






