package com.example.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttKafkaBridge implements MqttCallback {
	private MqttAsyncClient mqtt;
	private KafkaProducer<String, String> kafkaProducer;
	private Properties props = new Properties();
	
	public void start() {
		props.setProperty("bootstrap.servers", "localhost:9092");
		//props.setProperty("metadata.broker.list", Config.getProperty(Property.RTBUILDER_KAFKA_SUB_SERVERS_URI_LIST, Property.STRING));
		//props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
		props.setProperty("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		//String pubServerURI = "tcp://172.30.66.127:1883";
		String pubServerURI = "tcp://172.30.66.240:1883";
		String clientId = "mqttKafkaBridge";
		
		//String[] mqttTopicFilters = {"car_topic"};
//		String[] mqttTopicFilters = {"cars_1"};
		String[] mqttTopicFilters = {"cars_4"};
		
		
		System.out.println("ClientId       : "  + clientId);
		System.out.println("PubServerURI   : "  + pubServerURI);
		
		try {
			this.connect(pubServerURI, clientId);
		} catch (MqttException e) {
			e.printStackTrace();
		}
		
		System.out.println("MqttTopicFilters : "	+ Arrays.toString(mqttTopicFilters));
		try {
			this.subscribe(mqttTopicFilters);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}
	
	private void connect(String pubServerURI, String clientId) throws MqttException {
		mqtt = new MqttAsyncClient(pubServerURI, clientId);
		mqtt.setCallback(this);
		IMqttToken token = mqtt.connect();

		kafkaProducer = new KafkaProducer<String,String>(props);
		token.waitForCompletion();
		
		System.out.println("Connected to MQTT and Kafka");
	}

	public void reconnect() throws MqttException {
		IMqttToken token = mqtt.connect();
		token.waitForCompletion();
	}

	public void subscribe(String[] mqttTopicFilters) throws MqttException {
		int[] qos = new int[mqttTopicFilters.length];
		for (int i = 0; i < qos.length; ++i) {
			qos[i] = 0;
			
		}
		mqtt.subscribe(mqttTopicFilters, qos);
	}

	@Override
	public void connectionLost(Throwable cause) {
		while (true) {
			try {
				System.out.println("Attempting to reconnect to MQTT server");
				reconnect();
				System.out.println("Reconnected to MQTT server, resuming");
				return;
			} catch (MqttException e) {
				// logger.warn("Reconnect failed, retrying in 10 seconds", e);
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		byte[] payload = message.getPayload();
		System.out.println(topic);
		System.out.println(message);
		kafkaProducer.send(new ProducerRecord<String, String>(topic, new String(payload)));
	}
}
