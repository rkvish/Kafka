package com.kafka.producer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;
import java.util.Date;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 * This is plain java class publishing fake messages to Kafka Topic.
 * 
 * Usage : java -cp KafkaProducer-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.kafka.producer.KafkaProducer tweets 10 localhost:9092 0
 */

public class KafkaProducer {
	public static void main(String[] args) {
		try {
			if (args.length < 4) {
				System.out
						.println("Usage :  java -cp <Jar File Name> <Class Name> <Topic Name> <Sleep Seconds> <Broker List> <ack : 1 / 0 /-1>");
				System.exit(0);
			}
			// Capture the arguments
			String topic = args[0];
			int sleepSec = Integer.parseInt(args[1]);
			String broker = args[2];
			String ack = args[3];

			// Print the arguments
			System.out.println(" topic - " + topic);
			System.out.println(" sleepSec - " + sleepSec);
			System.out.println(" zk --" + broker);
			System.out.println(" ack - " + ack);

			// Get random number
			Random rn = new Random();
			int rnum = 5000;
			
			DateFormat dateFormat = new SimpleDateFormat("yyyMMddHHmmss");

			// Set Kafka properties
			Properties props = new Properties();
			props.put("metadata.broker.list", broker);
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("request.required.acks", ack);

			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(
					config);

			while (true) {

				StringBuilder buffer = new StringBuilder();

				// Generate fake messages
				buffer.append("Test data" + rn.nextInt(rnum))
					  .append(" generated " + rn.nextInt(rnum))
					  .append(" at "+dateFormat.format(new Date()));

				KeyedMessage<String, String> msg = new KeyedMessage<String, String>(
						topic, buffer.toString());
				producer.send(msg);
				buffer.setLength(0);

				Thread.sleep(sleepSec);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
