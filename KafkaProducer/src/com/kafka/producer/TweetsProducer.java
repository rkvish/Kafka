package com.kafka.producer;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 * Usage : java -cp <jar> <className> <topicName> <sleepSec> <broker(List)> <ack>
 * Usage : java -cp KafkaProducer-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.kafka.producer.TweetsProducer tweets 10 localhost:9092 0
 */

public class TweetsProducer {
	public static void main(String[] args) {
		try{
            	if(args.length < 4 )
            	{
                    System.out.println("Usage :  java -cp <Jar File Name> <Class Name> <Topic Name> <Sleep Seconds> <Broker List> <ack : 1 / 0 /-1>");
                    System.exit(0);
            	}
            	//Capture the arguments
            	String topic = args[0];
				int sleepSec = Integer.parseInt(args[1]);
				String broker = args[2];
				String ack = args[3];
				
				//Print the arguments
				System.out.println(" topic - "+topic);
				System.out.println(" sleepSec - "+sleepSec);
				System.out.println(" zk --"+broker);
				System.out.println(" ack - "+ack);
				
				//Get random number
				Random rn = new Random();
				int rnum = 5000;
				
				//Get time
				//long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
				//long end = Timestamp.valueOf("2013-12-31 00:00:00").getTime();
				//long diff = end - offset + 1;
				
				//Set Kafka properties
				Properties props = new Properties();
		        props.put("metadata.broker.list", broker);
		        props.put("serializer.class", "kafka.serializer.StringEncoder");
		        props.put("request.required.acks", ack);
        
		        ProducerConfig config = new ProducerConfig(props);
		        Producer<String, String> producer = new Producer<String, String>(config);
		        
		        while(true)
		        {
				    //Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
				    StringBuilder buffer = new StringBuilder();
				    
				    //Generate fake messages
				    buffer.append("test"+rn.nextInt(rnum)+" ")
				    	  .append("abc"+rn.nextInt(rnum));      
				    
				    KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, buffer.toString());
			    	producer.send(msg);
			    	//producer.send(message);
			    	buffer.setLength(0);
			    	
			    	Thread.sleep(sleepSec);
		        
		        }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
