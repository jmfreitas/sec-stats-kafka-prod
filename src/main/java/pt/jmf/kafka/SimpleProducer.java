
package pt.jmf.kafka;


import java.util.Properties;
import java.util.Date;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;



public class SimpleProducer
{
	
	private static Producer<String, String> producer;
	
	
	public SimpleProducer()
	{
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.1.77:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "0");
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	
	public static void main(String[] args)
	{
		int argsCount = args.length;
		if (argsCount < 2)
			throw new IllegalArgumentException("Please provide topic name and message count as argument");
		
		String topic = args[0];
		String count = args[1];
		
		int msgCount = 0;
		try {
			msgCount = Integer.parseInt(count);
		}
		catch (NumberFormatException ex) {
			throw new IllegalArgumentException("Second argument (message count) must be integer");
		}
		System.out.println("Topic    : " + topic);
		System.out.println("Msg Count: " + msgCount);
		
		
		SimpleProducer producer = new SimpleProducer();
		producer.publishMessage(topic, msgCount);
	}
	
	
	
	private void publishMessage(String topic, int msgCount)
	{
		for (int i = 0; i < msgCount; i++)
		{
			String date = new Date().toString();
			String content = "Published Message date - " + date;
			
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, content); 
			System.out.println("Sending message: " + content);
			// publish the message to the topic
			try {
				producer.send(msg);
			}
			catch (Exception e)
			{
				System.out.println("Error sending message!");
			}
		}
		// close connection to broker
		producer.close();
			
	}
	
	
	
}