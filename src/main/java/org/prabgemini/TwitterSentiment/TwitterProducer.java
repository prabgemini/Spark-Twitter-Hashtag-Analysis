package org.prabgemini.TwitterSentiment;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.*;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;



public class TwitterProducer {
	private static final String topic = "twitter-topic";

	
	public static void run() throws InterruptedException {

		//Keys and tokens for connecting Twitter server
		String consumerKey = "Jzp0bOisECssdJs0H406uKKEW";
	    String consumerSecret = "y87gTNI11AtGpjIqVrSkON5kn5IW05mutH53KqbhogsUY25W3R";
	    String accessToken = "57676049-I4twV5T0bSCov74eJLIjZUcnnZc4MxGVLuLktUWPZ";
	    String accessTokenSecret = "8ULFKtomSdXMT2ppO1i3VbJYfC1Q8rm4TjjqFMmopDM3H";
	    
	    //Set properties for Kafka producer
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "52.171.131.173:9092");
		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
	    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		//create a new object of KafkaProducer
		Producer<String, String> produce = new KafkaProducer <>(properties);
		
		
		//Twitter stuffs: create a queue to hold the tweets temporarily
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(Lists.newArrayList("#bigdata"));
		//Authntcate to Twitter
		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,accessTokenSecret);
		

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, queue.take());
			System.out.println("Message: " + queue.take());
			produce.send(record);
		}
		produce.close();
		client.stop();

	}

	public static void main(String[] args) {
		try {
			TwitterProducer.run();
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}
