package com;

/**
 * Kafka Client!
 *
 */
public class App
{
    public static void main( String[] args )
    {
    	if(args[0].equals("producer"))
    	{
	        KafkaProducer producer=new KafkaProducer(args[1]);
	        producer.run();
        }
        //producer.closeProducer();
    	else if(args[0].equals("consumer"))
    	{
    		KafkaConsumer consumer=new KafkaConsumer();
    		consumer.run();
    	}
    }
}
