package proj.zoie.dataprovider.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public class JMSStreamDataProvider<T> extends StreamDataProvider<T> {
	
	private static final Logger log = Logger.getLogger(JMSStreamDataProvider.class);

	private final String name;
	private final TopicConnectionFactory connectionFactory;
	private final TopicFactory topicFactory;
	private final DataEventBuilder<T> dataEventBuilder;
	private TopicSubscriber subscriber;
	private TopicConnection connection;
	

	public JMSStreamDataProvider(String name,
			TopicConnectionFactory connectionFactory, TopicFactory topicFactory,
			DataEventBuilder<T> dataEventBuilder) {
		super();
		this.name = name;
		this.connectionFactory = connectionFactory;
		this.topicFactory = topicFactory;
		this.dataEventBuilder = dataEventBuilder;
	}

	@Override
	public void start() {
		try {
			connection = connectionFactory.createTopicConnection();
			
			TopicSession session = connection.createTopicSession(false, 
					Session.AUTO_ACKNOWLEDGE);
			
			Topic topic = topicFactory.createTopic();
			
			subscriber = session.createDurableSubscriber(topic, name);
			
			connection.start();
			super.start();
		} catch (JMSException e) {
			throw new RuntimeException("Could nnot start JMS data provider", e);
		}
	}

	@Override
	public DataEvent<T> next() {
		for (;;) {
			try {
				Message m = subscriber.receive();
				return dataEventBuilder.buildDataEvent(m); 
			} catch (JMSException e) {
				log.error("error receiving message", e);
				//step back for a while
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					log.error(e1);
				}
			}
		}
	}

	@Override
	public void reset() {
	}
	
	@Override
	public void stop() {
		try {
			connection.stop();
		} catch (JMSException e) {
			log.error("could not stop connection", e);
		}
		
		try {
			connection.close();
		} catch (JMSException e) {
			log.error("could not close connection", e);
		}
		
		super.stop();
	}

}
