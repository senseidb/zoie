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

	private static final String name = "JMSStreamDataProvider";
	private final String topicName;
	private final String clientID;
	private final TopicConnectionFactory connectionFactory;
	private final TopicFactory topicFactory;
	private final DataEventBuilder<T> dataEventBuilder;
	private TopicSubscriber subscriber;
	private TopicConnection connection;
	
	private volatile boolean stopped = true;
	

	public JMSStreamDataProvider(String topicName, String clientID,
			TopicConnectionFactory connectionFactory, TopicFactory topicFactory,
			DataEventBuilder<T> dataEventBuilder) {
		super();
		this.topicName = topicName;
		this.clientID = clientID;
		this.connectionFactory = connectionFactory;
		this.topicFactory = topicFactory;
		this.dataEventBuilder = dataEventBuilder;
	}

	@Override
	public void start() {
		log.info("starting " + toString());
		
		stopped = false;

		try {
			
			reconnect();
			
			connection.start();
			super.start();
		} catch (JMSException e) {
			throw new RuntimeException("Could nnot start JMS data provider", e);
		}
	}

	private void reconnect() throws JMSException {
		//close subscriber if not previously closed
		if (subscriber != null) {
			subscriber.close();
		}
		
		if (connection != null) {
			connection.close();
		}
		
		connection = connectionFactory.createTopicConnection();
		
		connection.setClientID(clientID);
		
		TopicSession session = connection.createTopicSession(false, 
				Session.AUTO_ACKNOWLEDGE);
		
		Topic topic = topicFactory.createTopic(topicName);
		
		subscriber = session.createDurableSubscriber(topic, name);
	}

	@Override
	public DataEvent<T> next() {
		for (;;) {
			if (stopped) {
				return null;
			}
			
			try {
				Message m = subscriber.receive();
				if (m != null) {
					return dataEventBuilder.buildDataEvent(m); 
				}
			} catch (JMSException e) {
				log.error("error receiving message", e);
				//step back for a while
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
					log.error(e1);
				}
				//try to reconnect
				try {
					reconnect();
				} catch (JMSException e1) {
					log.error("error trying to connect to JMS topic", e);
				}
			}
		}
	}

	@Override
	public void reset() {
		
	}
	
	@Override
	public void stop() {
		log.info("stopping " + toString());
		
		stopped = true;
		
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

	@Override
	public String toString() {
		return "JMSStreamDataProvider [clientID=" + clientID + ", topicName="
				+ topicName + "]";
	}
	
	

}
