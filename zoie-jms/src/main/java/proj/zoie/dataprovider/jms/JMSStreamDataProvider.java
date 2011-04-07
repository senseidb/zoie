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

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public class JMSStreamDataProvider<T, V extends ZoieVersion> extends StreamDataProvider<T, V> {
	
	private static final Logger logger = Logger.getLogger(JMSStreamDataProvider.class);

	private static final String name = "JMSStreamDataProvider";
	private final String topicName;
	private final String clientID;
	private final TopicConnectionFactory connectionFactory;
	private final TopicFactory topicFactory;
	private final DataEventBuilder<T, V> dataEventBuilder;
	private TopicSubscriber subscriber;
	private TopicConnection connection;
	
	private volatile int JMSErrorBackOffTime = 3000;  
	
	private volatile boolean stopped = true;
	

	public JMSStreamDataProvider(String topicName, String clientID,
			TopicConnectionFactory connectionFactory, TopicFactory topicFactory,
			DataEventBuilder<T, V> dataEventBuilder) {
		super();
		this.topicName = topicName;
		this.clientID = clientID;
		this.connectionFactory = connectionFactory;
		this.topicFactory = topicFactory;
		this.dataEventBuilder = dataEventBuilder;
	}

	@Override
	public void start() {
		logger.info("starting " + toString());
		
		stopped = false;

		super.start();
	}

	/**
	 * Tries to reconnect to the durable topic. This method blocks
	 * until the try is successful or this provider is stopped. 
	 */
	private void reconnect() {
		for (;;) {
			
			if (stopped) {
				return;
			}
			try {
				//close subscriber if not previously closed
				if (subscriber != null) {
					subscriber.close();
				}
				if (connection != null) {
					connection.close();
				}
				connection = connectionFactory.createTopicConnection();
				if (clientID != null) {
					connection.setClientID(clientID);
				}
				TopicSession session = connection.createTopicSession(false, 
						Session.AUTO_ACKNOWLEDGE);
				Topic topic = topicFactory.createTopic(topicName);
				subscriber = session.createDurableSubscriber(topic, name);
				connection.start();
				return;
			} catch (JMSException e) {
				logger.error("could not connect to durable topic, topic: " + topicName, e);
				//step back for a while
				backOffAfterJMSException(e);
			}
		}
		
	}

	@Override
	public DataEvent<T, V> next() {
		for (;;) {
			if (subscriber == null) {
				reconnect();
			}
			if (stopped) {
				return null;
			}
			try {
				Message m = subscriber.receive();
				if (m != null) {
					return dataEventBuilder.buildDataEvent(m); 
				}
			} catch (JMSException e) {
				logger.error("error receiving message", e);
				//step back for a while
				backOffAfterJMSException(e);
				//after any JMS exception try to reconnect
				reconnect();
			}
		}
	}
	
	/**
	 * Backs off certain amount of time before next interaction
	 * with JMS.  
	 * @param e
	 */
	void backOffAfterJMSException(JMSException e) {
		try {
			Thread.sleep(JMSErrorBackOffTime);
		} catch (InterruptedException e1) {
			logger.error(e1);
		}
	}

	public int getJMSErrorBackOffTime() {
		return JMSErrorBackOffTime;
	}

	public void setJMSErrorBackOffTime(int jMSErrorBackOffTime) {
		JMSErrorBackOffTime = jMSErrorBackOffTime;
	}

	@Override
	public void reset() {	
	  logger.error("reset called, not implemented by JMS data provider...");
	}
	
	@Override
	public void setStartingOffset(V version){
	  logger.error("starting offset called, not implemented by JMS data provider...");
	}
	
	@Override
	public void stop() {
		logger.info("stopping " + toString());
		stopped = true;
		try {
			connection.stop();
		} catch (JMSException e) {
			logger.error("could not stop connection", e);
		}
		try {
			connection.close();
		} catch (JMSException e) {
			logger.error("could not close connection", e);
		}
		super.stop();
	}

	@Override
	public String toString() {
		return "JMSStreamDataProvider [clientID=" + clientID + ", topicName="
				+ topicName + "]";
	}
	
	

}
