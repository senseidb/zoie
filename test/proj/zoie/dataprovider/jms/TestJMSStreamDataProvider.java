package proj.zoie.dataprovider.jms;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import proj.zoie.api.DataConsumer.DataEvent;

@RunWith(MockitoJUnitRunner.class)
public class TestJMSStreamDataProvider {

	@Mock
	TopicConnectionFactory connectionFactory;
	
	@Mock
	TopicFactory topicFactory;
	
	@Mock
	DataEventBuilder<Object> dataEventBuilder;
	
	@Mock
	TopicSubscriber subscriber;
	
	@Mock
	TopicSession session;
	
	@Mock
	TopicConnection connection;
	
	@Mock
	Message message;
	
	JMSStreamDataProvider<Object> provider;
	
	@Before
	public void setUpJMSStreamDataProvider() throws JMSException {
		
		when(dataEventBuilder.buildDataEvent(any(Message.class)))
			.thenReturn(new DataEvent<Object>(0, new Object()));
		
		provider =
			new JMSStreamDataProvider<Object>("topic", "clientID", connectionFactory, 
					topicFactory, dataEventBuilder);
	}
	
	/**
	 * The case, when JMS returns messages without problems
	 * @throws JMSException
	 */
	@Test
	public void testSuccessfulNext() throws JMSException {

		//stub successful connection
		when(connectionFactory.createTopicConnection())
			.thenReturn(connection);
		when(connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE))
			.thenReturn(session);
		when(session.createDurableSubscriber(any(Topic.class), anyString()))
			.thenReturn(subscriber);
		
		//stub successful receiving messages
		when(subscriber.receive())
			.thenReturn(message, message, message);
		
		provider.start();
		
		for (int i = 0; i < 3; i++) {
			//make sure events are generated
			assertNotNull(provider.next());
		}
		
		//make sure event builder has been called 3 times
		verify(dataEventBuilder, times(3)).buildDataEvent(any(Message.class));
		
		provider.stop();
		
	}
	
	/**
	 * The case, when JMS throws exception
	 * @throws JMSException 
	 */
	@Test
	public void testExceptionRecovery() throws JMSException {

		//stub some problems with connecting to topic, but
		//at the end connect anyway
		when(connectionFactory.createTopicConnection())
		.thenReturn(connection)
		.thenThrow(new JMSException("some problem 4"))
		.thenReturn(connection);  //the last stubbed value will be repeatedly returned
		when(connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE))
		.thenReturn(session)
		.thenThrow(new JMSException("some problem 5"))
		.thenReturn(session);
		when(session.createDurableSubscriber(any(Topic.class), anyString()))
		.thenReturn(subscriber)
		.thenThrow(new JMSException("some problem 6"))
		.thenReturn(subscriber); //the last stubbed value will be repeatedly returned
		
		//stub some problems with receiving messages, 
		//in total receive 3 messages
		when(subscriber.receive())
		.thenThrow(new JMSException("some problem 0"))
		.thenReturn(message)
		.thenThrow(new JMSException("some problem 1"))
		.thenReturn(message)
		.thenThrow(new JMSException("some problem 2"))
		.thenThrow(new JMSException("some problem 3"))
		.thenReturn(message);  //the last stubbed value will be repeatedly returned
		
		//for testing set back off time very small
		provider.setJMSErrorBackOffTime(1);
		
		provider.start();
		
		for (int i = 0; i < 3; i++) {
			//make sure events are generated
			assertNotNull(provider.next());
		}
		
		provider.stop();
		
	}
	
}
