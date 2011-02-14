package proj.zoie.dataprovider.jms;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.api.ZoieException;

@RunWith(MockitoJUnitRunner.class)
public class TestJMSStreamDataProvider {

	@Mock
	TopicConnectionFactory connectionFactory;
	
	@Mock
	TopicFactory topicFactory;
	
	@Mock
	DataEventBuilder<Object, DefaultZoieVersion> dataEventBuilder;
	
	@Mock
	TopicSubscriber subscriber;
	
	@Mock
	TopicSession session;
	
	@Mock
	TopicConnection connection;
	
	@Mock
	Message message;
	
	JMSStreamDataProvider<Object, DefaultZoieVersion> provider;
	
	@Before
	public void setUpJMSStreamDataProvider() throws JMSException {
		
		final DefaultZoieVersionFactory zvf = new DefaultZoieVersionFactory();
		final AtomicLong v = new AtomicLong(0);
		
		when(dataEventBuilder.buildDataEvent(any(Message.class)))
		.thenAnswer(new Answer<DataEvent<Object, DefaultZoieVersion>>() {
			@Override
			public DataEvent<Object, DefaultZoieVersion> answer(
					InvocationOnMock invocation) throws Throwable {
				return new DataEvent<Object, DefaultZoieVersion>(new Object(),
						zvf.getZoieVersion(String.valueOf(v.incrementAndGet())));
			}
		});
		
		provider =
			new JMSStreamDataProvider<Object, DefaultZoieVersion>("topic", "clientID", connectionFactory, 
					topicFactory, dataEventBuilder);
	}
	
	/**
	 * The case, when JMS returns messages without problems
	 * @throws JMSException
	 * @throws ZoieException 
	 */
	@Test
	public void testSuccessfulNext() throws JMSException, ZoieException {

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
		
		final AtomicBoolean failed = new AtomicBoolean(false);
		
		provider.setDataConsumer(new DataConsumer<Object, DefaultZoieVersion>() {
			
			private volatile DefaultZoieVersion version = null;
			private long v = 1;

			@Override
			public void consume(
					Collection<proj.zoie.api.DataConsumer.DataEvent<Object, DefaultZoieVersion>> data)
					throws ZoieException {
				for (DataEvent<Object, DefaultZoieVersion> e : data) {
					if (e.getVersion().getVersionId() != v) {
						failed.set(true);
					}
					v++;
					version = e.getVersion();
				}
			}

			@Override
			public DefaultZoieVersion getVersion() {
				return version;
			}
		}
		);
		
		provider.start();
		
		DefaultZoieVersion versionToSync = new DefaultZoieVersion();
		versionToSync.setVersionId(100);
		
		provider.syncWithVersion(5000, versionToSync);
		
		assertFalse(failed.get());
		
		provider.stop();
		
	}
	
	/**
	 * The case, when JMS throws exception
	 * @throws JMSException 
	 * @throws ZoieException 
	 */
	@Test
	public void testExceptionRecovery() throws JMSException, ZoieException {

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
		
		final AtomicBoolean failed = new AtomicBoolean(false);
		
		provider.setDataConsumer(new DataConsumer<Object, DefaultZoieVersion>() {
			
			private volatile DefaultZoieVersion version = null;
			private long v = 1;

			@Override
			public void consume(
					Collection<proj.zoie.api.DataConsumer.DataEvent<Object, DefaultZoieVersion>> data)
					throws ZoieException {
				for (DataEvent<Object, DefaultZoieVersion> e : data) {
					if (e.getVersion().getVersionId() != v) {
						failed.set(true);
					}
					v++;
					version = e.getVersion();
				}
			}

			@Override
			public DefaultZoieVersion getVersion() {
				return version;
			}
		}
		);
		
		provider.start();
		
		DefaultZoieVersion versionToSync = new DefaultZoieVersion();
		versionToSync.setVersionId(100);
		
		provider.syncWithVersion(5000, versionToSync);
		
		assertFalse(failed.get());
		
		provider.stop();
		
	}
	
}
