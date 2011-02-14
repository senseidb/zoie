package proj.zoie.dataprovider.jms;

import javax.jms.JMSException;
import javax.jms.Message;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.DataConsumer.DataEvent;

public interface DataEventBuilder<T, V extends ZoieVersion> {

	public DataEvent<T, V> buildDataEvent(Message message) throws JMSException;
	
}
