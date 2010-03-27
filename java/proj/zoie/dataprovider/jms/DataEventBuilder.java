package proj.zoie.dataprovider.jms;

import javax.jms.Message;

import proj.zoie.api.DataConsumer.DataEvent;

public interface DataEventBuilder<T> {

	public DataEvent<T> buildDataEvent(Message message);
	
}
