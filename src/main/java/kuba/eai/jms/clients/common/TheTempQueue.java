package kuba.eai.jms.clients.common;

import javax.jms.JMSException;

public class TheTempQueue extends TheQueue implements javax.jms.TemporaryQueue {

	public TheTempQueue() {
		super("temp");		
	}

	@Override
	public void delete() throws JMSException {}

}
