package kuba.eai.jms.clients.common;

import javax.jms.JMSException;

public class TheQueue extends TheDestination implements javax.jms.Queue {

	public TheQueue(String name) {
		super(name);		
	}

	@Override
	public String getQueueName() throws JMSException {
		return name;
	}	
}
