package kuba.eai.jms.clients.common;

import javax.jms.JMSException;

public class TheTopic extends TheDestination implements javax.jms.Topic {

	public TheTopic(String name) {
		super(name);		
	}

	@Override
	public String getTopicName() throws JMSException {
		return name;
	}

}
