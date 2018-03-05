package kuba.eai.jms.clients.common;

import javax.jms.JMSException;

public class TheTempTopic extends TheTopic implements javax.jms.TemporaryTopic {

	public TheTempTopic() {
		super("temp");		
	}

	@Override
	public void delete() throws JMSException {}

}
