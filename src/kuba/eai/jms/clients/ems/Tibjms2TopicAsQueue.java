package kuba.eai.jms.clients.ems;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

import com.tibco.tibjms.TibjmsDestination;
import com.tibco.tibjms.TibjmsTopic;

public class Tibjms2TopicAsQueue implements Queue {
	
	private String q = null;
	
	public Tibjms2TopicAsQueue(String q) {
		this.q = q;
	}

	@Override
	public String getQueueName() throws JMSException {
		return q;
	}
	
	public String toString() {
		return q;
	}

	public Topic getNativeTopic() {
		return new TibjmsTopic(q);
	}

	public TibjmsDestination getNativeDestination() {
		return new TibjmsDestination(q);
	}
}
