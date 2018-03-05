package kuba.eai.jms.clients.ems;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

public class Tibjms2 {
	
	public final static boolean isTopicAsQueue(Destination d) {
		return d instanceof Tibjms2TopicAsQueue;
	}

	public static Topic getNativeTopic(Destination d) throws JMSException {
		if (d instanceof Tibjms2TopicAsQueue) {
			Tibjms2TopicAsQueue tq = (Tibjms2TopicAsQueue) d;
			return tq.getNativeTopic();
		}
		if (d instanceof Topic)
			return (Topic)d;
		throw new JMSException("Destination is not topic: "+d+" of "+d.getClass().getName());
	}

	public static Queue getWrappedQueue(Destination d) {
		if (d instanceof Tibjms2TopicAsQueue)
			return (Queue) d;
		if (d instanceof Queue)
			return (Queue) d;
		return new Tibjms2TopicAsQueue(d.toString());
	}
}
