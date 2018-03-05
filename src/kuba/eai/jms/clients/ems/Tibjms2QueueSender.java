package kuba.eai.jms.clients.ems;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;

public class Tibjms2QueueSender implements QueueSender {
	
	private MessageProducer mp = null;
	private Destination q = null;

	public Tibjms2QueueSender(MessageProducer mp, Destination nativeDst) {
		this.mp = mp;
		this.q = nativeDst;
	}

	@Override
	public void close() throws JMSException {
		mp.close();
	}

	@Override
	public long getDeliveryDelay() throws JMSException {
		return mp.getDeliveryDelay();
	}

	@Override
	public int getDeliveryMode() throws JMSException {
		return mp.getDeliveryMode();
	}

	@Override
	public Destination getDestination() throws JMSException {
		return mp.getDestination();
	}

	@Override
	public boolean getDisableMessageID() throws JMSException {
		return mp.getDisableMessageID();
	}

	@Override
	public boolean getDisableMessageTimestamp() throws JMSException {
		return mp.getDisableMessageTimestamp();
	}

	@Override
	public int getPriority() throws JMSException {
		return mp.getPriority();
	}

	@Override
	public long getTimeToLive() throws JMSException {
		return mp.getTimeToLive();
	}

	@Override
	public void send(Destination d, Message m) throws JMSException {
		mp.send(d, m);
	}

	@Override
	public void send(Message m, CompletionListener cl) throws JMSException {
		mp.send(m, cl);
	}

	@Override
	public void send(Destination d, Message m, CompletionListener cl) throws JMSException {
		mp.send(d, m, cl);
	}

	@Override
	public void send(Destination d, Message m, int dm, int prio, long ttl) throws JMSException {
		mp.send(d, m, dm, prio, ttl);
	}

	@Override
	public void send(Message m, int dm, int prio, long ttl, CompletionListener cl) throws JMSException {
		mp.send(m, dm, prio, ttl, cl);
	}

	@Override
	public void send(Destination d, Message m, int dm, int prio, long ttl, CompletionListener cl)
			throws JMSException {
		mp.send(d, m, dm, prio, ttl, cl);
	}

	@Override
	public void setDeliveryDelay(long d) throws JMSException {
		mp.setDeliveryDelay(d);
	}

	@Override
	public void setDeliveryMode(int dm) throws JMSException {
		mp.setDeliveryMode(dm);
	}

	@Override
	public void setDisableMessageID(boolean b) throws JMSException {
		mp.setDisableMessageID(b);
	}

	@Override
	public void setDisableMessageTimestamp(boolean b) throws JMSException {
		mp.setDisableMessageID(b);
	}

	@Override
	public void setPriority(int p) throws JMSException {
		mp.setPriority(p);
	}

	@Override
	public void setTimeToLive(long ttl) throws JMSException {
		mp.setTimeToLive(ttl);
	}

	@Override
	public Queue getQueue() throws JMSException {
		return Tibjms2.getWrappedQueue(q);
	}

	@Override
	public void send(Message m) throws JMSException {
		mp.send(m);
	}

	@Override
	public void send(Queue q, Message m) throws JMSException {
		if (Tibjms2.isTopicAsQueue(q))
			mp.send(Tibjms2.getNativeTopic(q), m);
		else
			mp.send(q, m);
	}

	@Override
	public void send(Message m, int dm, int prio, long ttl) throws JMSException {
		mp.send(m, dm, prio, ttl);
	}

	@Override
	public void send(Queue q, Message m, int dm, int prio, long ttl) throws JMSException {
		Destination d = Tibjms2.isTopicAsQueue(q) ? Tibjms2.getNativeTopic(q) : q;
		mp.send(d, m, dm, prio, ttl);
	}
}
