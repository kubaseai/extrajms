package kuba.eai.jms.clients.ems;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

public class Tibjms2QueueReceiver implements QueueReceiver {
	
	private MessageConsumer mc = null;
	/** Queue or Tibjms2TopicAsQueue **/
	private Queue q = null;

	public Tibjms2QueueReceiver(MessageConsumer mc, Queue q) {
		this.mc = mc;
		this.q = q;
	}

	@Override
	public void close() throws JMSException {
		mc.close();
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		return mc.getMessageListener();
	}

	@Override
	public String getMessageSelector() throws JMSException {
		return mc.getMessageSelector();
	}

	@Override
	public Message receive() throws JMSException {
		return mc.receive();
	}

	@Override
	public Message receive(long tm) throws JMSException {
		return mc.receive(tm);
	}

	@Override
	public Message receiveNoWait() throws JMSException {
		return mc.receiveNoWait();
	}

	@Override
	public void setMessageListener(MessageListener ml) throws JMSException {
		mc.setMessageListener(ml);
	}

	@Override
	public Queue getQueue() throws JMSException {
		return q;
	}
}
