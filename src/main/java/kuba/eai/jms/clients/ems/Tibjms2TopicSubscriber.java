package kuba.eai.jms.clients.ems;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

public class Tibjms2TopicSubscriber implements TopicSubscriber {
	
	private MessageConsumer mc = null;
	private Topic t = null;

	public Tibjms2TopicSubscriber(MessageConsumer mc, Topic t) {
		this.mc = mc;
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
	public boolean getNoLocal() throws JMSException {
		return false;
	}

	@Override
	public Topic getTopic() throws JMSException {
		return t;
	}
}
