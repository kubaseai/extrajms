package kuba.eai.jms.clients.ems;

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

public class Tibjms2Session implements QueueSession {
	
	private Session sess = null;
	private String subName2 = null;
	
	public Tibjms2Session(Session session, int factoryType, String subName) {
		this.sess = session;		
		this.subName2 = subName;
	}

	@Override
	public void close() throws JMSException {
		sess.close();		
	}

	@Override
	public void commit() throws JMSException {
		sess.commit();		
	}

	@Override
	public BytesMessage createBytesMessage() throws JMSException {
		return sess.createBytesMessage();
	}

	@Override
	public MessageConsumer createConsumer(Destination d) throws JMSException {
		if (Tibjms2.isTopicAsQueue(d))
			return sess.createSharedConsumer(Tibjms2.getNativeTopic(d), subName2);
		return sess.createConsumer(d);
	}

	@Override
	public MessageConsumer createConsumer(Destination d, String subName) throws JMSException {
		if (Tibjms2.isTopicAsQueue(d))
			return sess.createSharedConsumer(Tibjms2.getNativeTopic(d), subName);
		return sess.createConsumer(d, subName);
	}

	@Override
	public MessageConsumer createConsumer(Destination d, String ms, boolean noLocal) throws JMSException {
		if (Tibjms2.isTopicAsQueue(d))
			return sess.createSharedConsumer(Tibjms2.getNativeTopic(d), subName2, ms);
		return sess.createConsumer(d, ms, noLocal);
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic t, String sub) throws JMSException {
		return new Tibjms2TopicSubscriber(sess.createSharedDurableConsumer(t, sub), t);
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic t, String sub, String ms, boolean noLocal)
			throws JMSException {
		return new Tibjms2TopicSubscriber(sess.createSharedDurableConsumer(t, sub, ms), t);
	}

	@Override
	public MapMessage createMapMessage() throws JMSException {
		return sess.createMapMessage();
	}

	@Override
	public Message createMessage() throws JMSException {
		return sess.createMessage();
	}

	@Override
	public ObjectMessage createObjectMessage() throws JMSException {
		return sess.createObjectMessage();
	}

	@Override
	public ObjectMessage createObjectMessage(Serializable s) throws JMSException {
		return sess.createObjectMessage(s);
	}

	@Override
	public MessageProducer createProducer(Destination d) throws JMSException {
		return sess.createProducer(d);
	}

	@Override
	public StreamMessage createStreamMessage() throws JMSException {
		return sess.createStreamMessage();
	}

	@Override
	public TemporaryTopic createTemporaryTopic() throws JMSException {
		return sess.createTemporaryTopic();
	}

	@Override
	public TextMessage createTextMessage() throws JMSException {
		return sess.createTextMessage();
	}

	@Override
	public TextMessage createTextMessage(String s) throws JMSException {
		return sess.createTextMessage(s);
	}

	@Override
	public Topic createTopic(String t) throws JMSException {
		return sess.createTopic(t);
	}

	@Override
	public int getAcknowledgeMode() throws JMSException {
		return sess.getAcknowledgeMode();
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		return sess.getMessageListener();
	}

	@Override
	public boolean getTransacted() throws JMSException {
		return sess.getTransacted();
	}

	@Override
	public void recover() throws JMSException {
		sess.recover();		
	}

	@Override
	public void rollback() throws JMSException {
		sess.rollback();		
	}

	@Override
	public void run() {
		sess.run();		
	}

	@Override
	public void setMessageListener(MessageListener ml) throws JMSException {
		sess.setMessageListener(ml);		
	}

	@Override
	public void unsubscribe(String u) throws JMSException {
		sess.unsubscribe(u);		
	}

	@Override
	public QueueBrowser createBrowser(Queue q) throws JMSException {
		return sess.createBrowser(q);
	}

	@Override
	public QueueBrowser createBrowser(Queue q, String s) throws JMSException {
		return sess.createBrowser(q, s);
	}

	@Override
	public Queue createQueue(String q) throws JMSException {
		return new Tibjms2TopicAsQueue(q);
	}

	@Override
	public QueueReceiver createReceiver(Queue q) throws JMSException {
		return createReceiver(q, null);
	}

	@Override
	public QueueReceiver createReceiver(Queue q, String s) throws JMSException {
		if (Tibjms2.isTopicAsQueue(q)) {
			return new Tibjms2QueueReceiver(sess.createSharedConsumer(Tibjms2.getNativeTopic(q), subName2, s), q);
		}
		else
			return new Tibjms2QueueReceiver(sess.createConsumer(q, s), q);
	}

	@Override
	public QueueSender createSender(Queue q) throws JMSException {
		Destination nativeDst = Tibjms2.isTopicAsQueue(q) ? Tibjms2.getNativeTopic(q) : q;
		return new Tibjms2QueueSender(sess.createProducer(nativeDst), nativeDst);
	}

	@Override
	public TemporaryQueue createTemporaryQueue() throws JMSException {
		return sess.createTemporaryQueue();
	}

	@Override
	public MessageConsumer createDurableConsumer(Topic t, String sub) throws JMSException {
		return sess.createSharedDurableConsumer(t, sub);
	}

	@Override
	public MessageConsumer createDurableConsumer(Topic t, String sub, String ms, boolean noLocal)
			throws JMSException {
		return sess.createSharedDurableConsumer(t, sub, ms);
	}

	@Override
	public MessageConsumer createSharedConsumer(Topic t, String sub) throws JMSException {
		return sess.createSharedConsumer(t, sub);
	}

	@Override
	public MessageConsumer createSharedConsumer(Topic t, String sub, String ms) throws JMSException {
		return sess.createSharedConsumer(t, sub, ms);
	}

	@Override
	public MessageConsumer createSharedDurableConsumer(Topic t, String sub) throws JMSException {
		return sess.createSharedDurableConsumer(t, sub);
	}

	@Override
	public MessageConsumer createSharedDurableConsumer(Topic t, String sub, String ms) throws JMSException {
		return sess.createSharedDurableConsumer(t, sub, ms);
	}
}
