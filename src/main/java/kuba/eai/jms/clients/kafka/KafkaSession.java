package kuba.eai.jms.clients.kafka;

import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import kuba.eai.jms.clients.common.TheQueue;
import kuba.eai.jms.clients.common.TheTempQueue;
import kuba.eai.jms.clients.common.TheTempTopic;
import kuba.eai.jms.clients.common.TheTopic;

public class KafkaSession implements javax.jms.Session, QueueSession, TopicSession {
	
	private ConcurrentLinkedQueue<KafkaConsumer> consumers = new ConcurrentLinkedQueue<>();
	private ConcurrentHashMap<String,org.apache.kafka.clients.producer.KafkaProducer<String,String>> producersMap = new ConcurrentHashMap<>();
	private ConcurrentHashMap<KafkaProducer, AtomicReference<Boolean>> trMap = new ConcurrentHashMap<>();
	private Object producersMutex = new Object();
	private Properties p = new Properties();
	private int ackMode = Session.AUTO_ACKNOWLEDGE;
	private boolean transactional = false;
	private MessageListener ml = null;
    private boolean autoCommit = true;
    private final static AtomicLong producersCounter = new AtomicLong(0);
			
	public KafkaSession(int i, Properties props, boolean tr, int ackMode) {
		p.putAll(props);
		this.transactional = tr;
		this.ackMode = ackMode;	
		if (tr)
			p.setProperty("transactional.id", UUID.randomUUID().toString());
		if (ackMode == Session.CLIENT_ACKNOWLEDGE || ackMode == Session.SESSION_TRANSACTED) {
			p.setProperty("enable.auto.commit", "false");
			autoCommit = false;
		}
	}

	@Override
	public void close() throws JMSException {
		for (org.apache.kafka.clients.producer.KafkaProducer<String, String> prod : producersMap.values()) {
			try {
				prod.close();
			}
			catch (Exception e) {}
		}
		for (KafkaConsumer con : consumers) {
			try {
				con.close();
			}
			catch (Exception e) {}
		}		
	}	
	
	@Override
	public void commit() throws JMSException {
		if (transactional) {
			for (org.apache.kafka.clients.producer.KafkaProducer<String, String> prod : producersMap.values()) {
				try {
					prod.commitTransaction();
				}
				catch (Exception e) {
					throw new JMSException("Commit error: "+e);
				}
			}
			for (AtomicReference<Boolean> ar : trMap.values()) {
				ar.set(Boolean.FALSE);
			}
			for (KafkaConsumer consumer : consumers) {
				consumer.confirmMessages();
			}
		}		
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0) throws JMSException {
		/**FIXME: todo */
		throw new JMSException("Not implemented");
	}

	@Override
	public QueueBrowser createBrowser(Queue arg0, String arg1) throws JMSException {
		/**FIXME: todo */
		throw new JMSException("Not implemented");
	}

	@Override
	public BytesMessage createBytesMessage() throws JMSException {
		throw new JMSException("Not supported");
	}

	@Override
	public MessageConsumer createConsumer(Destination dest) throws JMSException {
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, null);
		consumers.add(cons);
		return cons;
	}

	@Override
	public MessageConsumer createConsumer(Destination dest, String sel) throws JMSException {
		RecordMetadata offset = messageSelectorToMetadata(sel);
		if (sel!=null && sel.length() > 0 && offset==null)
			throw new JMSException("Message selector is not supported");
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, offset);
		consumers.add(cons);
		return cons;
	}

	@Override
	public MessageConsumer createConsumer(Destination dest, String sel, boolean noLocal) throws JMSException {
		RecordMetadata offset = messageSelectorToMetadata(sel);
		if (sel!=null && sel.length() > 0 && offset==null)
			throw new JMSException("Message selector is not supported");
		if (noLocal)
			throw new JMSException("NoLocal is not supported");
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, offset);
		consumers.add(cons);
		return cons;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic dest, String sel) throws JMSException {
		RecordMetadata offset = messageSelectorToMetadata(sel);
		if (sel!=null && sel.length() > 0 && offset==null)
			throw new JMSException("Message selector is not supported");
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, offset);
		consumers.add(cons);
		return cons;
	}

	@Override
	public TopicSubscriber createDurableSubscriber(Topic dest, String sel, String sub, boolean noLocal)
			throws JMSException {
		RecordMetadata offset = messageSelectorToMetadata(sel);
		if (sel!=null && sel.length() > 0 && offset==null)
			throw new JMSException("Message selector is not supported");
		Properties p = new Properties();
		p.putAll(this.p);
		if (sub!=null && sub.length() > 0)
			p.put("group.id", sub);
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, offset);
		consumers.add(cons);
		return cons;
	}

	@Override
	public MapMessage createMapMessage() throws JMSException {
		throw new JMSException("Not supported");
	}

	@Override
	public Message createMessage() throws JMSException {
		return new kuba.eai.jms.clients.common.Message();
	}

	@Override
	public ObjectMessage createObjectMessage() throws JMSException {
		throw new JMSException("Not supported");
	}

	@Override
	public ObjectMessage createObjectMessage(Serializable arg0) throws JMSException {
		throw new JMSException("Not supported");
	}

	@Override
	public MessageProducer createProducer(Destination dest) throws JMSException {
		return new KafkaProducer(this.p, dest, this);
	}

	@Override
	public Queue createQueue(String arg0) throws JMSException {
		return new TheQueue(arg0);
	}

	@Override
	public StreamMessage createStreamMessage() throws JMSException {
		throw new JMSException("Not supported");
	}

	@Override
	public TemporaryQueue createTemporaryQueue() throws JMSException {
		return new TheTempQueue();
	}

	@Override
	public TemporaryTopic createTemporaryTopic() throws JMSException {
		return new TheTempTopic();
	}

	@Override
	public TextMessage createTextMessage() throws JMSException {
		return new kuba.eai.jms.clients.common.Message();
	}

	@Override
	public TextMessage createTextMessage(String arg0) throws JMSException {
		return new kuba.eai.jms.clients.common.Message(null, arg0, null, null);
	}

	@Override
	public Topic createTopic(String arg0) throws JMSException {
		return new TheTopic(arg0);
	}

	@Override
	public int getAcknowledgeMode() throws JMSException {
		return ackMode;
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		return ml;
	}

	@Override
	public boolean getTransacted() throws JMSException {
		return transactional;
	}

	@Override
	public void recover() throws JMSException {
		/**FIXME: todo */		
	}

	@Override
	public void rollback() throws JMSException {
		if (transactional) {
			for (org.apache.kafka.clients.producer.KafkaProducer<String, String> prod : producersMap.values()) {
				try {
					prod.abortTransaction();
				}
				catch (Exception e) {
					throw new JMSException("Abort error: "+e);
				}
			}
			for (AtomicReference<Boolean> ar : trMap.values()) {
				ar.set(Boolean.FALSE);
			}
		}		
	}
	

	@Override
	public void run() {}

	@Override
	public void setMessageListener(MessageListener arg0) throws JMSException {
		this.ml = arg0;		
	}

	@Override
	public void unsubscribe(String arg0) throws JMSException {}

	@Override
	public TopicPublisher createPublisher(Topic dest) throws JMSException {
		return new KafkaProducer(p, dest, this);
	}

	@Override
	public TopicSubscriber createSubscriber(Topic dest) throws JMSException {
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, null);
		consumers.add(cons);
		return cons;
	}

	@Override
	public TopicSubscriber createSubscriber(Topic dest, String sel, boolean noLocal) throws JMSException {
		RecordMetadata offset = messageSelectorToMetadata(sel);
		if (sel!=null && sel.length() > 0 && offset==null)
			throw new JMSException("Message selector is not supported");
		if (noLocal)
			throw new JMSException("NoLocal is not supported");
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, offset);
		consumers.add(cons);
		return cons;
	}

	@Override
	public QueueReceiver createReceiver(Queue dest) throws JMSException {	
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, null);
		consumers.add(cons);
		return cons;
	}
	
	private static RecordMetadata messageSelectorToMetadata(String ms) {
		if (ms==null || ms.length()==0)
			return null;
		ms = ms.toLowerCase();
		if (ms.startsWith("offset=")) {
			String[] kv = ms.split("\\=");
			if (kv.length == 2) {
				String partOffsetStr = kv[1].replace("'", "").replace("\"", "");
				try {
					String[] po = partOffsetStr.split("\\:");
					if (po.length==2) {
						return new RecordMetadata(new TopicPartition("", Integer.valueOf(po[0])), 0, Long.valueOf(po[1]), 0, null, 0, 0);
					}
				}
				catch (Exception e) {};
			}
		}
		return null;
	}

	@Override
	public QueueReceiver createReceiver(Queue dest, String sel) throws JMSException {
		RecordMetadata offset = messageSelectorToMetadata(sel);
		if (sel!=null && sel.length() > 0 && offset==null)
			throw new JMSException("Message selector is not supported: "+sel);
		KafkaConsumer cons = new KafkaConsumer(this.p, dest, this, offset);
		consumers.add(cons);
		return cons;
	}

	@Override
	public QueueSender createSender(Queue dest) throws JMSException {
		return new KafkaProducer(p, dest, this);
	}

	public void startConsumers() {
		for (KafkaConsumer con : consumers) {
			try {
				con.start();
			}
			catch (Exception e) {}
		}		
	}

	public void stopConsumers() {
		for (KafkaConsumer con : consumers) {
			try {
				con.stop();
			}
			catch (Exception e) {}
		}		
	}
	
	private String getProducerKey(int deliveryMode) {
		return "tr="+transactional+"&dm="+deliveryMode;
	}
	
	private void adjustClientIdForProducer(Properties p) {
		String cid = p.getProperty("client.id");
		if (cid!=null)
			p.setProperty("client.id", cid+"."+producersCounter.incrementAndGet());
	}
	
	public org.apache.kafka.clients.producer.KafkaProducer<String,String> getProducerForDeliveryMode(int deliveryMode) {
		String key = getProducerKey(deliveryMode);
		org.apache.kafka.clients.producer.KafkaProducer<String,String> prod = producersMap.get(key);
		if (prod!=null)
			return prod;
		synchronized(producersMutex) {
			prod = producersMap.get(key);
			if (prod!=null)
				return prod;
			Properties props = new Properties();
			props.putAll(p);
			adjustClientIdForProducer(props);
			Object acks = props.get("acks");
			if (acks!=null && acks.toString().length()>0 && !acks.equals("all")) {
				int ackCount = -1;
				try {
					ackCount = Integer.valueOf((String)acks);
				}
				catch (Exception numExc) {}
				if (ackCount > 1) {					
					props.put("min.insync.replicas", ackCount);
					props.put("acks", "all");
				}
			}
			if (props.get("acks")==null)
				props.put("acks", deliveryMode == kuba.eai.jms.clients.common.Message.DELIVERY_MODE_PERSISTENT ? "all" : "1");
			KafkaConnection.sanitizePropertiesForProducer(props);
			producersMap.put(key, prod = new org.apache.kafka.clients.producer.KafkaProducer<>(props));
			return prod;
		}
	}

	public void onConsumerClosed(KafkaConsumer kafkaConsumer) {
		if (ackMode == Session.AUTO_ACKNOWLEDGE)
			kafkaConsumer.confirmMessages();
		consumers.remove(kafkaConsumer);		
	}

	public void registerTransactionalProducer(KafkaProducer kafkaProducer, AtomicReference<Boolean> trInited) {
		trMap.put(kafkaProducer, trInited);		
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public MessageConsumer createDurableConsumer(Topic t, String s) throws JMSException {
		return createDurableSubscriber(t, s);
	}

	public MessageConsumer createDurableConsumer(Topic dest, String sub, String sel, boolean noLocal)
			throws JMSException {
		return createDurableSubscriber(dest, sub, sel, noLocal);
	}

	public MessageConsumer createSharedConsumer(Topic t, String s) throws JMSException {
		return createConsumer(t, s);
	}

	public MessageConsumer createSharedConsumer(Topic dest, String sub, String sel) throws JMSException {
		return createDurableConsumer(dest, sub, sel, false);
	}

	public MessageConsumer createSharedDurableConsumer(Topic t, String s) throws JMSException {
		return createConsumer(t, s);
	}

	public MessageConsumer createSharedDurableConsumer(Topic t, String sub, String sel) throws JMSException {
		return createDurableConsumer(t, sub, sel, false);
	}	
}
