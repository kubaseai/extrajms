package kuba.eai.jms.clients.kafka;

import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducer implements javax.jms.MessageProducer, TopicPublisher, QueueSender {

	protected Properties props = null;
	private KafkaSession sess = null;
	private Destination dest = null;	
	private int deliveryMode = kuba.eai.jms.clients.common.Message.DELIVERY_MODE_DEFAULT;	
	private boolean disableMessageID = false;
	private boolean disableMessageTimestamp = false;
	private int priority = 4;
	private long ttl = 0;
	private long dd = 0;
	private AtomicReference<Boolean> trInited = new AtomicReference<>();
	private boolean useHeaders = false; // headers are not available in Kafka replication
		
	public KafkaProducer(Properties props, Destination dest, KafkaSession kafkaSession) throws JMSException {
		this.props = props;
		this.dest = dest;
		this.sess = kafkaSession;
		if (sess.getTransacted()) {
			sess.registerTransactionalProducer(this, trInited);
			trInited.set(Boolean.FALSE);
		}
		String useJMSHeaders = this.props.get("useJMSHeaders") + "";
		this.useHeaders = "1".equals(useJMSHeaders) || "true".equalsIgnoreCase(useJMSHeaders) || "yes".equalsIgnoreCase(useJMSHeaders);
	}
	
	@Override
	public void close() throws JMSException {
		/* nothing to be done here */
	}

	@Override
	public int getDeliveryMode() throws JMSException {
		return deliveryMode;
	}

	@Override
	public Destination getDestination() throws JMSException {
		return dest;
	}

	@Override
	public boolean getDisableMessageID() throws JMSException {
		return disableMessageID;
	}

	@Override
	public boolean getDisableMessageTimestamp() throws JMSException {
		return disableMessageTimestamp;
	}

	@Override
	public int getPriority() throws JMSException {
		return priority;
	}

	@Override
	public long getTimeToLive() throws JMSException {
		return ttl;
	}

	@Override
	public void send(Message msg) throws JMSException {
		sendWithParams(dest, msg, deliveryMode, priority, ttl, null);		
	}

	@Override
	public void send(Destination d, Message msg) throws JMSException {
		sendWithParams(d, msg, deliveryMode, priority, ttl, null);
	}

	@Override
	public void send(Message msg, int deliveryMode, int priority, long ttl) throws JMSException {
		sendWithParams(dest, msg, deliveryMode, priority, ttl, null);		
	}

	private ProducerRecord<String,String> enrichMessage(kuba.eai.jms.clients.common.Message msg, Destination d) throws JMSException {
		String msgId = msg.getJMSMessageID();
		if (msgId==null)
			msgId = UUID.randomUUID().toString();
		msg.setJMSMessageID(msgId);
		msg.setJMSDeliveryMode(deliveryMode);
		msg.setJMSDestination(d);
		msg.setJMSExpiration(ttl==0 ? 0 : System.currentTimeMillis() + ttl);
		msg.setJMSPriority(priority);
		msg.setJMSTimestamp(System.currentTimeMillis());		
		if (!useHeaders)
			msgId = msg.getSerializedProperties();
		ProducerRecord<String,String> record = new ProducerRecord<String,String>(d.toString(), msgId, msg.getStringProperty("Body"));
		if (useHeaders) {
			Enumeration<?> en = msg.getPropertyNames(true);
			while (en.hasMoreElements()) {
				String propName = en.nextElement().toString();
				String prop = msg.getStringProperty(propName);
				if (prop!=null && prop.length()>0)
					try {
						record.headers().add(propName, prop.getBytes("utf-8"));
					}
					catch (Exception e) {
						throw new JMSException("Cannot add header: "+en.toString()+"="+prop);
					}
			}
		}
		return record;
	}

	public void sendWithParams(Destination d, Message m, int deliveryMode, int priority, long ttl, Callback cb) throws JMSException {
		if (d==null)
			throw new JMSException("Destination cannot be null");
		kuba.eai.jms.clients.common.Message msg = (kuba.eai.jms.clients.common.Message) m;
		if (msg==null)
			throw new JMSException("Message cannot be null");
		ProducerRecord<String,String> record = enrichMessage(msg, d);
		org.apache.kafka.clients.producer.KafkaProducer<String,String> producer = sess.getProducerForDeliveryMode(deliveryMode);
		if (Boolean.FALSE.equals(trInited.get())) {
			producer.initTransactions();
			producer.beginTransaction();
		}
		try {
			Future<RecordMetadata> futureMd = producer.send(record, cb);
			if (deliveryMode==kuba.eai.jms.clients.common.Message.DELIVERY_MODE_PERSISTENT ||
					deliveryMode==kuba.eai.jms.clients.common.Message.DELIVERY_MODE_NON_PERSISTENT) {
				RecordMetadata md = futureMd.get();
				msg.setJMSMessageID(msg.getJMSMessageID()+":"+md.partition()+":"+md.offset());
			}			
		}
		catch (Exception e) {
			throw new JMSException("Cannot send: "+e);
		}
	}	

	@Override
	public void setDeliveryMode(int dm) throws JMSException {
		this.deliveryMode = dm;		
	}

	@Override
	public void setDisableMessageID(boolean b) throws JMSException {
		this.disableMessageID = b;		
	}

	@Override
	public void setDisableMessageTimestamp(boolean b) throws JMSException {
		this.disableMessageTimestamp = b;		
	}

	@Override
	public void setPriority(int p) throws JMSException {
		this.priority = p;		
	}

	@Override
	public void setTimeToLive(long ttl) throws JMSException {
		this.ttl = ttl;		
	}

	@Override
	public Topic getTopic() throws JMSException {
		if (dest instanceof Topic)
			return (Topic)dest;
		return null;
	}

	@Override
	public void publish(Message msg) throws JMSException {
		sendWithParams(dest, msg, deliveryMode, priority, ttl, null);		
	}

	@Override
	public void publish(Topic dest, Message msg) throws JMSException {
		sendWithParams(dest, msg, deliveryMode, priority, ttl, null);		
	}

	@Override
	public void publish(Message msg, int dm, int priority, long ttl) throws JMSException {
		sendWithParams(dest, msg, dm, priority, ttl, null);
	}

	@Override
	public void publish(Topic topic, Message msg, int dm, int prio, long ttl)
			throws JMSException {
		sendWithParams(topic, msg, dm, prio, ttl, null);		
	}

	@Override
	public Queue getQueue() throws JMSException {
		if (dest instanceof Queue)
			return (Queue)dest;
		return null;
	}

	@Override
	public void send(Queue dest, Message msg) throws JMSException {
		sendWithParams(dest, msg, deliveryMode, priority, ttl, null);		
	}

	@Override
	public void send(Queue dest, Message msg, int dm, int prio, long ttl)
			throws JMSException {
		sendWithParams(dest, msg, dm, prio, ttl, null);			
	}

	@Override
	public void send(Destination d, Message msg, int dm, int prio, long ttl)
			throws JMSException {
		sendWithParams(d, msg, dm, prio, ttl, null);		
	}

	/* JMS 2.0 */
	public long getDeliveryDelay() throws JMSException {
		return dd;
	}
	
	public void setDeliveryDelay(long l) throws JMSException {
		this.dd = l;		
	}

	public void send(Message m, CompletionListener comp) throws JMSException {
		send(dest, m, deliveryMode, priority, ttl, comp);		
	}

	public void send(Destination d, Message m, CompletionListener comp) throws JMSException {
		send(d, m, deliveryMode, priority, ttl, comp);			
	}

	public void send(Message m, int dm, int prio, long ttl, CompletionListener comp) throws JMSException {
		send(dest, m, dm, prio, ttl, comp);	
	}

	public void send(Destination dest, final Message m, int deliveryMode, int priority, long ttl, final CompletionListener comp)
			throws JMSException
	{
		Callback cb = new Callback() {			
			@Override
			public void onCompletion(RecordMetadata md, Exception e) {
				if (e!=null)
					comp.onException(m, e);
				else
					comp.onCompletion(m);				
			}
		};
		sendWithParams(dest, m, deliveryMode, priority, ttl, cb);		
	}
}
