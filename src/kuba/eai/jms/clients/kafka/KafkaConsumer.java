package kuba.eai.jms.clients.kafka;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import kuba.eai.jms.clients.common.MessageAck;

public class KafkaConsumer implements javax.jms.MessageConsumer, TopicSubscriber, QueueReceiver {

	private volatile org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumer = null;
	private MessageListener ml = null;
	private String messageSelector = null;
	private Destination dest = null;
	private Iterator<ConsumerRecord<String, String>> cachedRecords = null;
	private KafkaSession sess = null;
	private MessageAck ack = null;
	private ConcurrentHashMap<TopicPartition,OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
	private final static AtomicLong counter = new AtomicLong(0);
	private RecordMetadata pOffset = null;
	private LinkedBlockingQueue<Long> receiveTrigger = new LinkedBlockingQueue<>();
	private LinkedBlockingQueue<Object> receivedResponse = new LinkedBlockingQueue<>();
	private Thread receiver = null;
	private Object receiveLock = new Object();
	
	private void adjustClientId(Properties p) {
		String cid = p.getProperty("client.id");
		if (cid!=null)
			p.setProperty("client.id", cid+"."+counter.incrementAndGet());
	}
	
	public KafkaConsumer(Properties props, Destination dest, KafkaSession sess, RecordMetadata md) throws JMSException {
		if (dest==null)
			throw new JMSException("Cannot consume null destination");
		this.dest = dest;		
		this.sess = sess;
		this.pOffset = md;
		adjustClientId(props);
		KafkaConnection.sanitizePropertiesForConsumer(props);
		consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
		if (md!=null) {
			LinkedList<TopicPartition> tps = new LinkedList<>();
			//for (PartitionInfo md : consumer.partitionsFor(dest.toString())) {
				tps.add(new TopicPartition(dest.toString(), md.partition()));
			//}
			consumer.assign(tps);
		}
		else {
			consumer.subscribe(Arrays.asList(dest.toString()));
		}
		if (!sess.isAutoCommit()) {
			final org.apache.kafka.clients.consumer.KafkaConsumer<String,String> cons = consumer;
			this.ack = new MessageAck() {
				public void ack() {
					cons.commitSync();
				}
			};
		}
		receiver = new Thread() {
			public void run() {
				org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumerReference = consumer;
				while (consumer!=null) {
					try {
						Long timeout = receiveTrigger.poll(1000, TimeUnit.MILLISECONDS);
						if (timeout!=null) {
							Object response = null;
							try {								
								response = _receive(timeout);
							}
							catch (Exception e) {
								response = e;
							}
							if (response!=null)
								receivedResponse.add(response);							
						}
					}
					catch (InterruptedException e) {
						continue;
					}
				}
				if (consumerReference!=null) {
					try {
						Thread.sleep(1000);
					}
					catch (InterruptedException e1) {}
					for (int i=0; i < 3; i++) {
						try {
							consumerReference.close();
							break;
						}
						catch (Throwable e) {}
					}					
				}
			}
		};
		receiver.start();
	}
	
	@Override
	public void close() throws JMSException {		
		sess.onConsumerClosed(this);
		if (consumer!=null) {			
			counter.decrementAndGet();
			receiver.interrupt();
			/* consumer.close(); should be done by receiver thread */
			consumer = null;
		}
	}

	@Override
	public MessageListener getMessageListener() throws JMSException {
		return ml;
	}

	@Override
	public String getMessageSelector() throws JMSException {
		return messageSelector;
	}

	@Override
	public Message receive() throws JMSException {
		return receive(0);
	}
	
	private Message receiveInternally(Long timeout) throws JMSException {
		if (receiveTrigger.size() > 0) {			
			throw new JMSException("Kafka consumer is stuck");
		}
		receiveTrigger.add(timeout);		
		Object obj = null;
		try {
			obj = receivedResponse.poll(timeout, TimeUnit.MILLISECONDS);
			if (obj!=null && obj instanceof Message) {
				return (Message)obj;
			}
			if (obj!=null && obj instanceof JMSException) {
				throw (JMSException)obj;					
			}
			if (obj!=null && obj instanceof Throwable) {
				throw new JMSException(obj.toString());					
			}
		}
		catch (InterruptedException e) {}		
		return null;
	}
	
	@Override
	public Message receive(long timeout) throws JMSException {
		if (timeout!=0) {
			return receiveInternally(timeout);			
		}
		else while (true) {
			Message m = receiveInternally(10000L);
			if (m!=null)
				return m;
		}		
	}

	private Message _receive(long timeout) throws JMSException {
		if (timeout==-1)
			timeout = 1;
		Message msg = null;
		synchronized(receiveLock) {
			while (true) {	
				if (cachedRecords==null || !cachedRecords.hasNext()) {
					if (pOffset!=null) {
						consumer.seek(new TopicPartition(dest.toString(), pOffset.partition()), pOffset.offset());							
						pOffset = null;
					}					
					ConsumerRecords<String, String> records = consumer.poll(timeout==0 ? 1000 : timeout);
					cachedRecords = records!=null ? records.iterator() : null;					
				}
				
				ConsumerRecord<String, String> r = cachedRecords!=null && cachedRecords.hasNext() ? cachedRecords.next() : null;
				if (r!=null) {
					try {
						TopicPartition key = new TopicPartition(dest.toString(), r.partition());
						OffsetAndMetadata val = new OffsetAndMetadata(r.offset());
						offsets.put(key, val);
						msg = new kuba.eai.jms.clients.common.Message(r.key(), r.value(), r.headers(), ack);
						msg.setStringProperty("JMSXKafkaOffset", r.offset()+"");
						msg.setStringProperty("JMSXKafkaPartition", r.partition()+"");
						break;
					}
					catch (Exception e) {
						throw new JMSException("Cannot receive message: "+e);
					}
				}
				if (timeout!=0)
					break;	
			}
		}
		return msg;
	}

	@Override
	public Message receiveNoWait() throws JMSException {
		return receive(-1);
	}

	@Override
	public void setMessageListener(MessageListener ml) throws JMSException {
		this.ml = ml;		
	}

	@Override
	public Queue getQueue() throws JMSException {
		if (dest instanceof Queue)
			return (Queue) dest;
		return null;
	}

	@Override
	public boolean getNoLocal() throws JMSException {
		return false;
	}

	@Override
	public Topic getTopic() throws JMSException {
		if (dest instanceof Topic)
			return (Topic)dest;
		return null;
	}

	public void start() {
		consumer.pause(consumer.assignment());		
	}

	public void stop() {
		consumer.resume(consumer.assignment());		
	}

	public void confirmMessages() {
		consumer.commitSync(offsets);		
	}
}
