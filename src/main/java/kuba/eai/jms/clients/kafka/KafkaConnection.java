package kuba.eai.jms.clients.kafka;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;

import kuba.eai.jms.clients.common.ConnectionImpl;
import kuba.eai.jms.clients.common.TheConnectionMetaData;

public class KafkaConnection extends ConnectionImpl {
	
	private final static String PROD_PROPS = (" acks batch.size bootstrap.servers buffer.memory client.id compression.type "+
	"connections.max.idle.ms enable.idempotence interceptor.classes key.serializer linger.ms max.block.ms "+
	"max.in.flight.requests.per.connection max.request.size	metadata.max.age.ms	metric.reporters metrics.num.samples "+
	"metrics.recording.level metrics.sample.window.ms partitioner.class	receive.buffer.bytes reconnect.backoff.max.ms "+
	"reconnect.backoff.ms request.timeout.ms retries retry.backoff.ms sasl.jaas.config sasl.kerberos.kinit.cmd "+
	"sasl.kerberos.min.time.before.relogin sasl.kerberos.service.name sasl.kerberos.ticket.renew.jitter "+
	"sasl.kerberos.ticket.renew.window.factor sasl.mechanism send.buffer.bytes ssl.cipher.suites ssl.enabled.protocols "+
	"ssl.endpoint.identification.algorithm ssl.key.password ssl.keymanager.algorithm ssl.keystore.location "+
	"ssl.keystore.password ssl.keystore.type ssl.protocol ssl.provider ssl.secure.random.implementation "+
	"ssl.trustmanager.algorithm ssl.truststore.location ssl.truststore.password ssl.truststore.type "+
	"transaction.timeout.ms transactional.id value.serializer ");
	private final static String CONS_PROPS = (" auto.commit.interval.ms auto.offset.reset bootstrap.servers "+
	"check.crcs client.id connections.max.idle.ms enable.auto.commit exclude.internal.topics fetch.max.bytes "+
	"fetch.max.wait.ms fetch.min.bytes group.id heartbeat.interval.ms interceptor.classes internal.leave.group.on.close "+
	"isolation.level key.deserializer max.partition.fetch.bytes max.poll.interval.ms max.poll.records metadata.max.age.ms "+
	"metric.reporters metrics.num.samples metrics.recording.level metrics.sample.window.ms partition.assignment.strategy "+
	"receive.buffer.bytes reconnect.backoff.max.ms reconnect.backoff.ms request.timeout.ms retry.backoff.ms "+
	"sasl.jaas.config sasl.kerberos.kinit.cmd sasl.kerberos.min.time.before.relogin sasl.kerberos.service.name "+
	"sasl.kerberos.ticket.renew.jitter sasl.kerberos.ticket.renew.window.factor sasl.mechanism security.protocol "+
	"send.buffer.bytes session.timeout.ms ssl.cipher.suites ssl.enabled.protocols ssl.endpoint.identification.algorithm "+
	"ssl.key.password ssl.keymanager.algorithm ssl.keystore.location ssl.keystore.password ssl.keystore.type "+
	"ssl.protocol ssl.provider ssl.secure.random.implementation ssl.trustmanager.algorithm ssl.truststore.location "+
	"ssl.truststore.password ssl.truststore.type value.deserializer ");
	
	public static final ConnectionImpl FACTORY = new KafkaConnection();
	private String clientId = null;
	private Properties props = new Properties();
	private ExceptionListener exListener = null;
	
	private ConcurrentLinkedQueue<KafkaSession> sessions = new ConcurrentLinkedQueue<>();
	
	private void addProp(String key, String value, boolean override) {
		if ((props.get(key)==null || override) && value!=null)
			props.put(key, value);
	}
	

	public KafkaConnection(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		props.putAll(properties);
		setClientID(clientId);		
		addProp("user", user, false);
		addProp("pass", pass, false);
		addProp("bootstrap.servers", url, false);
		addProp("acks", "all", false);
		addProp("retries", "0", false);
		addProp("batch.size", "16384", false);
		addProp("linger.ms", "15", false);
		addProp("buffer.memory", "33554432", false);
		addProp("auto.offset.reset", "earliest", false);
		addProp("enable.auto.commit", "true", false);
		addProp("isolation.level", "read_committed", false);
		addProp("max.poll.records", "64", false);
		addProp("request.timeout.ms", "30000", false);
		addProp("max.block.ms", "30000", false);
		addProp("transaction.timeout.ms", "45000", false);
		addProp("client.id", clientId, false);
		addProp("group.id", clientId, false);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kuba.eai.jms.clients.common.Logger.debug("Kafka connection properties :" + props);
	}

	protected KafkaConnection() {}
	
	private final static void sanitizeProperties(Properties p, String def) {
		var toBeRemoved = new LinkedList<String>();
		for (Object key : p.keySet()) {
			if (def.indexOf(" "+key+" ")==-1)
				toBeRemoved.add(key.toString());
		}
		for (String key : toBeRemoved)
			p.remove(key);
	}

	public final static void sanitizePropertiesForProducer(Properties p) {
		sanitizeProperties(p, PROD_PROPS);
	}
	
	public final static void sanitizePropertiesForConsumer(Properties p) {
		sanitizeProperties(p, CONS_PROPS);
	}
	
	@Override
	public void close() throws JMSException {
		for (KafkaSession sess : sessions) {
			try {
				sess.close();
			}
			catch (Exception e) {}
		}			
	}

	@Override
	public ConnectionConsumer createConnectionConsumer(Destination arg0, String arg1, ServerSessionPool arg2, int arg3)
			throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	@Override
	public ConnectionConsumer createDurableConnectionConsumer(Topic arg0, String arg1, String arg2,
			ServerSessionPool arg3, int arg4) throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	@Override
	public Session createSession(boolean tr, int ackMode) throws JMSException {
		return new KafkaSession(0, props, tr, ackMode);
	}

	@Override
	public String getClientID() throws JMSException {
		return clientId;
	}

	@Override
	public ExceptionListener getExceptionListener() throws JMSException {
		return exListener;
	}

	@Override
	public ConnectionMetaData getMetaData() throws JMSException {
		return TheConnectionMetaData.METADATA;
	}

	@Override
	public void setClientID(String cid) throws JMSException {
		this.clientId = cid!=null && cid.length() > 0 ? cid : UUID.randomUUID().toString();
		addProp("client.id", clientId, true);
		addProp("group.id", clientId, true);		
	}

	@Override
	public void setExceptionListener(ExceptionListener el) throws JMSException {
		this.exListener = el;
		
	}

	@Override
	public void start() throws JMSException {
		for (KafkaSession sess : sessions) {
			try {
				sess.startConsumers();
			}
			catch (Exception e) {}
		}
	}

	@Override
	public void stop() throws JMSException {
		for (KafkaSession sess : sessions) {
			try {
				sess.stopConsumers();
			}
			catch (Exception e) {}
		}		
	}

	@Override
	public ConnectionConsumer createConnectionConsumer(Queue arg0, String arg1, ServerSessionPool arg2, int arg3)
			throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	@Override
	public QueueSession createQueueSession(boolean tr, int ackMode) throws JMSException {
		return new KafkaSession(1, props, tr, ackMode);
	}

	@Override
	public ConnectionConsumer createConnectionConsumer(Topic arg0, String arg1, ServerSessionPool arg2, int arg3)
			throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	@Override
	public TopicSession createTopicSession(boolean tr, int ackMode) throws JMSException {
		return new KafkaSession(2, props, tr, ackMode);
	}

	@Override
	public ConnectionImpl create(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		return new KafkaConnection(i, url, user, pass, clientId, properties);
	}
	
}
