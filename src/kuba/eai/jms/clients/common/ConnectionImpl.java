package kuba.eai.jms.clients.common;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;

public abstract class ConnectionImpl implements Connection,QueueConnection,TopicConnection {

	private final static HashMap<String, ConnectionImpl> implementations = new HashMap<>();
	
	private static void registerImpl(String proto, String className) {
		try {
			Constructor<?> ctor = Class.forName(className).getDeclaredConstructor();
			ctor.setAccessible(true);
			implementations.put(proto, (ConnectionImpl) ctor.newInstance()); 
		}
		catch (Throwable t) {
			System.out.println("Protocol "+proto+" is not enabled, problem with class "+className);
			t.printStackTrace();
		}
	}
	
	static {
		registerImpl("kafka", "kuba.eai.jms.clients.kafka.KafkaConnection");
//		registerImpl("ems", "kuba.eai.jms.clients.ems.EmsConnection");
//		registerImpl("tibjms2", "kuba.eai.jms.clients.ems.Tibjms2Connection");
	}
	
	public static ConnectionImpl create(String impl, int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		if (impl==null)
			throw new JMSException("No implementation for given url");
		ConnectionImpl conn = implementations.get(impl);
		if (conn==null)
			throw new JMSException("No implementation for "+impl);
		if (properties==null)
			properties = new HashMap<>();
		return conn.create(i, url, user, pass, clientId, properties);
	}

	public abstract ConnectionImpl create(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException;
	
	
	public ConnectionConsumer createConnectionConsumer(Queue arg0, String arg1, ServerSessionPool arg2, int arg3)
			throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	public ConnectionConsumer createConnectionConsumer(Topic arg0, String arg1, ServerSessionPool arg2, int arg3)
			throws JMSException {
		throw new JMSException("Server side API not supported");
	}
	
	public ConnectionConsumer createConnectionConsumer(Destination arg0, String arg1, ServerSessionPool arg2, int arg3)
			throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	public ConnectionConsumer createDurableConnectionConsumer(Topic arg0, String arg1, String arg2,
			ServerSessionPool arg3, int arg4) throws JMSException {
		throw new JMSException("Server side API not supported");
	}
	
	public Session createSession() throws JMSException {
		return createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	public Session createSession(int ackMode) throws JMSException {
		return createSession(false, ackMode);
	}

	public ConnectionConsumer createSharedConnectionConsumer(Topic arg0, String arg1, String arg2,
			ServerSessionPool arg3, int arg4) throws JMSException {
		throw new JMSException("Server side API not supported");
	}

	public ConnectionConsumer createSharedDurableConnectionConsumer(Topic arg0, String arg1, String arg2,
			ServerSessionPool arg3, int arg4) throws JMSException {
		throw new JMSException("Server side API not supported");
	}	
}
