package kuba.eai.jms.clients.common;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;


public class TheConnectionFactory implements ConnectionFactory,QueueConnectionFactory,TopicConnectionFactory {

	protected Hashtable<? super Object,? super Object> params = null;
	private int kind = 0;
	
	public TheConnectionFactory(Hashtable<?,?> params, boolean q, boolean t) {
		this.params = new Hashtable<>(params);	
		kind = q ? 1 : (t? 2 : 0);
	}

	public TheConnectionFactory(String url) {
		this(null, false, false);
		if (url!=null)
			params.put(InitialContext.PROVIDER_URL, url);
		params.put(InitialContext.CONN_TYPE, kind);
	}

	public TopicConnection createTopicConnection() throws JMSException {
		return new TheConnection(params);
	}

	public TopicConnection createTopicConnection(String user, String pass)
			throws JMSException {
		if (user!=null)
			params.put(InitialContext.SECURITY_PRINCIPAL, user);
		if (pass!=null)
			params.put(InitialContext.SECURITY_CREDENTIALS, pass);
		params.put(InitialContext.CONN_TYPE, 2);
		return new TheConnection(params);
	}

	public QueueConnection createQueueConnection() throws JMSException {
		params.put(InitialContext.CONN_TYPE, 1);
		return new TheConnection(params);
	}

	public QueueConnection createQueueConnection(String user, String pass)
			throws JMSException {
		if (user!=null)
			params.put(InitialContext.SECURITY_PRINCIPAL, user);
		if (pass!=null)
			params.put(InitialContext.SECURITY_CREDENTIALS, pass);
		params.put(InitialContext.CONN_TYPE, 1);
		return new TheConnection(params);
	}

	public Connection createConnection() throws JMSException {
		return new TheConnection(params);
	}

	public Connection createConnection(String user, String pass)
			throws JMSException {
		if (user!=null)
			params.put(InitialContext.SECURITY_PRINCIPAL, user);
		if (pass!=null)
			params.put(InitialContext.SECURITY_CREDENTIALS, pass);
		return new TheConnection(params);
	}

	public JMSContext createContext() {
		throw new RuntimeException("not implemented");
	}

	public JMSContext createContext(int arg0) {
		throw new RuntimeException("not implemented");
	}

	public JMSContext createContext(String arg0, String arg1) {
		throw new RuntimeException("not implemented");
	}

	public JMSContext createContext(String arg0, String arg1, int arg2) {
		throw new RuntimeException("not implemented");
	}
}
