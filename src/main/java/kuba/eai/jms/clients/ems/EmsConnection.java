package kuba.eai.jms.clients.ems;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import com.tibco.tibjms.TibjmsConnectionFactory;
import com.tibco.tibjms.TibjmsQueueConnectionFactory;
import com.tibco.tibjms.TibjmsTopicConnectionFactory;

import kuba.eai.jms.clients.common.ConnectionImpl;

public class EmsConnection extends ConnectionImpl {
	
	protected Connection conn = null;
	public static final ConnectionImpl FACTORY = new EmsConnection();
	
	public EmsConnection() {}
	

	private void setupTimeouts(TibjmsConnectionFactory connFactory, int timeout) {
		for (String method : new String[] { "setConnAttemptTimeout", "setReconnAttemptTimeout"}) {
			try {
				connFactory.getClass().getDeclaredMethod(method, Integer.class).invoke(connFactory, timeout);
			}
			catch (Throwable t) {
				//sorry, JMS client is too old
			}
		}
	}

	protected void setupConnectionFactory(TibjmsConnectionFactory connFactory, String url, String user, String pass,
		String clientId, HashMap<String, String> props) throws JMSException 
	{
		connFactory.setClientID(clientId);
		connFactory.setConnAttemptCount(5);
		//connFactory.setConnAttemptTimeout(10000); //Not in 4.1, but we use online maven repos
		//connFactory.setReconnAttemptTimeout(10000); //Not in 4.1, but we use online maven repos
		setupTimeouts(connFactory, 10000);
		connFactory.setReconnAttemptCount(Integer.MAX_VALUE);
		connFactory.setReconnAttemptDelay(1000);		
		connFactory.setServerUrl(url);
		connFactory.setUserName(user);
		connFactory.setUserPassword(pass);
		if (props==null)
			props = new HashMap<>();
		connFactory._properties = new HashMap<String,String>(props);
	}
	
	public EmsConnection(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		if (i==1) {
			TibjmsQueueConnectionFactory connFactory = new TibjmsQueueConnectionFactory();
			setupConnectionFactory(connFactory, url, user, pass, clientId, properties);
			conn = connFactory.createQueueConnection(user, pass);
		}
		else if (i==2) {
			TibjmsTopicConnectionFactory connFactory = new TibjmsTopicConnectionFactory();
			setupConnectionFactory(connFactory, url, user, pass, clientId, properties);
			conn = connFactory.createTopicConnection(user, pass);
		}
		else {
			TibjmsConnectionFactory connFactory = new TibjmsConnectionFactory();
			setupConnectionFactory(connFactory, url, user, pass, clientId, properties);
			conn = connFactory.createConnection(user, pass);
		}
	}

	@Override
	public void close() throws JMSException {
		conn.close();		
	}

	@Override
	public Session createSession(boolean tr, int ackMode) throws JMSException {
		return conn.createSession(tr, ackMode);
	}

	@Override
	public String getClientID() throws JMSException {
		return conn.getClientID();
	}

	@Override
	public ExceptionListener getExceptionListener() throws JMSException {
		return conn.getExceptionListener();
	}

	@Override
	public ConnectionMetaData getMetaData() throws JMSException {
		return conn.getMetaData();
	}

	@Override
	public void setClientID(String cl) throws JMSException {
		conn.setClientID(cl);		
	}

	@Override
	public void setExceptionListener(ExceptionListener l) throws JMSException {
		conn.setExceptionListener(l);		
	}

	@Override
	public void start() throws JMSException {
		conn.start();		
	}

	@Override
	public void stop() throws JMSException {
		conn.stop();		
	}

	@Override
	public QueueSession createQueueSession(boolean tr, int ackMode) throws JMSException {
		return ((QueueConnection)conn).createQueueSession(tr, ackMode);
	}

	@Override
	public TopicSession createTopicSession(boolean tr, int ackMode) throws JMSException {
		return ((TopicConnection)conn).createTopicSession(tr, ackMode);
	}

	@Override
	public ConnectionImpl create(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		return new EmsConnection(i, url, user, pass, clientId, properties);
	}	
}
