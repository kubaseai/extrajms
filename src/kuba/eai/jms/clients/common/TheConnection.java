package kuba.eai.jms.clients.common;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;


public class TheConnection implements Connection,QueueConnection,TopicConnection {
	
	public final static int STATUS_ALIVE = 1;
	public final static int STATUS_INVALID = 2;
	public final static int STATUS_CLOSED_BY_REQ = 3;
	private static final String PING_WATERMARK = "...PING...";
	
	public static int CONN_RETRY_TIMEOUT = 9000;
	private static int CONN_RECV_TIMEOUT = 10000;
	
	private ConnectionImpl connection = null;
	private String url = null;
	private String user = null;
	private String pass = null;
	private String clientId = null;
	private ExceptionListener excListener = null;
	private Integer status = new Integer(STATUS_INVALID);	
	private AtomicBoolean running = new AtomicBoolean(false);
	private Integer connType = null;
	private HashMap<String,String> properties = new HashMap<>();
	private String impl = null;
	private Thread connectionRenewer = null;
	private final static boolean enablePinger = false;
	
	public void setStatus(int status) {
		synchronized(this.status) {
			if (this.status==STATUS_ALIVE && status==STATUS_INVALID)
				Logger.warn("Connection is damaged "+url);
			this.status = status;			
		}				
	}	
	
	public int getStatus() {
		synchronized(status) {
			return status;
		}		
	}		
	
	private void createConnection() throws JMSException {
		if (Integer.valueOf(1).equals(connType))
			createQueueConnection();
		else if (Integer.valueOf(2).equals(connType))
			createTopicConnection();
		else {
			connection = ConnectionImpl.create(impl, connType!=null ? connType : 0, url, user, pass, clientId, properties);
		}
	}
	
	private void createQueueConnection() throws JMSException {
		try {
			connection = ConnectionImpl.create(impl, 1, url, user, pass, clientId, properties);
			connection.setExceptionListener(new InternalExceptionListener(this));
			connection.start();
			_checkAliveAndSetStatus();			
		}
		catch (JMSException jmse) {
			setStatus(STATUS_INVALID);
			throw jmse;
		}
	}
	
	private void createTopicConnection() throws JMSException {
		try {
			connection = ConnectionImpl.create(impl, 2, url, user, pass, clientId, properties);
			connection.setExceptionListener(new InternalExceptionListener(this));
			connection.start();
			_checkAliveAndSetStatus();		
		}
		catch (JMSException jmse) {
			setStatus(STATUS_INVALID);
			throw jmse;
		}
	}

	@SuppressWarnings("rawtypes")
	public TheConnection(Hashtable params) {
		String url = (String) params.get(InitialContext.PROVIDER_URL);
		user = (String) params.get(InitialContext.SECURITY_PRINCIPAL);
		pass = (String) params.get(InitialContext.SECURITY_CREDENTIALS);
		clientId = (String) params.get(InitialContext.CLIENT_ID);
		connType = (Integer) params.get(InitialContext.CONN_TYPE);
		int pos = (url+"").indexOf("://");
		if (pos==-1)
			throw new RuntimeException("Invalid URL "+url);
		else
			impl = url.substring(0, pos);
		LinkedList<String> hosts = new LinkedList<>();
		for (String s : url.split("\\,")) {
			if (s.contains("=")) {
				String[] kv = s.split("\\=");
				properties.put(kv[0], kv[1]);
			}
			else
				hosts.add(s.startsWith(impl) ? s.substring(impl.length()+3) : s);
		}
		this.url = hosts.toString().replace("[", "").replace("]", "");
		
		try {
			createConnection();				
		}
		catch (Exception ex) {
			throw new RuntimeException("Cannot create connection: "+ex, ex);
		}
		start();
	}

	private void checkConnectionNonNull() throws JMSException {
		if (connection==null)
			throw new JMSException("No connection");
	}
	
	public QueueSession createQueueSession(boolean transactional, int ackMode)
			throws JMSException {
		checkConnectionNonNull();
		return connection.createQueueSession(transactional, ackMode);
	}

	public TopicSession createTopicSession(boolean transactional, int ackMode)
			throws JMSException {
		checkConnectionNonNull();
		return connection.createTopicSession(transactional, ackMode);
	}
	
	public Session createSession(boolean transactional, int ackMode) throws JMSException {
		checkConnectionNonNull();
		return connection.createSession(transactional, ackMode);
	}	
	
	public ConnectionConsumer createConnectionConsumer(Destination dest,
			String messageSelector, ServerSessionPool ssp, int maxMsgs) throws JMSException {
		checkConnectionNonNull();
		return connection.createConnectionConsumer(dest, messageSelector, ssp, maxMsgs);
	}

	public ConnectionConsumer createDurableConnectionConsumer(Topic dest,
			String subscription, String messageSelector, ServerSessionPool ssp, int maxMsgs)
			throws JMSException {
		checkConnectionNonNull();
		return connection.createDurableConnectionConsumer(dest, subscription, messageSelector, ssp, maxMsgs);
	}
	
	public ConnectionConsumer createConnectionConsumer(Topic dest,
			String messageSelector, ServerSessionPool ssp, int maxMsgs) throws JMSException {
		checkConnectionNonNull();
		return connection.createConnectionConsumer(dest, messageSelector, ssp, maxMsgs);
	}

	public ConnectionConsumer createConnectionConsumer(Queue dest,
			String messageSelector, ServerSessionPool ssp, int maxMsgs) throws JMSException {
		checkConnectionNonNull();
		return connection.createConnectionConsumer(dest, messageSelector, ssp, maxMsgs);
	}	

	public void close() throws JMSException {
		running.set(false);
		try {
			setStatus(STATUS_CLOSED_BY_REQ);
			if (connection!=null)
				connection.close();				
		}
		catch (Exception exc) {
			Logger.swallow(exc);
		}
		finally {				
			connection = null;
		}				
	}	

	public String getClientID() throws JMSException {
		return clientId;
	}

	public ExceptionListener getExceptionListener() throws JMSException {
		return excListener;
	}

	public ConnectionMetaData getMetaData() throws JMSException {
		return TheConnectionMetaData.METADATA;
	}

	public void setClientID(String clid) throws JMSException {
		if ((clientId!=null && clientId.equals(clid)) || (clientId==null && clid==null))
			return;
		this.clientId = clid;
		Logger.debug("ClientID="+clid);
		if (connection!=null)
			connection.setClientID(clid!=null ? clid : null);		
	}

	public void setExceptionListener(ExceptionListener excl)
			throws JMSException {
		this.excListener = excl;		
	}

	public void start() {	
		Logger.debug("Connection start "+this);
		if (connectionRenewer!=null) {
			running.set(false);
			try {
				connectionRenewer.join();
			}
			catch (InterruptedException e) {}
		}
		running.set(true);
		if (enablePinger) {
			connectionRenewer = new Thread() {
				public void run() {
					setName("Connection renewer "+getId());
					Logger.debug("Starting connection renewer "+Thread.currentThread());
					while (running.get()) {
						renewConnection();
						try {
							Thread.yield();
							Thread.sleep(CONN_RETRY_TIMEOUT);
						}
						catch (InterruptedException e) {}
					}
					Logger.debug("Stopping connection renewer "+Thread.currentThread());
				}
			};
			connectionRenewer.setDaemon(true);
			connectionRenewer.start();
		}
	}
	
	public void stop() throws JMSException {
		Logger.debug("Connection stop "+url);
		if (getStatus() == STATUS_ALIVE && connection!=null) {
			try {
				connection.stop();
			}
			catch (Exception ex) {
				Logger.warn("Cannot stop connection "+url+": "+ ex.toString());
			}
		}
	}

	private void _checkAliveAndSetStatus() {
		if (!enablePinger)
			return;
		Session sessPing = null;
		TemporaryQueue tq = null;
		MessageProducer mp = null;
		MessageConsumer mc = null;
		boolean alive = false;
		try {
			sessPing = connection.createSession(false,	Session.AUTO_ACKNOWLEDGE);
			tq = sessPing.createTemporaryQueue();
			mp = sessPing.createProducer(tq);
			Message m = sessPing.createTextMessage(PING_WATERMARK);

			m.setJMSExpiration(CONN_RECV_TIMEOUT);
			mp.send(m);
			mc = sessPing.createConsumer(tq);
			Message rm = mc.receive(CONN_RECV_TIMEOUT);

			alive = rm != null && rm.getJMSMessageID().equals(m.getJMSMessageID());

			if (!alive)
				throw new Exception("No alive response for "+ CONN_RECV_TIMEOUT);	
			setStatus(STATUS_ALIVE);
		}
		catch (Exception e) {
			if (getStatus() != STATUS_CLOSED_BY_REQ) {
				setStatus(STATUS_INVALID);
				Logger.error("Connection alive check failed for " + url, e);
			}
		}
		finally {
			try {
				if (mp != null)
					mp.close();
			}
			catch (Exception exc) {}

			try {
				if (mc != null)
					mc.close();
			}
			catch (Exception exc) {}

			try {
				if (tq != null)
					tq.delete();
			}
			catch (Exception exc) {}

			try {
				if (sessPing != null)
					sessPing.close();
			}
			catch (Exception exc) {}

			if (!alive) {
				try {
					connection.close();
				}
				catch (Exception exc) {}
			}
		}
	}
	
	private void renewConnection() {
		if (!running.get())
			return;
		if (getStatus() == STATUS_INVALID) {
			try {
				createConnection();
				Logger.warn("Connection is alive again "+url);
			}
			catch (Exception ex) {}	
		}
		else 
			_checkAliveAndSetStatus();
	}	

	public String getConnectionUrl() {
		return url;
	}

	public boolean isUserClosed() {
		return getStatus() == STATUS_CLOSED_BY_REQ;
	}
	
	public void finalize() {
		try {
			close();
		}
		catch (JMSException e) {}
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

