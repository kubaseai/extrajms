package kuba.eai.jms.clients.ems;

import java.util.HashMap;

import javax.jms.JMSException;
import javax.jms.QueueSession;

import com.tibco.tibjms.TibjmsTopicConnectionFactory;

import kuba.eai.jms.clients.common.ConnectionImpl;

public class Tibjms2Connection extends EmsConnection {
	
	public static final ConnectionImpl FACTORY = new Tibjms2Connection();
	private int factoryType = 0;
	
	public Tibjms2Connection(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		TibjmsTopicConnectionFactory connFactory = new TibjmsTopicConnectionFactory();
		setupConnectionFactory(connFactory, url, user, pass, clientId, properties);
		conn = connFactory.createConnection(user, pass);
		factoryType = i;
	}

	public Tibjms2Connection() {}

	@Override
	public QueueSession createQueueSession(boolean tr, int ackMode) throws JMSException {
		String subName = conn.getClientID();
		if (subName == null)
			subName = conn.toString();
		return new Tibjms2Session(conn.createSession(tr, ackMode), factoryType, subName);
	}
	
	@Override
	public ConnectionImpl create(int i, String url, String user, String pass, String clientId,
			HashMap<String, String> properties) throws JMSException {
		return new Tibjms2Connection(i, url, user, pass, clientId, properties);
	}
}
