package kuba.eai.jms.clients.ems.tests;

import java.util.Hashtable;

import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;

import kuba.eai.jms.clients.common.InitialContext;
import kuba.eai.jms.clients.common.InitialContextFactory;

public class CtxTest {
	
	public final static void main(String[] args) throws Throwable {
		InitialContextFactory icf = new InitialContextFactory();			
		Hashtable<? super Object,? super Object> h = new Hashtable<>();
		h.put(InitialContext.PROVIDER_URL, "ems://127.0.0.1:7222");
		QueueConnectionFactory recv = (QueueConnectionFactory) icf.getInitialContext(h).lookup("QueueConnectionFactory");
		QueueConnection qc = recv.createQueueConnection();
		QueueSession qs = qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		qs.createQueue("test");
		qs.close();
		qc.close();		
	}
}
