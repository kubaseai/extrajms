package kuba.eai.jms.clients.kafka.tests;

import java.util.Date;
import java.util.Hashtable;

import javax.jms.DeliveryMode;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import kuba.eai.jms.clients.common.InitialContext;
import kuba.eai.jms.clients.common.InitialContextFactory;

public class Availability {
	
	public static void main(String[] args) throws Exception {
		InitialContextFactory icf = new InitialContextFactory();			
		Hashtable<? super Object,? super Object> h = new Hashtable<>();
		h.put(InitialContext.PROVIDER_URL, "kafka://10.0.2.15:9092,127.0.0.1:9092,192.168.0.1:9092,acks=all");
		InitialContext ctx = icf.getInitialContext(h);
		QueueConnectionFactory qcfSend = (QueueConnectionFactory) ctx.lookup("QueueConnectionFactory");
		QueueConnection qcSend = qcfSend.createQueueConnection();
		h.put(InitialContext.PROVIDER_URL, "kafka://10.0.2.15:9092");
		QueueConnectionFactory qcfRecv = (QueueConnectionFactory) icf.getInitialContext(h).lookup("QueueConnectionFactory");
		QueueConnection qcRecv = qcfRecv.createQueueConnection();
		qcSend.setClientID("unit-test-consistency-send");
		qcRecv.setClientID("unit-test-consistency-recv");
		QueueSession sessSend = qcSend.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
		QueueSession sessRecv = qcRecv.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
		QueueSender sender = sessSend.createSender(sessSend.createQueue("log-payloads"));
		sender.setDeliveryMode(DeliveryMode.PERSISTENT);
		String uniqText = new Date().toString();
		TextMessage m = sessSend.createTextMessage(uniqText);
		sender.send(m);
		System.err.println("sent "+m.getJMSMessageID()+", will wait for sync");
		String off = m.getJMSMessageID();
		off = off.substring(off.indexOf(':')+1, off.length());	
		Thread.sleep(60000);
		QueueReceiver qr = sessRecv.createReceiver(sessRecv.createQueue("log-payloads"), "offset='"+off+"'");
		for (int i=0; i < 60; i++) {
			javax.jms.TextMessage msg  = (TextMessage) qr.receive(1000);
			if (msg!=null) {
				msg.acknowledge();
				if (msg.getText().contains(uniqText)) {
					System.err.println("match in "+i+". msg: "+msg.getJMSMessageID());
					break;
				}				
			}
		}
		qr.close();
		sender.close();
		qcRecv.close();
		sessSend.close();
		sessRecv.close();
		qcSend.close();
		qcRecv.close();
		System.err.println("done");
	}

}
