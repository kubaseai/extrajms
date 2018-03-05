package kuba.eai.jms.clients.ems.tests;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSession;

import kuba.eai.jms.clients.ems.Tibjms2Connection;

public class Jms2 {
	
	public final static void main(String[] args) throws JMSException, InterruptedException {
		final ConcurrentHashMap<String,String> map = new ConcurrentHashMap<>();
		final AtomicLong sendCnt = new AtomicLong(0);
		Tibjms2Connection conn = new Tibjms2Connection(0, "tcp://localhost:7222", "admin", null, "java-jms2-test", null);
		conn.start();
		final TopicSession prodSess = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
		final QueueSession[] recvSess = new QueueSession[5];
		for (int i = 0; i < recvSess.length; i++)
			recvSess[i] = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		final MessageProducer mp = prodSess.createProducer(prodSess.createTopic("jms2_topic"));
		new Thread() {
			public void run() {
				while (true) {
					try {
						mp.send( prodSess.createTextMessage(sendCnt.incrementAndGet()+"") );
					}
					catch (JMSException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		for (int i=0; i < recvSess.length; i++) {
			final int k = i;
			new Thread() {
				public void run() {
					QueueReceiver qr;
					try {
						qr = recvSess[k].createReceiver(recvSess[k].createQueue("jms2_topic"));
						System.out.println("QueueReceiver "+k);
						while (true) {
							Message m = qr.receive(1000);
							if (m!=null) {
								TextMessage tm = (TextMessage) m;
								String hit = map.put(tm.getText(), tm.getText());
								if (hit!=null)
									System.err.println(tm.getText());
								else
									;
							}
						}
					}
					catch (JMSException e) {
						e.printStackTrace();
					}
				}
			}.start();			
		}
		Thread.sleep(60000);
		conn.close();
		System.exit(0);		
	}

}
