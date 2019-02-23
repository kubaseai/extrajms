package kuba.eai.jms.clients.common;

import java.util.Date;
import java.util.Hashtable;

import javax.jms.*;
import javax.naming.NamingException;

public class InitialContextFactory implements javax.naming.spi.InitialContextFactory {

		public InitialContext getInitialContext(Hashtable<?,?> map) throws NamingException {
			return new InitialContext(map);
		}
		
		public final static void main(String[] args) throws NamingException, JMSException {
			// https://github.com/danielwegener/logback-kafka-appender/issues/44
			InitialContextFactory icf = new InitialContextFactory();			
			Hashtable<? super Object,? super Object> h = new Hashtable<>();
			h.put(InitialContext.PROVIDER_URL, "kafka://10.87.40.80:9092,10.87.40.148:9092,acks=2");
			InitialContext ctx = icf.getInitialContext(h);
			QueueConnectionFactory qcf = (QueueConnectionFactory) ctx.lookup("QueueConnectionFactory");
			System.err.println("qcf "+qcf);
			QueueConnection qc = qcf.createQueueConnection();
			qc.setClientID("demo123");
			System.err.println("qc "+qc);
			QueueSession sess = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
			System.err.println("sess "+sess);
			QueueSender sender = sess.createSender(sess.createQueue("log-payloads"));
			sender.setDeliveryMode(2);
			System.err.println("sender "+sender);
			TextMessage m = sess.createTextMessage("demo "+new Date());
			System.err.println("msg "+m);
			sender.send(m);
			System.err.println("sent");
			QueueReceiver qr = sess.createReceiver(sess.createQueue("log-payloads"), /*"offset='0:165651'"*/ null);
			System.err.println("receiver "+qr);
			for (int i=0; i < 5; i++) {
				javax.jms.Message msg  = qr.receive(1000); // http://grokbase.com/t/kafka/users/15carrrkzg/new-consumer-0-9-api-poll-never-returns
				System.err.println("received "+msg);				
				if (msg!=null)
					msg.acknowledge();
			}
			qr.close();
			sess.close();
			qc.close();	
			System.err.println("done");
		}
}
