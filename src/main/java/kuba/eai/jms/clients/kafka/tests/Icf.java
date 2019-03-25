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

public class Icf {
    public final static void main(String[] args) throws Exception {
        // https://github.com/danielwegener/logback-kafka-appender/issues/44
        InitialContextFactory icf = new InitialContextFactory();			
        Hashtable<? super Object,? super Object> h = new Hashtable<>();
        h.put(InitialContext.PROVIDER_URL, "kafka://10.87.40.80:9092,10.87.40.148:9092,acks=2");
        InitialContext ctx = icf.getInitialContext(h);
        QueueConnectionFactory qcf = (QueueConnectionFactory) ctx.lookup("QueueConnectionFactory");
        try (QueueConnection qc = qcf.createQueueConnection(); QueueSession sess = qc.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
            QueueSender sender = sess.createSender(sess.createQueue("log-payloads"));
            QueueReceiver qr = sess.createReceiver(sess.createQueue("log-payloads"), /*"offset='0:165651'"*/ null)) {
            System.err.println("qcf "+qcf);
            qc.setClientID("demo123");
            System.err.println("qc "+qc);
            System.err.println("sess "+sess);
            sender.setDeliveryMode(2);
            System.err.println("sender "+sender);
            TextMessage m = sess.createTextMessage("demo "+new Date());
            System.err.println("msg "+m);
            sender.send(m);
            System.err.println("sent");
            System.err.println("receiver "+qr);
            for (int i=0; i < 5; i++) {
                javax.jms.Message msg  = qr.receive(1000); // http://grokbase.com/t/kafka/users/15carrrkzg/new-consumer-0-9-api-poll-never-returns
                System.err.println("received "+msg);				
                if (msg!=null)
                    msg.acknowledge();
            }
            System.err.println("done");
        }
    }
}