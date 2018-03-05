# extrajms
This is JMS client for Apache Kafka. In addition it is JMS client for Tibco EMS using original libs, but with ems:// or tibjms2:// url. Now explanation why tibjms2:// - Tibco EMS can run in JMS 2.0 mode, but Tibco BusinessWorks 5.x cannot, so the only way to consume the same topic with many sessions is to wrap JMS 2.0 topics as JMS 1.1 queue (tada!).
How to use Kafka as JMS?

```java
kuba.eai.jms.clients.common.InitialContextFactory = new InitialContextFactory();
Hashtable h = new Hashtable<>();
h.put(InitialContext.PROVIDER_URL, "kafka://10.0.2.15:9092");
InitialContext ctx = icf.getInitialContext(h);
QueueConnectionFactory qcfSend = (QueueConnectionFactory) ctx.lookup("QueueConnectionFactory");
QueueConnection qcSend = qcfSend.createQueueConnection();
qcSend.setClientID("my-app");
QueueSession sessSend = qcSend.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
QueueSender sender = sessSend.createSender(sessSend.createQueue("test-topic"));
sender.setDeliveryMode(DeliveryMode.PERSISTENT);
TextMessage m = sessSend.createTextMessage(new Date().toString());
sender.send(m);
sender.close();
sessSend.close();
qcSend.close();
```
Please notice that having InitialContextFactory class hardcoded we can switch to different messaging just with URL.
Back to Kafka as JMS: what is supported? delivery modes and acknowledge modes.
What is not supported? authentication and JMS selectors.

