package kuba.eai.jms.clients.common;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class Message implements javax.jms.Message, TextMessage, Serializable {
	
	private static final long serialVersionUID = 1L;
	public final static int DELIVERY_MODE_NON_PERSISTENT = DeliveryMode.NON_PERSISTENT;
	public final static int DELIVERY_MODE_PERSISTENT = DeliveryMode.PERSISTENT;
	public final static int DELIVERY_MODE_DEFAULT = 0;
	
	private HashMap<String,String> p = new HashMap<String,String>();
	private String msgId = null;
	private String body = null;
	private transient MessageAck ack = null;
		
	public Message() {}

	public Message(String key, String value, Headers headers, MessageAck ack) throws JMSException {
		if (headers!=null) {
			Iterator<Header> it = headers.iterator();
			while (it!=null && it.hasNext()) {
				Header h = it.next();
				p.put(h.key(), new String(h.value()));
			}
		}
		if (key!=null && key.startsWith("JMSMessageID=")) { // headers in key
			loadSerializedProperties(key);
			msgId = key = p.get("JMSMessageID");
		}
		else {
			p.put("JMSMessageID", msgId = key);
		}
		
		p.put("Body", body = value);
		this.ack = ack;		
	}
	
	

	@Override
	public void acknowledge() throws JMSException {
		if (ack!=null) {
			ack.ack();	
		}
	}

	@Override
	public void clearBody() throws JMSException {
		p.put("Body", body = null);		
	}

	@Override
	public void clearProperties() throws JMSException {
		p.clear();
		p.put("JMSMessageID", msgId);
	}

	@Override
	public boolean getBooleanProperty(String p) throws JMSException {
		try {
			String v = this.p.get(p);
			if ("1".equals(v))
				return true;
			if ("0".equals(v))
				return false;
			return Boolean.valueOf(v);
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}		
	}

	@Override
	public byte getByteProperty(String p) throws JMSException {
		try {
			return Byte.valueOf(this.p.get(p));
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}
	}

	@Override
	public double getDoubleProperty(String p) throws JMSException {
		try {
			return Double.valueOf(this.p.get(p));
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}
	}

	@Override
	public float getFloatProperty(String p) throws JMSException {
		try {
			return Float.valueOf(this.p.get(p));
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}
	}

	@Override
	public int getIntProperty(String p) throws JMSException {
		try {
			return Integer.valueOf(this.p.get(p));
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}
	}

	@Override
	public String getJMSCorrelationID() throws JMSException {
		return p.get("JMSCorrelationID");
	}

	@Override
	public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
		String id = p.get("JMSCorrelationID");
		if (id!=null)
			try {
				return id.getBytes("utf-8");
			}
			catch (UnsupportedEncodingException e) {
				return id.getBytes();
			}
		return new byte[0];
	}

	@Override
	public int getJMSDeliveryMode() throws JMSException {
		String dm = this.p.get("JMSDeliveryMode");
		if ("persistent".equalsIgnoreCase(dm))
			return Message.DELIVERY_MODE_PERSISTENT;
		else if ("non_persistent".equalsIgnoreCase(dm))
			return Message.DELIVERY_MODE_NON_PERSISTENT;
		return Message.DELIVERY_MODE_DEFAULT;
	}

	@Override
	public Destination getJMSDestination() throws JMSException {
		String dest = this.p.get("JMSDestination");
		return dest!=null ? new TheDestination(dest) : null;
	}

	@Override
	public long getJMSExpiration() throws JMSException {
		String exp = this.p.get("JMSExpiration");
		if (exp==null || exp.length()==0)
			return 0L;
		try {
			return Long.valueOf(exp);
		}
		catch (Exception e) {
			return 0L;
		}
	}

	@Override
	public String getJMSMessageID() throws JMSException {
		return this.p.get("JMSMessageID");
	}

	@Override
	public int getJMSPriority() throws JMSException {
		String prio = this.p.get("JMSPriority");
		if (prio!=null && prio.length()>0) {
			try {
				return Integer.valueOf(prio);
			}
			catch (Exception e) {}
		}
		return 4;
		
	}

	@Override
	public boolean getJMSRedelivered() throws JMSException {
		return false;
	}

	@Override
	public Destination getJMSReplyTo() throws JMSException {
		String reply = this.p.get("JMSReplyTo");
		return reply!=null ? new TheDestination(reply) : null;
	}

	@Override
	public long getJMSTimestamp() throws JMSException {
		String ts = this.p.get("JMSTimestamp");
		if (ts!=null && ts.length()>0) {
			try {
				return Long.valueOf(ts);
			}
			catch (Exception e) {}
		}
		return 0;
	}

	@Override
	public String getJMSType() throws JMSException {
		return this.p.get("JMSType");		
	}

	@Override
	public long getLongProperty(String p) throws JMSException {
		try {
			return Long.valueOf(this.p.get(p));
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}		
	}

	@Override
	public Object getObjectProperty(String arg0) throws JMSException {
		return p.get(arg0);
	}

	public Enumeration<?> getPropertyNames(boolean withJMS) throws JMSException {
		final LinkedList<String> filteredKeys = new LinkedList<>();
		for (String key : p.keySet()) {
			boolean isJMSHeader = key.startsWith("JMS") && !key.startsWith("JMSX");
			if ((!isJMSHeader || withJMS) && !"Body".equalsIgnoreCase(key))
				filteredKeys.add(key);
		}
		return new Enumeration<String>() {
			
			int i=0;

			@Override
			public boolean hasMoreElements() {
				return i >= 0 && i <= filteredKeys.size()-1;
			}

			@Override
			public String nextElement() {
				if (i > filteredKeys.size()-1)
					return null;
				return filteredKeys.get(i++);
			}
		};
	}
	
	public void loadSerializedProperties(String key) throws JMSException {
		Properties h = new Properties();
		try {
			h.load(new StringReader(key));
		}
		catch (IOException ioe) {
			throw new JMSException("Cannot deserialize JMS Headers: "+ioe);
		}
		key = h.getProperty("JMSMessageID");
		Enumeration<?> en = h.propertyNames();
		while (en.hasMoreElements()) {
			String k = en.nextElement().toString();
			String v = h.getProperty(k);
			if (v!=null)
				p.put(k, v);
		}
	}
	
	public String getSerializedProperties() throws JMSException {
		Properties p = new Properties();
		Enumeration<?> en = getPropertyNames(true);
		while (en.hasMoreElements()) {
			String key = en.nextElement().toString();
			String val = getStringProperty(key);
			if (val!=null && !key.equalsIgnoreCase("JMSMessageID"))
				p.put(key, val);
		}
		StringWriter sw = new StringWriter();
		sw.append("JMSMessageID=").append(getJMSMessageID()).append("\r\n");
		try {
			p.store(sw, null);
		}
		catch (IOException ioe) {
			throw new JMSException("JMS Headers serialization error: "+ioe);
		}		
		return sw.toString();
	}
	
	@Override
	public Enumeration<?> getPropertyNames() throws JMSException {
		return getPropertyNames(false);
	}

	@Override
	public short getShortProperty(String p) throws JMSException {
		try {
			return Short.valueOf(this.p.get(p));
		}
		catch (Exception e) {
			throw new JMSException(this.p.get(p));
		}		
	}

	@Override
	public String getStringProperty(String arg0) throws JMSException {
		return p.get(arg0);
	}

	@Override
	public boolean propertyExists(String arg0) throws JMSException {
		return p.containsKey(arg0);
	}

	@Override
	public void setBooleanProperty(String arg0, boolean arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setByteProperty(String arg0, byte arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setDoubleProperty(String arg0, double arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setFloatProperty(String arg0, float arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setIntProperty(String arg0, int arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setJMSCorrelationID(String arg0) throws JMSException {
		p.put("JMSCorrelationID", arg0);		
	}

	@Override
	public void setJMSCorrelationIDAsBytes(byte[] arg0) throws JMSException {
		p.put("JMSCorrelationID", new String(arg0));		
	}

	@Override
	public void setJMSDeliveryMode(int arg0) throws JMSException {
		String dm = "DEFAULT";
		if (arg0==Message.DELIVERY_MODE_PERSISTENT)
			dm = "PERSISTENT";
		else if (arg0==Message.DELIVERY_MODE_NON_PERSISTENT)
			dm = "NON_PERSISTENT";
		p.put("JMSDeliveryMode", dm);		
	}

	@Override
	public void setJMSDestination(Destination arg0) throws JMSException {
		if (arg0!=null)
			p.put("JMSDestination", arg0.toString());		
	}

	@Override
	public void setJMSExpiration(long arg0) throws JMSException {
		p.put("JMSExpiration", arg0+"");		
	}

	@Override
	public void setJMSMessageID(String arg0) throws JMSException {
		p.put("JMSMessageID", this.msgId = arg0);		
	}

	@Override
	public void setJMSPriority(int arg0) throws JMSException {
		p.put("JMSPriority", arg0+"");
		
	}

	@Override
	public void setJMSRedelivered(boolean arg0) throws JMSException {
		p.put("JMSRedelivered", arg0+"");		
	}

	@Override
	public void setJMSReplyTo(Destination arg0) throws JMSException {
		if (arg0!=null)
			p.put("JMSReplyTo", arg0+"");		
	}

	@Override
	public void setJMSTimestamp(long arg0) throws JMSException {
		p.put("JMSTimestamp", arg0+"");		
	}

	@Override
	public void setJMSType(String arg0) throws JMSException {
		p.put("JMSType", arg0);		
	}

	@Override
	public void setLongProperty(String arg0, long arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setObjectProperty(String arg0, Object arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setShortProperty(String arg0, short arg1) throws JMSException {
		p.put(arg0, arg1+"");		
	}

	@Override
	public void setStringProperty(String k, String v) throws JMSException {
		if ("JMSMessageID".equalsIgnoreCase(k))
			setJMSMessageID(v);
		else
			p.put(k, v);		
	}	

	@Override
	public String getText() throws JMSException {
		if (body!=null)
			return body;
		return body = p.get("Body");
	}

	@Override
	public void setText(String txt) throws JMSException {
		p.put("Body", body = txt);		
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		try {
			for (String key : p.keySet()) {
				if (!"Body".equalsIgnoreCase(key))
					sb.append("{").append(key).append(": ").append(p.get(key)).append("}, ");
			}
			sb.append(getText());
		}
		catch (JMSException e) {
			return super.toString();
		}
		return sb.toString();
	}

	@Override
	public <T> T getBody(Class<T> c) throws JMSException {
		if (String.class.equals(c))
			return c.cast(getText());
		return null;
	}

	public long getJMSDeliveryTime() throws JMSException {
		String dt = this.p.get("JMSDeliveryTime");
		if (dt==null || dt.length()==0)
			return 0L;
		try {
			return Long.valueOf(dt);
		}
		catch (Exception e) {
			return 0L;
		}
	}
	
	@Override
	public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class c) throws JMSException {
		return String.class.equals(c);
	}
	
	public void setJMSDeliveryTime(long t) throws JMSException {
		p.put("JMSDeliveryTime", t+"");		
	}
}
