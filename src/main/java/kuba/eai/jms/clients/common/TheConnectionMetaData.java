package kuba.eai.jms.clients.common;


import java.util.Enumeration;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

public class TheConnectionMetaData implements ConnectionMetaData {

	public final static TheConnectionMetaData METADATA = new TheConnectionMetaData();

	public int getJMSMajorVersion() throws JMSException {
		return 1;
	}

	public int getJMSMinorVersion() throws JMSException {
		return 1;
	}

	public String getJMSProviderName() throws JMSException {
		return "Extra JMS Provider";
	}

	public String getJMSVersion() throws JMSException {
		return getJMSMajorVersion() + "." + getJMSMinorVersion();
	}

	public Enumeration<String> getJMSXPropertyNames() throws JMSException {
		return new Enumeration<String>() {
			public boolean hasMoreElements() {
				return false;
			}

			public String nextElement() {
				return null;
			}
		};
	}

	public int getProviderMajorVersion() throws JMSException {
		return 1;
	}

	public int getProviderMinorVersion() throws JMSException {
		return 0;
	}

	public String getProviderVersion() throws JMSException {
		return getProviderMajorVersion() + "." + getProviderMinorVersion();
	}
}

