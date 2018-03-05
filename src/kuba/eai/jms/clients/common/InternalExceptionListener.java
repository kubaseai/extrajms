package kuba.eai.jms.clients.common;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public class InternalExceptionListener implements ExceptionListener {

	private TheConnection conn = null;
			
	public InternalExceptionListener(TheConnection connection) {
		this.conn = connection;		
	}

	public void onException(JMSException jmsexception) {
		conn.setStatus(TheConnection.STATUS_INVALID);		
	}
}
