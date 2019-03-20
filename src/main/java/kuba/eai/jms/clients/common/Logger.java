package kuba.eai.jms.clients.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;

public class Logger {
	
	private Object objLogger = null;
	private Method debug = null;
	
	private final static class LoggerHolder {
		public final static Logger LOGGER = new Logger();
	}
	
	private Logger() {
		try {
			Class<?> clazz = Class.forName("org.apache.log4j.Logger");
			objLogger = clazz.getDeclaredMethod("getLogger", new Class[] {String.class})
				.invoke(null, new Object[]{"bw.logger"});
			debug = clazz.getMethod("info", new Class[]{Object.class});
		}
		catch (Exception e) {};
	}
	
	public static Logger getInstance() {
		return LoggerHolder.LOGGER;
	}
	
	private void _debug(String msg) {
		boolean done = false;
		if (debug!=null) {
			try {
				debug.invoke(objLogger, new Object[] { msg });
				done = true;
			}
			catch (Exception e) {}
		}
		if (!done) {
			System.out.println(msg);
		}
	}
	
	public final static void debug(String msg) {
		if ("true".equals(System.getProperty("jms.debug","false")))
			getInstance()._debug(msg);
	}
	public final static void warn(String msg) {
		getInstance()._debug(msg);
	}
	public final static void trace(String msg) {
		if ("true".equals(System.getProperty("jms.trace","false")))
			getInstance()._debug(msg);
	}
	public final static void error(String msg, Throwable t) {
		getInstance()._error(msg, t);
	}

	private void _error(String msg, Throwable t) {
		StringWriter sw = new StringWriter();
		t.printStackTrace(new PrintWriter(sw));
		_debug(msg+" "+sw.toString());		
	}

	public static void swallow(Exception e) {
		if ("true".equals(System.getProperty("jms.debug","false")))
			e.printStackTrace();	
	}

	public static void traceSendReceive(Exception e) {
		if ("true".equals(System.getProperty("jms.debug","false")))
			e.printStackTrace();		
	}
}
