package kuba.eai.jms.clients.common;

import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;


@SuppressWarnings({ "rawtypes", "unchecked" })
public class InitialContext implements Context {
	
	public static final String CLIENT_ID = "CLIENT_ID";
	public static final String ENABLE_LOAD_BALANCING = "ENABLE_LB";
	public static final String CONN_TYPE = "CONN_TYPE";
	private Hashtable params = new Hashtable();

	public InitialContext(Hashtable map) {
		this.params.putAll(map);
	}

	public Object lookup(Name name) throws NamingException {
		if (name==null)
			return null;
		String nm = name.toString().toLowerCase();
		if (nm.contains("connection") && nm.contains("factory"))
			return new TheConnectionFactory(params, nm.contains("queue"), nm.contains("topic"));
		else if (nm.contains("connection"))
			return new TheConnection(params);		
		return new TheDestination(name.toString());			
	}

	public Object lookup(String name) throws NamingException {
		if (name==null)
			return null;
		String nm = name.toString().toLowerCase();
		if (nm.contains("connection") && nm.contains("factory"))
			return new TheConnectionFactory(params, nm.contains("queue"), nm.contains("topic"));
		else if (nm.contains("connection"))
			return new TheConnection(params);		
		return new TheDestination(name);
		
	}

	public void bind(Name name, Object obj) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void bind(String name, Object obj) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void rebind(Name name, Object obj) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void rebind(String name, Object obj) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void unbind(Name name) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void unbind(String name) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void rename(Name oldName, Name newName) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void rename(String oldName, String newName) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}

	public NamingEnumeration list(Name name) throws NamingException {
		final Object obj = lookup(name);
		final AtomicBoolean iterated = new AtomicBoolean(false);
		return new NamingEnumeration() {
			
			public Object nextElement() {
				if (!iterated.get()) {
					iterated.set(true);
					return obj;					
				}
				return null;
			}			
			public boolean hasMoreElements() {
				return !iterated.get();
			}			
			public Object next() throws NamingException {
				if (!iterated.get()) {
					iterated.set(true);
					return obj;					
				}
				return null;
			}			
			public boolean hasMore() throws NamingException {
				return !iterated.get();
			}			
			public void close() throws NamingException {}
		};
	}

	public NamingEnumeration list(String name) throws NamingException {
		final Object obj = lookup(name);
		final AtomicBoolean iterated = new AtomicBoolean(false);
		return new NamingEnumeration() {
			
			public Object nextElement() {
				if (!iterated.get()) {
					iterated.set(true);
					return obj;					
				}
				return null;
			}			
			public boolean hasMoreElements() {
				return !iterated.get();
			}			
			public Object next() throws NamingException {
				if (!iterated.get()) {
					iterated.set(true);
					return obj;					
				}
				return null;
			}			
			public boolean hasMore() throws NamingException {
				return !iterated.get();
			}			
			public void close() throws NamingException {}
		};
	}

	public NamingEnumeration listBindings(Name name) throws NamingException {
		throw new NamingException("Not supported at client side");	
	}
	public NamingEnumeration listBindings(String name) throws NamingException {
		throw new NamingException("Not supported at client side");	
	}
	public void destroySubcontext(Name name) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public void destroySubcontext(String name) throws NamingException {
		throw new NamingException("Not supported at client side");		
	}
	public Context createSubcontext(Name name) throws NamingException {
		throw new NamingException("Not supported at client side");
	}
	public Context createSubcontext(String name) throws NamingException {
		throw new NamingException("Not supported at client side");
	}
	public Object lookupLink(Name name) throws NamingException {
		return lookup(name);
	}
	public Object lookupLink(String name) throws NamingException {
		return lookup(name);
	}

	public NameParser getNameParser(Name name) throws NamingException {
		throw new NamingException("Not supported");
	}
	public NameParser getNameParser(String name) throws NamingException {
		throw new NamingException("Not supported");
	}
	public Name composeName(Name name, Name prefix) throws NamingException {
		throw new NamingException("Not supported");
	}
	public String composeName(String name, String prefix)
			throws NamingException {
		throw new NamingException("Not supported");
	}

	public Object addToEnvironment(String propName, Object propVal)
			throws NamingException {
		throw new NamingException("Not supported at client side");
	}
	public Object removeFromEnvironment(String propName) throws NamingException {
		throw new NamingException("Not supported at client side");
	}

	public Hashtable getEnvironment() throws NamingException {
		return params;
	}

	public void close() throws NamingException {}

	public String getNameInNamespace() throws NamingException {
		return "";
	}
}
