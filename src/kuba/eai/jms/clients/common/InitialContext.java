package kuba.eai.jms.clients.common;

import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

public class InitialContext implements Context {
	
	public static final String CLIENT_ID = "CLIENT_ID";
	public static final String ENABLE_LOAD_BALANCING = "ENABLE_LB";
	public static final String CONN_TYPE = "CONN_TYPE";
	private final static NamingException NOT_SUPPORTED = new NamingException("Not supported at client side");	
	private Hashtable<? super Object,? super Object> params = null;

	public InitialContext(Hashtable<?,?> map) {
		this.params = new Hashtable<>(map);
	}

	public Object lookup(Name name) throws NamingException {
		return name==null ? null : lookup(name.toString());			
	}

	public Object lookup(String name) throws NamingException {
		if (name==null)
			return null;
		String nm = name.toLowerCase();
		if (nm.contains("connection") && nm.contains("factory"))
			return new TheConnectionFactory(params, nm.contains("queue"), nm.contains("topic"));
		else if (nm.contains("connection"))
			return new TheConnection(params);		
		return new TheDestination(name);		
	}
	
	public void bind(Name name, Object obj) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public void bind(String name, Object obj) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public void rebind(Name name, Object obj) throws NamingException {
		throw NOT_SUPPORTED;		
	}
	public void rebind(String name, Object obj) throws NamingException {
		throw NOT_SUPPORTED;		
	}
	public void unbind(Name name) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public void unbind(String name) throws NamingException {
		throw NOT_SUPPORTED;		
	}
	public void rename(Name oldName, Name newName) throws NamingException {
		throw NOT_SUPPORTED;		
	}
	public void rename(String oldName, String newName) throws NamingException {
		throw NOT_SUPPORTED;	
	}

	public NamingEnumeration<NameClassPair> list(final Name name) throws NamingException {
		return list(name!=null ? name.toString() : null);
	}

	public NamingEnumeration<NameClassPair> list(final String name) throws NamingException {
		final Object obj = lookup(name);
		final AtomicBoolean overIteration = new AtomicBoolean(obj==null);
		return new NamingEnumeration<NameClassPair>() {
			
			public NameClassPair nextElement() {
				if (!overIteration.get()) {
					overIteration.set(true);
					return new NameClassPair(name, obj.getClass().getName());				
				}
				return null;
			}			
			public boolean hasMoreElements() {
				return !overIteration.get();
			}			
			public NameClassPair next() throws NamingException {
				return nextElement();
			}			
			public boolean hasMore() throws NamingException {
				return !overIteration.get();
			}			
			public void close() throws NamingException {}
		};
	}

	public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public void destroySubcontext(Name name) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public void destroySubcontext(String name) throws NamingException {
		throw NOT_SUPPORTED;	
	}
	public Context createSubcontext(Name name) throws NamingException {
		throw NOT_SUPPORTED;
	}
	public Context createSubcontext(String name) throws NamingException {
		throw NOT_SUPPORTED;
	}
	public Object lookupLink(Name name) throws NamingException {
		return lookup(name);
	}
	public Object lookupLink(String name) throws NamingException {
		return lookup(name);
	}

	public NameParser getNameParser(Name name) throws NamingException {
		throw NOT_SUPPORTED;
	}
	public NameParser getNameParser(String name) throws NamingException {
		throw NOT_SUPPORTED;
	}
	public Name composeName(Name name, Name prefix) throws NamingException {
		throw NOT_SUPPORTED;
	}
	public String composeName(String name, String prefix)
			throws NamingException {
		throw NOT_SUPPORTED;
	}

	public Object addToEnvironment(String propName, Object propVal)
			throws NamingException {
		return params.put(propName, propVal);
	}
	public Object removeFromEnvironment(String propName) throws NamingException {
		return params.remove(propName);
	}

	public Hashtable<?,?> getEnvironment() throws NamingException {
		return params;
	}

	public void close() throws NamingException {}

	public String getNameInNamespace() throws NamingException {
		return "";
	}
}
