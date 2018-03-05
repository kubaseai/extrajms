package kuba.eai.jms.clients.common;

public class TheDestination implements javax.jms.Destination {
	
	protected String name = null;

	public TheDestination(String name) {
		this.setName(name);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String toString() {
		return name;
	}

}
