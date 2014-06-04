/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import vps.mapreduce.Configuration;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;

/**
 * Reads a KeyValuePair from an underlying reader
 */
public class KeyValueReader implements Reader<KeyValuePair<String, String>> {

	// Attributes
	private final Reader<String> m_reader;
	private int lineNumber = 0;
	
	// Constructors
	/**
	 * Creates an instance of KeyValueReader
	 * 
	 * @param p_reader
	 *            the underlying reader to use
	 */
	public KeyValueReader(final Reader<String> p_reader) {
		// TODO: Aufgabe 1.1
		Contract.checkNotNull(p_reader, "no reader given");
		
		this.m_reader = p_reader;
	}

	// Methods
	/**
	 * Reads a KeyValuePair
	 * 
	 * @return the KeyValuePair
	 */
	@Override
	public KeyValuePair<String, String> read() {
		// TODO: Aufgabe 1.1
		KeyValuePair<String, String> ret = null;
		String key = "";
		String val = "";
		int sepPos = 0;
		
		String line = this.m_reader.read();
		this.lineNumber++;
		
		if (line != null) {
			if (line.contains(Configuration.KEY_VALUE_SEPARATOR)) {
				sepPos = line.indexOf(Configuration.KEY_VALUE_SEPARATOR);
				
				key = line.substring(0, sepPos);
				val = line.substring(sepPos + 1);
				
				ret = new KeyValuePair<String, String>(key, val);
			} else {
				ret = new KeyValuePair<String, String>("" + lineNumber, line);
			}
		} else {
			ret = null;
		}
		
		return ret;
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 1.1
		this.m_reader.close();
	}

}
