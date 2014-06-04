/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

import vps.mapreduce.Configuration;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;

/**
 * Writes a KeyValuePair to an underlying writer
 */
public class KeyValueWriter implements Writer<KeyValuePair<String, String>> {

	// Attributes
	final private Writer<String> m_writer;
	
	// Constructors
	/**
	 * Creates an instance of KeyValueWriter
	 * 
	 * @param p_writer
	 *            the underlying writer to use
	 */
	public KeyValueWriter(final Writer<String> p_writer) {
		// TODO: Aufgabe 2.1
		Contract.checkNotNull(p_writer, "writer is null");
		
		this.m_writer = p_writer;
	}

	// Methods
	/**
	 * Writes a KeyValuePair
	 * 
	 * @param p_element
	 *            the KeyValuePair
	 */
	@Override
	public void write(final KeyValuePair<String, String> p_element) {
		// TODO: Aufgabe 2.1
		Contract.checkNotNull(p_element, "element is null");
		
		this.m_writer.write(p_element.getKey() + Configuration.KEY_VALUE_SEPARATOR + p_element.getValue());
	}

	/**
	 * Closes the writer
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 2.1
		this.m_writer.close();
	}

}
