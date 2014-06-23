/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

import vps.mapreduce.Configuration;
import vps.mapreduce.util.Contract;


/**
 * Writes elements to different underlying writers
 */
public class SplittingWriter<ValueType> implements Writer<ValueType> {

	// Attributes
	final private Writer<ValueType>[] m_writer;
	
	// Constructors
	/**
	 * Creates an instance of SplittingWriter
	 * 
	 * @param p_writer
	 *            the writer to use
	 */
	public SplittingWriter(final Writer<ValueType>[] p_writer) {
		// TODO: Aufgabe 2.2
		
		Contract.checkNotNull(p_writer, "writer array is null");
		Contract.checkNotEmpty(p_writer, "writer array is empty");
		
		this.m_writer = p_writer;
	}

	// Methods
	/**
	 * Writes an element
	 * 
	 * @param p_element
	 *            the element
	 */
	@Override
	public void write(final ValueType p_element) {
		// TODO: Aufgabe 2.2
		// Berechne den Hash des Strings
		int hash = ((String) p_element).split(Configuration.KEY_VALUE_SEPARATOR)[0].hashCode();
		
		// Schreibe das Element mit Writer (hash modulo Anzahl Writer) 
		this.m_writer[Math.abs(hash % this.m_writer.length)].write(p_element);
	}

	/**
	 * Closes the writer
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 2.2
		
		for (int i = 0; i < this.m_writer.length; i++) {
			this.m_writer[i].close();
		}
	}

}
