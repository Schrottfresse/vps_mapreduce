/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

import vps.mapreduce.util.KeyValuePair;

/**
 * Writes a KeyValuePair to an underlying writer
 */
public class KeyValueWriter implements Writer<KeyValuePair<String, String>> {

	// Constructors
	/**
	 * Creates an instance of KeyValueWriter
	 * 
	 * @param p_writer
	 *            the underlying writer to use
	 */
	public KeyValueWriter(final Writer<String> p_writer) {
		// TODO: Aufgabe 2.1
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
	}

	/**
	 * Closes the writer
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 2.1
	}

}
