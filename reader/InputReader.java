/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import vps.mapreduce.util.Contract;

/**
 * Reads a string from an input
 */
public class InputReader implements Reader<String> {

	// Attributes
	private final Input m_input;

	// Constructors
	/**
	 * Creates an instance of InputReader
	 * 
	 * @param p_input
	 *            the input to use
	 */
	public InputReader(final Input p_input) {
		Contract.checkNotNull(p_input, "no input given");

		m_input = p_input;
	}

	// Methods
	/**
	 * Reads a String
	 * 
	 * @return the String
	 */
	@Override
	public String read() {
		return m_input.readLine();
	}

	/**
	 * Closes the reader
	 */
	@Override
	public final void close() {
		m_input.close();
	}

}
