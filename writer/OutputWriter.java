/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

import vps.mapreduce.util.Contract;

/**
 * Writes a string to an output
 */
public class OutputWriter implements Writer<String> {

	// Attributes
	private final Output m_output;

	// Constructors
	/**
	 * Creates an instance of OutputWriter
	 * 
	 * @param p_output
	 *            the output to use
	 */
	public OutputWriter(final Output p_output) {
		Contract.checkNotNull(p_output, "no output given");

		m_output = p_output;
	}

	// Methods
	/**
	 * Writes a String
	 * 
	 * @param p_element
	 *            the String
	 */
	@Override
	public final void write(final String p_element) {
		Contract.checkNotNull(p_element, "no element given");

		m_output.writeLine(p_element);
	}

	/**
	 * Closes the writer
	 */
	@Override
	public final void close() {
		m_output.close();
	}

}
