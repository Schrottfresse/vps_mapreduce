/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

/**
 * Represents an output
 */
public interface Output {

	// Methods
	/**
	 * Writes a line
	 * 
	 * @param p_line
	 *            the line
	 */
	void writeLine(final String p_line);

	/**
	 * Closes the output
	 */
	void close();

}
