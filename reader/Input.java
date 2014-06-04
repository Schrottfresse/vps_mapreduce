/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

/**
 * Represents an input
 */
public interface Input {

	// Methods
	/**
	 * Reads a line
	 * 
	 * @return the line
	 */
	String readLine();

	/**
	 * Closes the input
	 */
	void close();

}
