/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

/**
 * Represents a reader
 * 
 * @param <Type>
 *            the type to read
 */
public interface Reader<Type> {

	// Methods
	/**
	 * Reads an element
	 * 
	 * @return the element
	 */
	Type read();

	/**
	 * Closes the reader
	 */
	void close();

}
