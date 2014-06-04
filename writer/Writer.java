/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

/**
 * Represents a writer
 * 
 * @param <Type>
 *            the type to write
 */
public interface Writer<Type> {

	// Methods
	/**
	 * Writes an element
	 * 
	 * @param p_element
	 *            the element
	 */
	void write(final Type p_element);

	/**
	 * Closes the writer
	 */
	void close();

}
