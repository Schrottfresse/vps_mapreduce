/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;


/**
 * Writes elements to different underlying writers
 */
public class SplittingWriter<ValueType> implements Writer<ValueType> {

	// Constructors
	/**
	 * Creates an instance of SplittingWriter
	 * 
	 * @param p_writer
	 *            the writer to use
	 */
	public SplittingWriter(final Writer<ValueType>[] p_writer) {
		// TODO: Aufgabe 2.2
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
	}

	/**
	 * Closes the writer
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 2.2
	}

}
