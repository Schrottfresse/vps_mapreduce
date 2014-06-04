/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.core;

import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.Executable;
import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.Writer;

/**
 * Represents a mapper
 */
public abstract class Mapper<KeyType extends Comparable<KeyType>, ValueType> implements Executable {

	// Attributes
	private final int m_id;

	private final MapContext<KeyType, ValueType> m_context;
	private final Reader<KeyValuePair<KeyType, ValueType>> m_reader;
	private final Writer<KeyValuePair<KeyType, ValueType>> m_writer;

	// Constructors
	/**
	 * Creates an instance of Mapper
	 * 
	 * @param p_id
	 *            the id of the mapper
	 * @param p_reader
	 *            the underlying reader to use
	 * @param p_writer
	 *            the underlying writer to use
	 */
	public Mapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer) {
		Contract.checkNotNull(p_reader, "no reader given");
		Contract.checkNotNull(p_writer, "no writer given");

		m_id = p_id;

		m_context = new MapContext<>();
		m_reader = getReader(p_reader);
		m_writer = getWriter(p_writer);
	}

	// Methods
	/**
	 * Perform the map operation
	 * 
	 * @param p_pair
	 *            the KeyValuePair
	 * @param p_context
	 *            the context
	 */
	protected abstract void map(final KeyValuePair<KeyType, ValueType> p_pair,
			final MapContext<KeyType, ValueType> p_context);

	/**
	 * Gets a reader for KeyValuePairs
	 * 
	 * @param p_reader
	 *            the underlying reader
	 * @return a reader for KeyValuePairs
	 */
	protected abstract Reader<KeyValuePair<KeyType, ValueType>> getReader(final Reader<String> p_reader);

	/**
	 * Gets a writer for KeyValuePairs
	 * 
	 * @param p_writer
	 *            the underlying writer
	 * @return a writer for KeyValuePairs
	 */
	protected abstract Writer<KeyValuePair<KeyType, ValueType>> getWriter(final Writer<String> p_writer);

	/**
	 * Executes the class
	 */
	@Override
	public void execute() {
		final long start;
		final long end;

		System.out.println("Mapper(" + m_id + ") started");
		start = System.currentTimeMillis();

		// TODO: Aufgabe 3.1

		end = System.currentTimeMillis();
		System.out.println("Mapper(" + m_id + ") finished in " + (end - start) + " ms");
	}

}
