/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.core;

import java.lang.reflect.Array;

import vps.mapreduce.reader.IterableValueReader;
import vps.mapreduce.reader.MergingReader;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.Executable;
import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.Writer;

/**
 * Reprensent a reducer
 */
public abstract class Reducer<KeyType extends Comparable<KeyType>, ValueType> implements Executable {

	// Attributes
	private final int m_id;

	private final ReduceContext<KeyType, ValueType> m_context;
	private final Reader<KeyValuePair<KeyType, Iterable<ValueType>>> m_reader;
	private final Writer<KeyValuePair<KeyType, ValueType>> m_writer;

	// Constructors
	/**
	 * Creates an instance of Reducer
	 * 
	 * @param p_id
	 *            the id of the mapper
	 * @param p_reader
	 *            the underlying reader to use
	 * @param p_writer
	 *            the underlying writer to use
	 */
	public Reducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer) {
		Contract.checkNotNull(p_reader, "no reader given");
		Contract.checkNotNull(p_writer, "no writer given");

		m_id = p_id;

		m_context = new ReduceContext<>();
		m_reader = new IterableValueReader<>(new MergingReader<>(getReader(p_reader)));
		m_writer = getWriter(p_writer);
	}

	// Methods
	/**
	 * Performs the reduce operation
	 * 
	 * @param p_pair
	 *            the KeyValuePair
	 * @param p_context
	 *            the context
	 */
	protected abstract void reduce(final KeyValuePair<KeyType, Iterable<ValueType>> p_pair,
			final ReduceContext<KeyType, ValueType> p_context);

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
	 * Gets readers for KeyValuePairs
	 * 
	 * @param p_reader
	 *            the underlying reader
	 * @return readers for KeyValuePairs
	 */
	@SuppressWarnings("unchecked")
	private Reader<KeyValuePair<KeyType, ValueType>>[] getReader(final Reader<String>[] p_reader) {
		final Reader<KeyValuePair<KeyType, ValueType>>[] ret;

		Contract.checkNotEmpty(p_reader, "no reader given");

		ret = (Reader<KeyValuePair<KeyType, ValueType>>[])Array.newInstance(Reader.class, p_reader.length);
		for (int i = 0;i < p_reader.length;i++) {
			ret[i] = getReader(p_reader[i]);
		}

		return ret;
	}

	/**
	 * Executes the class
	 */
	@Override
	public void execute() {
		final long start;
		final long end;

		System.out.println("Reducer(" + m_id + ") started");
		start = System.currentTimeMillis();

		// TODO: Aufgabe 3.2

		end = System.currentTimeMillis();
		System.out.println("Reducer(" + m_id + ") finished in " + (end - start) + " ms");
	}

}
