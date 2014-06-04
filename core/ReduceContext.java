/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.core;

import java.util.Collection;
import java.util.LinkedList;

import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.Writer;

/**
 * Manages KeyValuePairs in the reduce phase
 */
public final class ReduceContext<KeyType extends Comparable<KeyType>, ValueType> implements Context<KeyType, ValueType> {

	// Attributes
	private final Collection<KeyValuePair<KeyType, ValueType>> m_output;

	// Constructors
	/**
	 * Creates an instance of ReduceContext
	 */
	public ReduceContext() {
		m_output = new LinkedList<>();
	}

	// Methods
	/**
	 * Stores a KeyValuePair in the context
	 * 
	 * @param p_pair
	 *            the KeyValuePair to store
	 */
	@Override
	public void store(final KeyValuePair<KeyType, ValueType> p_pair) {
		Contract.checkNotNull(p_pair, "no pair given");

		m_output.add(p_pair);
	}

	/**
	 * Flushs the content to the given writer
	 * 
	 * @param p_writer
	 *            the writer
	 */
	@Override
	public void flush(final Writer<KeyValuePair<KeyType, ValueType>> p_writer) {
		for (final KeyValuePair<KeyType, ValueType> pair : m_output) {
			p_writer.write(pair);
		}
	}

}
