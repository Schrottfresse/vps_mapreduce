/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.core;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.Writer;

/**
 * Manages KeyValuePairs in the map phase
 */
public final class MapContext<KeyType extends Comparable<KeyType>, ValueType> implements Context<KeyType, ValueType> {

	// Attributes
	private final Map<KeyType, Collection<KeyValuePair<KeyType, ValueType>>> m_output;

	// Constructors
	/**
	 * Creates an instance of MapContext
	 */
	public MapContext() {
		m_output = new TreeMap<>();
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
		Collection<KeyValuePair<KeyType, ValueType>> pairs;

		Contract.checkNotNull(p_pair, "no pair given");

		pairs = m_output.get(p_pair.getKey());
		if (pairs == null) {
			pairs = new LinkedList<>();
			m_output.put(p_pair.getKey(), pairs);
		}

		pairs.add(p_pair);
	}

	/**
	 * Flushs the content to the given writer
	 * 
	 * @param p_writer
	 *            the writer
	 */
	@Override
	public void flush(final Writer<KeyValuePair<KeyType, ValueType>> p_writer) {
		Collection<KeyValuePair<KeyType, ValueType>> pairs;

		for (final KeyType key : m_output.keySet()) {
			pairs = m_output.get(key);

			for (final KeyValuePair<KeyType, ValueType> pair : pairs) {
				p_writer.write(pair);
			}
		}
	}

}
