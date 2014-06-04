/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.core;

import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.Writer;

/**
 * Manages KeyValuePairs
 */
interface Context<KeyType extends Comparable<KeyType>, ValueType> {

	// Methods
	/**
	 * Stores a KeyValuePair
	 * 
	 * @param p_pair
	 *            the KeyValuePair to store
	 */
	void store(final KeyValuePair<KeyType, ValueType> p_pair);

	/**
	 * Flushs the content to the given writer
	 * 
	 * @param p_writer
	 *            the writer
	 */
	void flush(final Writer<KeyValuePair<KeyType, ValueType>> p_writer);

}
