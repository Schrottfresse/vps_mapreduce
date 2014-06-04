/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import java.util.Iterator;
import java.util.LinkedList;

import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;

/**
 * Reads a KeyValuePair from an underlying reader
 */
public class IterableValueReader<KeyType extends Comparable<KeyType>, ValueType> implements
		Reader<KeyValuePair<KeyType, Iterable<ValueType>>>, Iterable<ValueType>, Iterator<ValueType> {

	// Attributes
	private final Reader<KeyValuePair<KeyType, ValueType>> m_reader;
	private Iterator<ValueType> m_val_iter;
	private KeyValuePair<KeyType, ValueType> lastRead = null;
	
	// Constructors
	/**
	 * Creates an instance of IterableValueReader
	 * 
	 * @param p_reader
	 *            the underlying reader to use
	 */
	public IterableValueReader(final Reader<KeyValuePair<KeyType, ValueType>> p_reader) {
		// TODO: Aufgabe 1.2
		Contract.checkNotNull(p_reader, "no reader given");
		
		this.m_reader = p_reader;
		this.lastRead = this.m_reader.read();
	}

	// Methods
	/**
	 * Reads a KeyValuePair
	 * 
	 * @return the KeyValuePair
	 */
	@Override
	public KeyValuePair<KeyType, Iterable<ValueType>> read() {
		// TODO: Aufgabe 1.2
		
		LinkedList<ValueType> valList = new LinkedList<ValueType>();
		
		if (this.lastRead == null) {
			return null;
		}
		
		KeyType key = this.lastRead.getKey();
		
		System.out.println(key);
		
		while (this.lastRead.getKey().compareTo(key) == 0) {
			valList.add(this.lastRead.getValue());
			this.lastRead = this.m_reader.read();
			
			System.out.println(this.lastRead);
			
			if (this.lastRead == null) {
				break;
			}
		}
		
		this.m_val_iter = valList.iterator();
		
		System.out.println(valList);
		
		return new KeyValuePair<KeyType,Iterable<ValueType>>(key, valList);
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 1.2
		this.m_reader.close();
	}

	/**
	 * Checks if a next element exist
	 * 
	 * @return true if a next element exist, false otherwise
	 */
	@Override
	public boolean hasNext() {
		// TODO: Aufgabe 1.2
		return this.m_val_iter.hasNext();
	}

	/**
	 * Gets the next element
	 * 
	 * @return the next element
	 */
	@Override
	public ValueType next() {
		// TODO: Aufgabe 1.2
		return this.m_val_iter.next();
	}

	/**
	 * Removes the current element (not supported)
	 */
	@Override
	public void remove() {}

	/**
	 * Return an iterator for the elements
	 * 
	 * @return an iterator
	 */
	@Override
	public Iterator<ValueType> iterator() {
		if (this.m_val_iter == null) {
			this.m_val_iter = this.read().getValue().iterator();
		}
		
		return this.m_val_iter;
	}

}
