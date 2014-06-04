/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import java.util.LinkedList;

import vps.mapreduce.util.Contract;

/**
 * Reads from multiple readers
 */
public class MergingReader<Type extends Comparable<Type>> implements Reader<Type> {

	//Attributes
	final private LinkedList<BufferedReader> m_reader = new LinkedList<BufferedReader>();
	
	// Constructors
	/**
	 * Creates an instance of MergingReader
	 * 
	 * @param p_reader
	 *            the reader to use
	 * @param p_comparator
	 *            the comparator to use
	 */
	public MergingReader(final Reader<Type>[] p_reader) {
		// TODO: Aufgabe 1.3
		Contract.checkNotNull(p_reader, "no reader array given");
		
		for (int i = 0; i < p_reader.length; i++) {
			Contract.checkNotNull(p_reader[i], "given reader is null");
			this.m_reader.add(new BufferedReader(p_reader[i]));
		}
	}

	// Methods
	/**
	 * Reads an element
	 * 
	 * @return the element
	 */
	@Override
	public Type read() {
		// TODO: Aufgabe 1.3
		BufferedReader tmp = null;
		
		if (this.m_reader.size() == 1) {
			return this.m_reader.get(0).read();
		}
		
		tmp = this.m_reader.get(0);
		
		for (int i = 1; i < this.m_reader.size(); i++) {
			
			if (tmp.compareTo(this.m_reader.get(i)) > 0) {
				tmp = this.m_reader.get(i);
			}
		}
		
		return tmp.read();
	}

	/**
	 * Closes the reader
	 */
	@Override
	public void close() {
		// TODO: Aufgabe 1.3
		for (int i = 0; i < this.m_reader.size(); i++) {
			this.m_reader.get(i).close();
		}
	}

	// Classes
	/**
	 * Buffers the next element of a reader
	 */
	private class BufferedReader implements Reader<Type>, Comparable<BufferedReader> {

		// Attributes
		private final Reader<Type> m_reader;
		private Type m_next;

		// Constructors
		/**
		 * Creates an instance of BufferedReader
		 * 
		 * @param p_reader
		 *            the reader to use
		 */
		private BufferedReader(final Reader<Type> p_reader) {
			Contract.checkNotNull(p_reader, "no reader given");

			m_reader = p_reader;

			read();
		}

		// Methods
		/**
		 * Reads an element
		 * 
		 * @return the element
		 */
		@Override
		public Type read() {
			final Type ret;

			ret = m_next;
			m_next = m_reader.read();

			return ret;
		}

		/**
		 * Closes the reader
		 */
		@Override
		public void close() {
			m_reader.close();
		}

		/**
		 * Compares this BufferedReader to another
		 * 
		 * @param p_other
		 *            the other BufferedReader
		 * @return the compare result
		 */
		@Override
		public int compareTo(final BufferedReader p_other) {
			final int ret;

			Contract.checkNotNull(p_other, "no other given");
			
			if (m_next == p_other.m_next) {
				ret = 0;
			} else if (m_next == null) {
				ret = 1;
			} else if (p_other.m_next == null) {
				ret = -1;
			} else {
				ret = m_next.compareTo(p_other.m_next);
			}

			return ret;
		}

		/**
		 * Gets the string representation
		 * 
		 * @return the string representation
		 */
		@Override
		public String toString() {
			return BufferedReader.class.getSimpleName() + " [m_next=" + m_next + "]";
		}

	}

}
