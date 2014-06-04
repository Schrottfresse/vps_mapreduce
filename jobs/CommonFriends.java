/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.jobs;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import vps.mapreduce.core.Job;
import vps.mapreduce.core.Mapper;
import vps.mapreduce.core.Reducer;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.writer.Writer;

/**
 * Finds common friends
 */
public class CommonFriends implements Job {

	// Constructors
	/**
	 * Creates an instance of CommonFriends
	 */
	public CommonFriends() {}

	// Methods
	/**
	 * Creates a mapper
	 * 
	 * @param p_id
	 *            the id of the mapper
	 * @param p_reader
	 *            the underlying reader to use
	 * @param p_writer
	 *            the underlying writer to use
	 * @return a mapper
	 */
	@Override
	public Mapper<?, ?> createMapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer) {
		// TODO: Aufgabe 5.2
		return null;
	}

	/**
	 * Creates a reducer
	 * 
	 * @param p_id
	 *            the id of the mapper
	 * @param p_reader
	 *            the underlying reader to use
	 * @param p_writer
	 *            the underlying writer to use
	 * @return a reducer
	 */
	@Override
	public Reducer<?, ?> createReducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer) {
		// TODO: Aufgabe 5.3
		return null;
	}

	// Classes
	/**
	 * Represents a set of elements
	 */
	private static class CFSet implements Comparable<CFSet>, Iterable<String> {

		// Constants
		private static final String ELEMENT_SEPERATOR = ";";

		// Attributes
		private final Set<String> m_set;

		// Constructors
		/**
		 * Creates an instance of CFSet
		 */
		private CFSet() {
			m_set = new TreeSet<>();
		}

		/**
		 * Creates an instance of CFSet
		 * 
		 * @param p_set
		 *            the set to add
		 * @param p_element
		 *            the element to add
		 */
		private CFSet(final CFSet p_set, final String p_element) {
			m_set = new TreeSet<>(p_set.m_set);
			m_set.add(p_element);
		}

		// Methods
		/**
		 * Adds an element
		 * 
		 * @param p_element
		 *            the element to add
		 */
		public void add(final String p_element) {
			m_set.add(p_element);
		}

		/**
		 * Calculates the intersection of the given sets
		 * 
		 * @param p_sets
		 *            the sets
		 * @return the intersection
		 */
		public static CFSet intersect(final Iterable<CFSet> p_sets) {
			CFSet ret;
			Iterator<CFSet> iterator;

			ret = new CFSet();
			if (p_sets != null) {
				iterator = p_sets.iterator();

				if (iterator.hasNext()) {
					ret.m_set.addAll(iterator.next().m_set);

					while (iterator.hasNext()) {
						ret.m_set.retainAll(iterator.next().m_set);
					}
				}
			}

			return ret;
		}

		/**
		 * Parses a String
		 * 
		 * @param p_string
		 *            the String
		 * @return a CFSet
		 */
		public static CFSet parse(final String p_string) {
			CFSet ret;
			String[] elements;

			ret = new CFSet();

			elements = p_string.split(ELEMENT_SEPERATOR);
			for (final String element : elements) {
				ret.add(element);
			}

			return ret;
		}

		/**
		 * Parses a CFSet
		 * 
		 * @param p_set
		 *            the CFSet
		 * @return a string
		 */
		public static String parse(final CFSet p_set) {
			StringBuffer ret;

			ret = new StringBuffer();

			for (final String element : p_set) {
				ret.append(element + ELEMENT_SEPERATOR);
			}
			ret.deleteCharAt(ret.length() - 1);

			return ret.toString();
		}

		/**
		 * Gets the iterator
		 * 
		 * @return the iterator
		 */
		@Override
		public Iterator<String> iterator() {
			return m_set.iterator();
		}

		/**
		 * Compares this CFSet with another
		 * 
		 * @param p_other
		 *            the other CFSet
		 * @return the compare result
		 */
		@Override
		public int compareTo(final CFSet p_other) {
			// TODO: Aufgabe 5.2
			return 0;
		}

		/**
		 * Gets the string representation
		 * 
		 * @param the
		 *            string representation
		 */
		@Override
		public String toString() {
			return CFSet.class.getSimpleName() + " [m_set=" + Arrays.toString(m_set.toArray()) + "]";
		}

	}

}
