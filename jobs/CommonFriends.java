/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.jobs;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import vps.mapreduce.Configuration;
import vps.mapreduce.core.Job;
import vps.mapreduce.core.MapContext;
import vps.mapreduce.core.Mapper;
import vps.mapreduce.core.ReduceContext;
import vps.mapreduce.core.Reducer;
import vps.mapreduce.reader.KeyValueReader;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;
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
		return new CFMapper(p_id, p_reader, p_writer);
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
		return new CFReducer(p_id, p_reader, p_writer);
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
		 * @param p_elements
		 *            the elements to add
		 */
		private CFSet(final String[] p_elements) {
			m_set = new TreeSet<>();
			for (String element : p_elements) {
				m_set.add(element);
			}
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
		 * Adds multiple elements
		 * 
		 * @param p_elements
		 *            the elements to add
		 */
		@SuppressWarnings("unused")
		public void add(final String[] p_elements) {
			for (String element : p_elements) {
				m_set.add(element);
			}
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
		@SuppressWarnings("unused")
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
			final int ret;

			Contract.checkNotNull(p_other, "no other given");
			
			if (m_set == p_other.m_set) {
				ret = 0;
			} else if (m_set == null) {
				ret = 1;
			} else if (p_other.m_set == null) {
				ret = -1;
			} else {
				ret = m_set.toString().compareTo(p_other.m_set.toString());
			}

			return ret;
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
	
	/**
	 * The mapper for the common friends
	 */
	private class CFMapper extends Mapper<CFSet, CFSet> {

		// Constructors
		/**
		 * Creates an instance of CFMapper
		 * 
		 * @param p_id
		 *            the id of the mapper
		 * @param p_reader
		 *            the underlying reader to use
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public CFMapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer) {
			super(p_id, p_reader, p_writer);
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
		@Override
		protected void map(final KeyValuePair<CFSet, CFSet> p_pair, final MapContext<CFSet, CFSet> p_context) {
			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");
			
			Iterator<String> iter = p_pair.getValue().iterator();

			while (iter.hasNext()) {
				p_context.store(new KeyValuePair<>(new CFSet(p_pair.getKey(), iter.next()), p_pair.getValue()));
			}
			
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<CFSet, CFSet>> getReader(final Reader<String> p_reader) {
			return new CFSetReader(new KeyValueReader(p_reader));
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<CFSet, CFSet>> getWriter(final Writer<String> p_writer) {
			return new CFSetWriter(p_writer);
		}

	}

	/**
	 * The reducer for the common friends
	 */
	private class CFReducer extends Reducer<CFSet, CFSet> {

		// Constructors
		/**
		 * Creates an instance of CFReducer
		 * 
		 * @param p_id
		 *            the id of the reducer
		 * @param p_reader
		 *            the underlying reader to use
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public CFReducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer) {
			super(p_id, p_reader, p_writer);
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
		@Override
		protected void reduce(final KeyValuePair<CFSet, Iterable<CFSet>> p_pair,
				final ReduceContext<CFSet, CFSet> p_context) {
			final CFSet set;

			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");
			set = CFSet.intersect(p_pair.getValue());

			p_context.store(new KeyValuePair<CFSet, CFSet>(p_pair.getKey(), set));
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<CFSet, CFSet>> getReader(final Reader<String> p_reader) {
			return new CFSetReader(new KeyValueReader(p_reader));
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<CFSet, CFSet>> getWriter(final Writer<String> p_writer) {
			return new CFSetWriter(p_writer);
		}

	}
	
	private class CFSetReader implements Reader<KeyValuePair<CFSet, CFSet>> {

		private final Reader<KeyValuePair<String, String>> m_reader;

		public CFSetReader(final Reader<KeyValuePair<String, String>> p_reader) {
			Contract.checkNotNull(p_reader, "no reader given");
			
			this.m_reader = p_reader;
		}
		
		@Override
		public KeyValuePair<CFSet, CFSet> read() {
			// TODO Auto-generated method stub
			
			final KeyValuePair<String, String> readed = this.m_reader.read();
			
			if (readed == null) {
				return null;
			}
			
			String[] keys = readed.getKey().split(CFSet.ELEMENT_SEPERATOR);
			String[] values = readed.getValue().split(CFSet.ELEMENT_SEPERATOR);
			CFSet key_set = new CFSet(keys);
			CFSet value_set = new CFSet(values);
			
			return new KeyValuePair<CFSet, CFSet>(key_set, value_set);
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			this.m_reader.close();
		}
		
	}
	
	private class CFSetWriter implements Writer<KeyValuePair<CFSet, CFSet>> {

		private final Writer<String> m_writer;
		
		public CFSetWriter(final Writer<String> p_writer) {
			Contract.checkNotNull(p_writer, "writer is null");
			
			this.m_writer = p_writer;
		}
		
		@Override
		public void write(KeyValuePair<CFSet, CFSet> p_element) {
			// TODO Auto-generated method stub
			this.m_writer.write(CFSet.parse(p_element.getKey()) +
				Configuration.KEY_VALUE_SEPARATOR +
				CFSet.parse(p_element.getValue())
			);
		}
		
		@Override
		public void close() {
			// TODO Auto-generated method stub
			this.m_writer.close();
		}
		
	}

}
