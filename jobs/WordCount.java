/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.jobs;

import java.util.Iterator;

import vps.mapreduce.core.Job;
import vps.mapreduce.core.MapContext;
import vps.mapreduce.core.Mapper;
import vps.mapreduce.core.ReduceContext;
import vps.mapreduce.core.Reducer;
import vps.mapreduce.reader.KeyValueReader;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.KeyValuePair;
import vps.mapreduce.writer.KeyValueWriter;
import vps.mapreduce.writer.Writer;

/**
 * Performs the word count
 */
public class WordCount implements Job {

	// Constructors
	/**
	 * Creates an instance of WordCount
	 */
	public WordCount() {}

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
		return new WCMapper(p_id, p_reader, p_writer);
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
		return new WCReducer(p_id, p_reader, p_writer);
	}

	// Classes
	/**
	 * The mapper for the word count
	 */
	private class WCMapper extends Mapper<String, String> {

		// Constructors
		/**
		 * Creates an instance of WCMapper
		 * 
		 * @param p_id
		 *            the id of the mapper
		 * @param p_reader
		 *            the underlying reader to use
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public WCMapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer) {
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
		protected void map(final KeyValuePair<String, String> p_pair, final MapContext<String, String> p_context) {
			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");

			p_context.store(new KeyValuePair<>(p_pair.getValue(), Integer.toString(1)));
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<String, String>> getReader(final Reader<String> p_reader) {
			return new KeyValueReader(p_reader);
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<String, String>> getWriter(final Writer<String> p_writer) {
			return new KeyValueWriter(p_writer);
		}

	}

	/**
	 * The reducer for the word count
	 */
	private class WCReducer extends Reducer<String, String> {

		// Constructors
		/**
		 * Creates an instance of WCReducer
		 * 
		 * @param p_id
		 *            the id of the mapper
		 * @param p_reader
		 *            the underlying reader to use
		 * @param p_writer
		 *            the underlying writer to use
		 */
		public WCReducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer) {
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
		protected void reduce(final KeyValuePair<String, Iterable<String>> p_pair,
				final ReduceContext<String, String> p_context) {
			int count;
			final Iterator<String> iterator;

			Contract.checkNotNull(p_pair, "no pair given");
			Contract.checkNotNull(p_context, "no context given");

			count = 0;
			iterator = p_pair.getValue().iterator();
			while (iterator.hasNext()) {
				iterator.next();

				count++;
			}

			p_context.store(new KeyValuePair<String, String>(p_pair.getKey(), Integer.toString(count)));
		}

		/**
		 * Gets a reader for KeyValuePairs
		 * 
		 * @param p_reader
		 *            the underlying reader
		 * @return a reader for KeyValuePairs
		 */
		@Override
		protected Reader<KeyValuePair<String, String>> getReader(final Reader<String> p_reader) {
			return new KeyValueReader(p_reader);
		}

		/**
		 * Gets a writer for KeyValuePairs
		 * 
		 * @param p_writer
		 *            the underlying writer
		 * @return a writer for KeyValuePairs
		 */
		@Override
		protected Writer<KeyValuePair<String, String>> getWriter(final Writer<String> p_writer) {
			return new KeyValueWriter(p_writer);
		}

	}

}
