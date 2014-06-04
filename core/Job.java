/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.core;

import vps.mapreduce.reader.Reader;
import vps.mapreduce.writer.Writer;

/**
 * Represents a MapReduce job
 */
public interface Job {

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
	Mapper<?, ?> createMapper(final int p_id, final Reader<String> p_reader, final Writer<String> p_writer);

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
	Reducer<?, ?> createReducer(final int p_id, final Reader<String>[] p_reader, final Writer<String> p_writer);

}
