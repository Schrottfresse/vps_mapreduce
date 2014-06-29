package vps.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;

import vps.mapreduce.reader.FileInput;
import vps.mapreduce.reader.InputReader;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.writer.FileOutput;
import vps.mapreduce.writer.OutputWriter;
import vps.mapreduce.writer.SplittingWriter;
import vps.mapreduce.writer.Writer;

/**
 * Represents the configuration
 */
public abstract class Configuration {

	// Constants
	public static final int MAPPER_COUNT = 3;
	public static final int REDUCER_COUNT = 2;
	public static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();

	public static final String JOB_PACKAGE = "vps.mapreduce.jobs.";

	private static final String INTERMEDIATE_NAME_FORMAT = "%s_m%03d_r%03d";
	private static final String OUTPUT_NAME_FORMAT = "%s.part%03d";

	public static final String KEY_VALUE_SEPARATOR = "\t";

	// Constructors
	/**
	 * Creates an instance of Configuration
	 */
	private Configuration() {}

	// Methods
	/**
	 * Creates the default reader for a mapper
	 * 
	 * @param p_path
	 *            the input file path
	 * @param p_offset
	 *            the data offset
	 * @param p_length
	 *            the data length
	 * @throws IOException
	 *             if the file could not be accessed
	 */
	public static Reader<String> createMapperReader(final String p_path, final long p_offset, final long p_length)
			throws IOException {
		return new InputReader(new FileInput(p_path, p_offset, p_length));
	}

	/**
	 * Creates the default writer for a mapper
	 * 
	 * @param p_path
	 *            the intermediate path
	 * @param p_id
	 *            the mapper id
	 * @throws IOException
	 *             if the path could not be accessed
	 */
	@SuppressWarnings("unchecked")
	public static Writer<String> createMapperWriter(final String p_path, final int p_id) throws IOException {
		final Writer<String> ret;
		final Writer<String>[] writer;

		writer = (Writer<String>[])Array.newInstance(Writer.class, REDUCER_COUNT);
		for (int i = 0;i < REDUCER_COUNT;i++) {
			writer[i] = new OutputWriter(new FileOutput(String.format(INTERMEDIATE_NAME_FORMAT, p_path, p_id, i)));
		}

		ret = new SplittingWriter<String>(writer);

		return ret;
	}

	/**
	 * Creates the default reader for a reducer
	 * 
	 * @param p_path
	 *            the intermediate path
	 * @param p_id
	 *            the reducer id
	 * @throws IOException
	 *             if the path could not be accessed
	 */
	@SuppressWarnings("unchecked")
	public static Reader<String>[] createReducerReader(final String p_path, final int p_id) throws IOException {
		final Reader<String>[] ret;

		ret = (Reader<String>[])Array.newInstance(Reader.class, MAPPER_COUNT);
		for (int i = 0;i < MAPPER_COUNT;i++) {
			ret[i] = new InputReader(new FileInput(String.format(INTERMEDIATE_NAME_FORMAT, p_path, i, p_id)));
		}
		
		return ret;
	}

	/**
	 * Creates the default writer for a reducer
	 * 
	 * @param p_path
	 *            the output path
	 * @param p_id
	 *            the reducer id
	 * @throws IOException
	 *             if the path could not be accessed
	 */
	public static Writer<String> createReducerWriter(final String p_path, final int p_id) throws IOException {
		return new OutputWriter(new FileOutput(String.format(Configuration.OUTPUT_NAME_FORMAT, p_path, p_id)));
	}
	
	/**
	 * Counts the lines in a file
	 * 
	 * @param p_filename
	 *            the name of the file
	 * @return the line count
	 * @throws IOException
	 *             if the file could not be accessed
	 */
	public static int getLineCount(final String p_filename) throws IOException {
		int ret = 0;
		BufferedReader reader;
		String line;

		reader = new BufferedReader(new FileReader(p_filename));

		ret = 0;
		line = reader.readLine();
		while (line != null) {
			ret++;

			line = reader.readLine();
		}
		
		reader.close();

		return ret;
	}

}
