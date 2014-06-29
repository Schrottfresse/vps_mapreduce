/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce;

import java.lang.reflect.Array;

import vps.mapreduce.core.Job;
import vps.mapreduce.core.Mapper;
import vps.mapreduce.core.Reducer;
import vps.mapreduce.reader.Reader;
import vps.mapreduce.util.Contract;
import vps.mapreduce.util.Executor;
import vps.mapreduce.writer.Writer;

/**
 * Start class for MapReduce
 */
public class MapReduce {

	// Constructors
	/**
	 * Creates an instance of MapReduce
	 */
	private MapReduce() {
		
	}

	// Methods
	/**
	 * Programm entry point for MapReduce
	 * 
	 * @param p_arguments
	 *            the programm arguments
	 */
	@SuppressWarnings("unchecked")
	public static void main(final String[] p_arguments) {
		// TODO: Aufgabe 4
		// Check arguments
		Contract.checkNotNull(p_arguments, "Args is null");
		Contract.check(p_arguments.length == 4, "Wrong number of arguments");
		Contract.checkNotEmpty(p_arguments, "Args is empty");
		
		Job job = null;
		Mapper<String, String>[] mapper = (Mapper<String, String>[])Array.newInstance(Mapper.class, Configuration.MAPPER_COUNT);
		Reducer<String, String>[] reducer = (Reducer<String, String>[])Array.newInstance(Reducer.class, Configuration.REDUCER_COUNT);
		int threads = Configuration.THREAD_COUNT;
		int mapper_count = Configuration.MAPPER_COUNT;
		int line_count = 0;
		
		try {
			line_count = Configuration.getLineCount(p_arguments[1]);
			Contract.check(line_count <= 0, "");
		} catch (Exception e) {
			System.out.println("File broken or empty");
			System.exit(1);
		}
		
		// Use less mappers, because there are less lines
		if (line_count < mapper_count) {
			mapper_count = line_count;
		}
		
		// Dynamically create a job
		try {
			job = (Job) Class.forName(Configuration.JOB_PACKAGE + p_arguments[0])
					.getConstructor((Class<?>[]) null)
					.newInstance((Object[]) null);
			Contract.check(job == null, "");
		} catch (Exception e) {
			System.out.println("Wrong job name");
			System.exit(1);
		}
		
		for (int i = 0; i < mapper.length; i++) {
			Reader<String> tmpReader = null;
			int linesToRead = line_count/mapper_count;
			
			// If there are some lines left by the division, let the last reader read them
			if (line_count % mapper_count != 0) {
				if (i == mapper.length - 1) {
					linesToRead += line_count % mapper_count;
				}
			}
			
			try {
				tmpReader = Configuration.createMapperReader(p_arguments[1],
						i*(line_count/mapper_count),
						linesToRead);
				Contract.check(tmpReader == null, "");
			} catch (Exception e) {
				System.out.println("Cannot create mapper reader");
				System.exit(1);
			}
			
			Writer<String> tmpWriter = null;
			try {
				tmpWriter = Configuration.createMapperWriter(p_arguments[2], i);
				Contract.check(tmpWriter == null, "");
			} catch (Exception e) {
				System.out.println("Cannot create mapper writer");
				System.exit(1);
			}
			
			mapper[i] = (Mapper<String, String>) job.createMapper(i, tmpReader, tmpWriter);
		}
		Executor.execute(mapper, threads);
		
		for (int i = 0; i < reducer.length; i++) {
			Reader<String>[] tmpReader = null;
			try {
				tmpReader = Configuration.createReducerReader(p_arguments[2], i);
				Contract.check(tmpReader == null, "");
			} catch (Exception e) {
				System.out.println("Cannot create reducer reader");
				System.exit(1);
			}
			
			Writer<String> tmpWriter = null;
			try {
				tmpWriter = Configuration.createReducerWriter(p_arguments[3], i);
				Contract.check(tmpWriter == null, "");
			} catch (Exception e) {
				System.out.println("Cannot create reducer writer");
				System.exit(1);
			}
			
			reducer[i] = (Reducer<String, String>) job.createReducer(i, tmpReader, tmpWriter);
		}
		Executor.execute(reducer, threads);
	}

}
