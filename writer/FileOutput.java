/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.writer;

import java.io.IOException;
import java.io.PrintWriter;

import vps.mapreduce.util.Contract;

/**
 * Uses a file as output
 */
public final class FileOutput implements Output {

	// Attributes
	private final PrintWriter m_writer;

	// Constructors
	/**
	 * Creates an instance of FileOutput
	 * 
	 * @param p_file
	 *            the file to use
	 * @throws IOException
	 *             if the file could not be accessed
	 */
	public FileOutput(final String p_file) throws IOException {
		Contract.checkNotNull(p_file, "no file given");

		m_writer = new PrintWriter(p_file, "UTF8");
	}

	/**
	 * Writes a line
	 * 
	 * @param p_line
	 *            the line
	 */
	@Override
	public void writeLine(final String p_line) {
		Contract.checkNotNull(p_line, "no line given");

		m_writer.println(p_line);
	}

	/**
	 * Closes the output
	 */
	@Override
	public void close() {
		m_writer.close();
	}

}
