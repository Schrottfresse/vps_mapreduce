/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import vps.mapreduce.util.Contract;

/**
 * Uses a file as input
 */
public final class FileInput implements Input {

	// Attributes
	private final BufferedReader m_reader;
	private final boolean m_full;
	private long m_remaining;

	// Constructors
	/**
	 * Creates an instance of FileInput
	 * 
	 * @param p_file
	 *            the file to use
	 * @throws IOException
	 *             if the file could not be accessed
	 */
	public FileInput(final String p_file) throws IOException {
		this(p_file, 0, 0);
	}

	/**
	 * Creates an instance of FileInput
	 * 
	 * @param p_file
	 *            the file to use
	 * @param p_offset
	 *            the line offset
	 * @param p_count
	 *            the number of lines
	 * @throws IOException
	 *             if the file could not be accessed
	 */
	public FileInput(final String p_file, final long p_offset, final long p_count) throws IOException {
		final InputStreamReader reader;

		Contract.checkNotNull(p_file, "no file given");
		Contract.check(p_offset >= 0, "invalid offset given: " + p_offset);
		Contract.check(p_count >= 0, "invalid length given: " + p_count);

		m_full = p_offset == 0 && p_count == 0;
		m_remaining = p_count;

		reader = new InputStreamReader(new FileInputStream(p_file), "UTF8");

		m_reader = new BufferedReader(reader);

		for (int i = 0;i < p_offset;i++) {
			m_reader.readLine();
		}
	}

	/**
	 * Reads a line
	 * 
	 * @return the line
	 */
	@Override
	public String readLine() {
		String ret = null;

		try {
			if (m_full || m_remaining > 0) {
				ret = m_reader.readLine();
				if (ret != null && !m_full) {
					m_remaining--;
				}
			}
		} catch (final IOException e) {}

		return ret;
	}

	/**
	 * Closes the input
	 */
	@Override
	public void close() {
		try {
			m_reader.close();
		} catch (final IOException e) {}
	}

}
