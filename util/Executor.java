/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Executes an executable class
 */
public class Executor implements Runnable {

	// Attributes
	private final Executable m_executable;

	// Constructors
	/**
	 * Creates an instance of Executor
	 * 
	 * @param p_executable
	 *            the executable class
	 */
	public Executor(final Executable p_executable) {
		Contract.checkNotNull(p_executable, "no executable given");

		m_executable = p_executable;
	}

	// Methods
	/**
	 * Executes the executable class
	 */
	@Override
	public void run() {
		m_executable.execute();
	}

	/**
	 * Executes executable classes with a number of threads
	 * 
	 * @param p_executables
	 *            the executable classes
	 * @param p_threadCount
	 *            the number of threads
	 */
	public static void execute(final Executable[] p_executables, final int p_threadCount) {
		final ExecutorService executorService;
		final Future<?>[] futures;

		Contract.checkNotEmpty(p_executables, "no executables given");
		Contract.check(p_threadCount > 0, "invalid thread count given");

		executorService = Executors.newFixedThreadPool(p_threadCount);

		futures = new Future[p_executables.length];
		for (int i = 0;i < p_executables.length;i++) {
			futures[i] = executorService.submit(new Executor(p_executables[i]));
		}

		for (final Future<?> future : futures) {
			try {
				future.get();
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
		executorService.shutdown();
	}
}
