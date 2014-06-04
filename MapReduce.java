/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce;

import vps.mapreduce.core.Job;

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
	public static void main(final String[] p_arguments) {
		// TODO: Aufgabe 4

		Job job = null;
		
		try {
			job = (Job) Class.forName(Configuration.JOB_PACKAGE + p_arguments[0])
					.getConstructor((Class<?>[]) null)
					.newInstance((Object[]) null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			job = null;
		}
		
		if (job != null) {
			System.out.println(job.getClass());
		} else {
			System.out.println("Uups!");
		}
	}

}
