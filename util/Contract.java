/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.util;


/**
 * Methods for checking objects and conditions
 */
public final class Contract {

	// Constructors
	/**
	 * Creates an instance of Contract
	 */
	private Contract() {}

	// Methods
	/**
	 * Checks if the condition is true and throws an ContractException otherwise
	 * 
	 * @param p_condition
	 *            the condition
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 *             if the condition is false
	 */
	public static void check(final boolean p_condition, final String p_message) {
		if (!p_condition) {
			throw new ContractException(p_message);
		}
	}

	/**
	 * Checks if the object is not null and throws an ContractException otherwise
	 * 
	 * @param p_object
	 *            the object
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 *             if the condition is false
	 */
	public static void checkNotNull(final Object p_object, final String p_message) {
		if (p_object == null) {
			throw new ContractException(p_message);
		}
	}

	/**
	 * Checks if the array is not empty and throws an ContractException otherwise
	 * 
	 * @param p_collection
	 *            the collection
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 *             if the condition is false
	 */
	public static <Type> void checkNotEmpty(final Type[] p_array, final String p_message) {
		if (p_array == null || p_array.length == 0) {
			throw new ContractException(p_message);
		}
	}

	// Classes
	/**
	 * Exception for breaking a contract
	 */
	public static final class ContractException extends RuntimeException {

		// Constants
		private static final long serialVersionUID = -3167467198371576288L;

		// Constructors
		/**
		 * Creates an instance of ContractException
		 */
		private ContractException() {
			super();
		}

		/**
		 * Creates an instance of ContractException
		 * 
		 * @param p_message
		 *            the message
		 */
		private ContractException(final String p_message) {
			super(p_message);
		}

	}

}
