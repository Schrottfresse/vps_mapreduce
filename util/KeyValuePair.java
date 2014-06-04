/**
 * Verteilte und Parallele Programmierung SS 2014 Abschlussprojekt Bearbeiter:
 */
package vps.mapreduce.util;

/**
 * Represents a key value pair
 */
public final class KeyValuePair<KeyType extends Comparable<KeyType>, ValueType> implements
		Comparable<KeyValuePair<KeyType, ValueType>> {

	// Attributes
	private final KeyType m_key;
	private final ValueType m_value;

	// Constructors
	/**
	 * Creates an instance of KeyValuePair
	 * 
	 * @param p_key
	 *            the key
	 * @param p_value
	 *            the value
	 */
	public KeyValuePair(final KeyType p_key, final ValueType p_value) {
		Contract.checkNotNull(p_key, "no key given");

		m_key = p_key;
		m_value = p_value;
	}

	// Getters
	/**
	 * Gets the key
	 * 
	 * @return the key
	 */
	public KeyType getKey() {
		return m_key;
	}

	/**
	 * Gets the value
	 * 
	 * @return the value
	 */
	public ValueType getValue() {
		return m_value;
	}

	// Methods
	/**
	 * Compares this KeyValuePair with another
	 * 
	 * @param p_other
	 *            the other KeyValuePair
	 * @return the compare result
	 */
	@Override
	public int compareTo(final KeyValuePair<KeyType, ValueType> p_other) {
		int ret;

		Contract.checkNotNull(p_other, "no other given");

		if (m_key == p_other.m_key) {
			ret = 0;
		} else if (m_key == null) {
			ret = -1;
		} else if (p_other.m_key == null) {
			ret = 1;
		} else {
			ret = m_key.compareTo(p_other.m_key);
		}

		return ret;
	}

	/**
	 * Gets the string representation
	 * 
	 * @return the string representation
	 */
	@Override
	public String toString() {
		return KeyValuePair.class.getSimpleName() + " [m_key=" + m_key + ", m_value=" + m_value + "]";
	}

}
