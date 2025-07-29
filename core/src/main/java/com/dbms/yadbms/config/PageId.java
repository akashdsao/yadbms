package com.dbms.yadbms.config;

import static com.dbms.yadbms.config.Constants.DEFAULT_PAGE_ID;

/* * ClassName: PageId
 * This class represents a unique identifier for a frame in the system.
 * It encapsulates an Integer value and provides methods to access it.
 */
public class PageId {

    private final Integer value;

    public PageId(Integer value) {
        if (value <= 0) {
            value = DEFAULT_PAGE_ID;
        }
        this.value = value;
    }

    /**
     * Returns the Integer value of this FrameId.
     *
     * @return the Integer value
     */
    public int getValue() {
        return value;
    }

    /**
     * Returns the String representation of this FrameId.
     *
     * @return the String representation
     */
    public String toString() {
        return Integer.toString(value);
    }

    /**
     * Checks if this FrameId is valid.
     * A FrameId is considered valid if its value is not null and is non-negative.
     *
     * @return true if valid, false otherwise
     */
    public Boolean isValid() {
        return value != null && value >= 0;
    }
}
