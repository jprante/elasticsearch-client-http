package org.elasticsearch.client.http;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Strings {

    private Strings() {
    }

    /**
     * Gives a string consisting of the elements of a given array of strings, each
     * separated by a given separator string.
     *
     * @param pieces    the strings to join
     * @param separator the separator
     * @return the joined string
     */
    public static String join(String separator, String[] pieces) {
        return join(separator, Arrays.asList(pieces));
    }

    /**
     * Gives a string consisting of the string representations of the elements of a
     * given array of objects, each separated by a given separator string.
     *
     * @param pieces    the elements whose string representations are to be joined
     * @param separator the separator
     * @return the joined string
     */
    public static String join(String separator, List<String> pieces) {
        StringBuilder buffer = new StringBuilder();
        for (Iterator<String> iter = pieces.iterator(); iter.hasNext(); ) {
            buffer.append(iter.next());
            if (iter.hasNext()) {
                buffer.append(separator);
            }
        }
        return buffer.toString();
    }
}
