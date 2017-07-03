/*
    Polypus: a Big Data Self-Deployable Architecture for Microblogging 
    Text Extraction and Real-Time Sentiment Analysis

    Copyright (C) 2017 Rodrigo Martínez (brunneis) <dev@brunneis.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.brunneis.polypus.stormsa.dao;

/**
 *
 * @author brunneis
 */
public class RowKeyHelper {

    private static final String[] PREFIXES;
    private static final int BUCKETS_NUMBER = 32;
    private static final int SALT_LENGTH = Integer.toString(BUCKETS_NUMBER).length();

    private static int counter = 0;

    static {
        PREFIXES = new String[BUCKETS_NUMBER];
        for (byte i = 0; i < BUCKETS_NUMBER; i++) {
            PREFIXES[i] = "";
            int iLength = Integer.toString(i).length();
            if (iLength < SALT_LENGTH) {
                int diff = SALT_LENGTH - iLength;
                for (int j = 0; j < diff; j++) {
                    PREFIXES[i] += "0";
                }
            }
            PREFIXES[i] += i;
        }
    }

    public static String getSourceFromKey(String key) {
        String[] splits = key.split("_");
        return splits[1];
    }

    public static String getOriginalKey(String saltedKey) {
        return saltedKey.substring(SALT_LENGTH);
    }

    public static String getSaltedKey(String originalKey) {
        String saltedKey = PREFIXES[counter] + originalKey;
        counter = (counter + 1) % BUCKETS_NUMBER;
        return saltedKey;
    }

}
