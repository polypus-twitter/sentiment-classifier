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
package com.brunneis.polypus.stormsa.bolt.en;

import com.cesarpomar.perldoop.HashPerl;
import java.util.Map;

/**
 *
 * @author brunneis
 */
public class CloneHelper {

    protected static Map<String, Map<String, Double>>
            getMapCopyKStringVMapKStringVDouble(Map<String, Map<String, Double>> map) {
        Map<String, Map<String, Double>> mainMapCopy = new HashPerl<>();

        map.keySet().forEach((String key) -> {
            Map<String, Double> internalMapCopy = new HashPerl<>();
            map.get(key).keySet().forEach((key2) -> {
                internalMapCopy.put(key2, map.get(key).get(key2));
            });
            mainMapCopy.put(key, internalMapCopy);
        });
        return mainMapCopy;
    }

    protected static Map<String, Double>
            getMapCopyKStringVDouble(Map<String, Double> map) {
        Map<String, Double> mapCopy = new HashPerl<>();
        map.keySet().forEach((String key) -> {
            mapCopy.put(key, map.get(key));
        });
        return mapCopy;
    }

}
