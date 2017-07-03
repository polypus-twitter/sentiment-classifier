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

import java.util.ArrayList;
import java.util.List;
import jregex.*;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.cesarpomar.perldoop.*;

public class NormalizerBoltEnglish extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declareStream("lang_en", new Fields("normalizer", "rowkey"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {

        ArrayList<String> sentences = (ArrayList<String>) tuple.getValueByField("sentences");
        List<String> saida = new PerlList<>();

        for (String sentence : sentences) {
            String processed = "";
            for (String word : sentence.split(" ")) {

                // Para resolver el problema de los emoticonos seguidos de punto o coma: :), --- :). ---
                String removed_last_char = null;
                if (word.length() > 1) {
                    // Si la última posición de la palabra es '.' o ',' pero no la penúltima
                    if (new Pattern("[^,.][,.]").matches(word.substring(word.length() - 2, word.length()))) {
                        removed_last_char = word.substring(word.length() - 1, word.length());
                        word = word.substring(0, word.length() - 1);
                    }
                }

                if (new Pattern(".*a[a]+.*").matches(word)) {
                    Matcher m = new Pattern("([^a]*)a[a]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "a" + m.group(2);
                    }
                }
                if (new Pattern(".*A[A]+.*").matches(word)) {
                    Matcher m = new Pattern("([^A]*)A[A]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "A" + m.group(2);
                    }
                }

                // *ee en lugar de *e para permitir palabras como "keep"
                if (new Pattern(".*ee[e]+.*").matches(word)) {
                    Matcher m = new Pattern("([^e]*)e[e]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "e" + m.group(2);
                    }
                }
                if (new Pattern(".*EE[E]+.*").matches(word)) {
                    Matcher m = new Pattern("([^E]*)E[E]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "E" + m.group(2);
                    }
                }

                if (new Pattern(".*u[i]+.*").matches(word)) {
                    Matcher m = new Pattern("([^i]*)i[i]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "i" + m.group(2);
                    }
                }
                if (new Pattern(".*U[I]+.*").matches(word)) {
                    Matcher m = new Pattern("([^I]*)I[I]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "I" + m.group(2);
                    }
                }

                // *oo en lugar de *o para permitir palabras como "blood"
                if (new Pattern(".*oo[o]+.*").matches(word)) {
                    Matcher m = new Pattern("([^o]*)o[o]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "o" + m.group(2);
                    }
                }
                if (new Pattern(".*OO[O]+.*").matches(word)) {
                    Matcher m = new Pattern("([^O]*)O[O]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "O" + m.group(2);
                    }
                }

                if (new Pattern(".*u[u]+.*").matches(word)) {
                    Matcher m = new Pattern("([^u]*)u[u]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "u" + m.group(2);
                    }
                }

                if (new Pattern(".*U[U]+.*").matches(word)) {
                    Matcher m = new Pattern("([^U]*)U[U]+(.*)").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + "U" + m.group(2);
                    }
                }

                // Simplificado en una expresión, añadidas más alternativas
                if (new Pattern("[Xx:;][Oo-]?[Dd\\])][Dd\\])]*").matches(word)) {
                    word = "EMOT_POS";
                    // Simplificado en una expresión, añadidas más alternativas
                } else if (new Pattern("[Xx:;][Oo-]?[\\[(][\\[(]*").matches(word)) {
                    word = "EMOT_NEG";
                    // Emoticonos de horror
                } else if (new Pattern("[Dd][Oo-]?[Xx:;]").matches(word)) {
                    word = "EMOT_NEG";
                    // Carjada mejorada, más alternativas
                } else if (new Pattern("[AJaj]{4}[AJaj]*").matches(word)) {
                    word = "EMOT_POS";
                } else if (new Pattern("[EJej]{4}[EJej]*").matches(word)) {
                    word = "EMOT_POS";
                } else if (new Pattern("[IJij]{4}[IJij]*").matches(word)) {
                    word = "EMOT_POS";
                    // Carjada mejorada, más alternativas
                } else if (new Pattern("[AHah]{4}[AHah]*").matches(word)) {
                    word = "EMOT_POS";
                } else if (new Pattern("[EHeh]{4}[EHeh]*").matches(word)) {
                    word = "EMOT_POS";
                } else if (new Pattern("[IHih]{4}[IHih]*").matches(word)) {
                    word = "EMOT_POS";
                } else if (new Pattern("\\\"").matches(word)) {
                    word = "";
                } else if (new Pattern("\\]\\]$").matches(word)) {
                    word = "";
                } else if (new Pattern("!\\[CDATA\\[").matches(word)) {
                    word = "";
                } else if (new Pattern("(?:\\s|^|\\W)(news|http|https|ftp|ftps):\\/\\/[^\\s\\n]*").matches(word)) {
                    word = " ";
                } else if (new Pattern("(?:\\s|^|\\\"|\\“)(@|#)[^\\s\\n]*").matches(word)) {
                    word = " ";
                } else if (new Pattern("^\\s*(.*\\S)\\s*$").matches(word)) {
                    Matcher m = new Pattern("^\\s*(.*\\S)\\s*$").matcher(word);
                    if (m.find()) {
                        word = m.group(1);
                    }
                } else if (new Pattern("([A-Z])\\.([A-Z])").matches(word)) {
                    Matcher m = new Pattern("([A-Z])\\.([A-Z])").matcher(word);
                    if (m.find()) {
                        word = m.group(1) + m.group(2);
                    }
                }
                if (!word.equals("")) {
                    processed += word + " ";
                    if (removed_last_char != null) {
                        processed += removed_last_char + " ";
                    }
                }
            }
            // Se añade una sentencia al vector de salida
            Perl.getInstance().push(saida, processed);
        }

        ArrayList<String> output = (ArrayList<String>) saida;
        boc.emit("lang_en", new Values(output, tuple.getValueByField("rowkey")));
    }
}
