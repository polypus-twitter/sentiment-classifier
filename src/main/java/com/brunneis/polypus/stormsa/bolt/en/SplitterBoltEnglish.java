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
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.cesarpomar.perldoop.*;

public class SplitterBoltEnglish extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declareStream("lang_en", new Fields("splitter", "rowkey"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {

        Perl perl = Perl.getInstance();

        ArrayList<String> text = (ArrayList<String>) tuple.getValueByField("tokens");

        List<String> saida = new PerlList<String>();
        for (Integer i = 0; i < text.size(); i++) {
            text.set(i, perl.chomp(text.get(i)));
            String token = text.get(i);
            if (JRegex.match(token, "m/ /")) {
                String[] tokens = perl.split(" ", token);
                for (String tok : tokens) {
                    perl.push(saida, tok);
                }
            } else {
                perl.push(saida, token);
            }
        }

        ArrayList<String> output = (ArrayList<String>) saida;
        boc.emit("lang_en", new Values(output, tuple.getValueByField("rowkey")));
    }

}
