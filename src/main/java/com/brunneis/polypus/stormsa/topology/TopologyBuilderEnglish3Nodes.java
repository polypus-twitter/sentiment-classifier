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
package com.brunneis.polypus.stormsa.topology;

import com.brunneis.polypus.stormsa.bolt.en.*;
import com.brunneis.polypus.stormsa.spout.PolypusSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @author brunneis
 */
public class TopologyBuilderEnglish3Nodes {

    public static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("polypus", new PolypusSpout(), 1);

        builder.setBolt("sentences", new SentencesBoltEnglish(), 1)
                .setNumTasks(16)
                .shuffleGrouping("polypus", "lang_en");

        builder.setBolt("normalizer", new NormalizerBoltEnglish(), 1)
                .setNumTasks(16)
                .shuffleGrouping("sentences", "lang_en");

        builder.setBolt("tokens", new TokensBoltEnglish(), 1)
                .setNumTasks(16)
                .shuffleGrouping("normalizer", "lang_en");

        builder.setBolt("splits", new SplitterBoltEnglish(), 1)
                .setNumTasks(16)
                .shuffleGrouping("tokens", "lang_en");

        builder.setBolt("ner", new NerBoltEnglish(), 2)
                .setNumTasks(16)
                .shuffleGrouping("splits", "lang_en");

        builder.setBolt("tagger", new TaggerBoltEnglish(), 6)
                .setNumTasks(16)
                .shuffleGrouping("ner", "lang_en");

        builder.setBolt("nbayes", new SentimentBoltEnglish(), 2)
                .setNumTasks(16)
                .shuffleGrouping("tagger", "lang_en");

        return builder.createTopology();
    }

}
