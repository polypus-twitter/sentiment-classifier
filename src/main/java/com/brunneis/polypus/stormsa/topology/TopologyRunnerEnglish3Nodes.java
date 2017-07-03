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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

/**
 *
 * @author brunneis
 */
public class TopologyRunnerEnglish3Nodes {

    public static void main(String[] args) {
        StormTopology topology = TopologyBuilderEnglish3Nodes.build();
        Config config = new Config();

        config.setNumWorkers(2);
        config.setMessageTimeoutSecs(30);

        try {
            StormSubmitter.submitTopology(
                    "polypus-sentiment-classifier",
                    config,
                    topology
            );
        } catch (AlreadyAliveException
                | InvalidTopologyException
                | AuthorizationException ex) {
            Logger.getLogger(TopologyRunnerEnglish3Nodes.class.getName())
                    .log(Level.SEVERE, null, ex);
        }

    }

}
