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
package com.brunneis.polypus.stormsa.spout;

import com.brunneis.polypus.stormsa.vo.BasicDigitalPostContent;
import com.brunneis.polypus.stormsa.dao.DigitalPostsSingletonFactoryDAO;
import com.brunneis.polypus.stormsa.conf.Conf;
import java.util.ArrayList;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 *
 * @author brunneis
 */
public class PolypusSpout extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    private ArrayList<BasicDigitalPostContent> digitalPosts;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declareStream("lang_en", new Fields("rowkey", "content"));
        // ofd.declareStream("lang_es", new Fields("rowkey", "content"));
        // ofd.declareStream("lang_pt", new Fields("rowkey", "content"));
        // ofd.declareStream("lang_gl", new Fields("rowkey", "content"));
        // ofd.declareStream("lang_fr", new Fields("rowkey", "content"));
        // ofd.declareStream("lang_it", new Fields("rowkey", "content"));
    }

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc) {
        this.outputCollector = soc;
        this.digitalPosts = new ArrayList<>();
        loadAvailablePosts();
    }

    private void loadAvailablePosts() {
        ArrayList<BasicDigitalPostContent> scannedPosts
                = DigitalPostsSingletonFactoryDAO.getDigitalPostDAOinstance(Conf.AEROSPIKE)
                        .readAllAvailableInputDigitalPosts();

        if (!scannedPosts.isEmpty()) {
            this.digitalPosts.addAll(scannedPosts);
        }
    }

    @Override
    public synchronized void nextTuple() {
        if (digitalPosts.isEmpty()) {
            this.loadAvailablePosts();

            if (digitalPosts.isEmpty()) {
                return;
            }
        }

        // Proceed if new posts are retrieved
        if (this.digitalPosts.size() > 0) {
            // Avoid null posts
            if (this.digitalPosts.get(0) == null) {
                this.digitalPosts.remove(0);
                return;
            }

            // The first item of the queue is emitted in its stream
            BasicDigitalPostContent firstOfTheQueue = new BasicDigitalPostContent(this.digitalPosts.get(0));

            // The emitted item is removed of the queue
            this.digitalPosts.remove(0);

            outputCollector.emit("lang_" + firstOfTheQueue.getLanguage(),
                    new Values(
                            firstOfTheQueue.getRowkey(),
                            firstOfTheQueue.getContent()
                    ));
        }
    }

}
