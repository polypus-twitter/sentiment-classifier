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

import com.brunneis.polypus.stormsa.vo.BasicDigitalPostContent;
import java.util.ArrayList;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Priority;
import com.brunneis.polypus.stormsa.conf.AerospikeConf;
import com.brunneis.polypus.stormsa.conf.Conf;

/**
 *
 * @author brunneis
 */
public class DigitalPostsSingletonDAOAerospike implements DigitalPostsDAO,
        ScanCallback {

    private static final DigitalPostsSingletonDAOAerospike INSTANCE
            = new DigitalPostsSingletonDAOAerospike();

    private final AerospikeClient inputClient;
    private final AerospikeClient outputClient;

    private ArrayList<BasicDigitalPostContent> posts;

    private final String inputNamespace
            = ((AerospikeConf) Conf.INPUT_BUFFER_DB.value()).NAME.value();
    private final String inputSet
            = ((AerospikeConf) Conf.INPUT_BUFFER_DB.value()).SET.value();

    private final String outputNamespace
            = ((AerospikeConf) Conf.OUTPUT_BUFFER_DB.value()).NAME.value();
    private final String outputSet
            = ((AerospikeConf) Conf.OUTPUT_BUFFER_DB.value()).SET.value();

    private DigitalPostsSingletonDAOAerospike() {
        String inputHost
                = ((AerospikeConf) Conf.INPUT_BUFFER_DB.value()).HOST.value();
        Integer inputPort
                = ((AerospikeConf) Conf.INPUT_BUFFER_DB.value()).PORT.value();

        String outputHost
                = ((AerospikeConf) Conf.OUTPUT_BUFFER_DB.value()).HOST.value();
        Integer outputPort
                = ((AerospikeConf) Conf.OUTPUT_BUFFER_DB.value()).PORT.value();

        ClientPolicy policy = new ClientPolicy();

        policy.readPolicyDefault.timeout = 60000;
        // Regardless the number of retries: 60s
        policy.readPolicyDefault.maxRetries = 5;
        policy.readPolicyDefault.sleepBetweenRetries = 500;

        policy.writePolicyDefault.timeout = 60000;
        policy.writePolicyDefault.maxRetries = 5;
        policy.writePolicyDefault.sleepBetweenRetries = 500;
        policy.writePolicyDefault.expiration = -1;

        policy.scanPolicyDefault.includeBinData = true;
        policy.scanPolicyDefault.priority = Priority.HIGH;
        policy.scanPolicyDefault.concurrentNodes = true;
        policy.scanPolicyDefault.timeout = 60000;
        policy.scanPolicyDefault.maxRetries = 5;
        policy.readPolicyDefault.sleepBetweenRetries = 500;

        this.inputClient = new AerospikeClient(policy, inputHost, inputPort);
        this.outputClient = new AerospikeClient(policy, outputHost, outputPort);
    }

    public static DigitalPostsSingletonDAOAerospike getInstance() {
        return INSTANCE;
    }

    @Override
    // AerospikeClient is already thread-safe.
    // See: http://www.aerospike.com/docs/client/java/usage/best_practices.html
    public void writeSentiment(String rowkey, Integer sentiment) {
        Key keyToHash = new Key(outputNamespace, outputSet, rowkey);
        Bin bsentiment = new Bin("sentiment", sentiment);
        Bin browkey = new Bin("rowkey", rowkey);

        // Añade el nuevo bin a la fila correspondiente (comportamiento por defecto)
        this.outputClient.put(null, keyToHash, browkey, bsentiment);
    }

    @Override
    public synchronized ArrayList<BasicDigitalPostContent> readAllAvailableInputDigitalPosts() {
        // Se limpia el buffer de posts del DAO y se carga
        this.posts = new ArrayList<>();
        this.inputClient.scanAll(null, inputNamespace, inputSet, this);
        return this.posts;
    }

    @Override
    // Called only under readAllAvailableInputDigitalPosts() (synchronized)
    public void scanCallback(Key key, Record record) throws AerospikeException {
        synchronized (this.posts) {
            this.posts.add(new BasicDigitalPostContent(
                    record.getString("rowkey"),
                    record.getString("content"),
                    record.getString("language")
            ));
        }

        // Delete every read record
        this.inputClient.delete(null, key);
    }

}
