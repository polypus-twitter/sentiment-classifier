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
package com.brunneis.polypus.stormsa.conf;

import com.brunneis.locker.Locker;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brunneis
 */
public class Conf {

    public final static int HBASE = 002;
    public final static int AEROSPIKE = 003;

    public final static Locker<String> CONF_FILE = new Locker<>();
    public final static Locker<Level> LOGGER_LEVEL = new Locker<>();
    public final static Locker<DBConf> INPUT_BUFFER_DB = new Locker<>();
    public final static Locker<DBConf> OUTPUT_BUFFER_DB = new Locker<>();

    static {

        if (!CONF_FILE.isLocked()) {
            CONF_FILE.set("/data/polypus-stormsa.conf");
        }

        if (!LOGGER_LEVEL.isLocked()) {
            LOGGER_LEVEL.set(Level.INFO);
        }

        File file = new File(CONF_FILE.value());
        if (!file.exists()) {
            System.exit(1);
        }

        Properties properties = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(CONF_FILE.value());
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Conf.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            properties.load(input);
        } catch (IOException ex) {
            Logger.getLogger(Conf.class.getName()).log(Level.SEVERE, null, ex);
        }

        AerospikeConf iasc = new AerospikeConf();
        iasc.CURRENT.set(AEROSPIKE);
        iasc.NAME.set(properties.getProperty("ASI_NAME"));
        iasc.HOST.set(properties.getProperty("ASI_HOST"));
        iasc.PORT.set(Integer.parseInt(properties.getProperty("ASI_PORT")));
        iasc.SET.set(properties.getProperty("ASI_SET"));

        AerospikeConf oasc = new AerospikeConf();
        oasc.CURRENT.set(AEROSPIKE);
        oasc.NAME.set(properties.getProperty("ASO_NAME"));
        oasc.HOST.set(properties.getProperty("ASO_HOST"));
        oasc.PORT.set(Integer.parseInt(properties.getProperty("ASO_PORT")));
        oasc.SET.set(properties.getProperty("ASO_SET"));

        INPUT_BUFFER_DB.set(iasc);
        OUTPUT_BUFFER_DB.set(oasc);

    }

}
