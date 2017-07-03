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
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.cesarpomar.perldoop.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;

public class SentencesBoltEnglish extends BaseBasicBolt {

    private HashMap<String, Object> abrev;
    private String UpperCase;
    private String LowerCase;
    private final String ABREV_FILE = "input/en/lexicon/abreviaturas-en.txt";
    private String mark_sigla;
    private String mark_abr;
    private Perl perl;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        perl = Perl.getInstance();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(this.ABREV_FILE)))) {

            this.abrev = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if ((JRegex.match(line, "m/\\./"))) {
                    line = perl.chomp(line);
                    String[] pd_me = perl.split(" ", line);
                    pd_me[0] = JRegex.s(pd_me[0], "s/\\./\\\\./g");
                    this.abrev.put(pd_me[0], null);
                }
            }

        } catch (Exception ex) {
            perl.die("Error reading the file: " + ex);
        }

        this.UpperCase = "[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÑÇÜ]";
        this.LowerCase = "[a-záéíóúàèìòùâêîôûñçü]";

        this.mark_sigla = "<SIGLA-TMP>";
        this.mark_abr = "<ABR-TMP>";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd
    ) {
        ofd.declareStream("lang_en", new Fields("sentences", "rowkey"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc
    ) {
        String texto = (String) tuple.getValueByField("content");
        List<String> saida = new PerlList<String>();

        for (String word : texto.split(" ")) {
            if (abrev.containsKey(word)) {
                texto = JRegex.s(texto, "s/^(" + word + ")/$1" + mark_abr + "/g");
                texto = JRegex.s(texto, "s/(\\s)(" + word + ")/$1$2" + mark_abr + "/g");
                texto = JRegex.s(texto, "s/\\.(" + mark_abr + ")/$1/g");
            }
        }

        texto = JRegex.s(texto, "s/(" + LowerCase + ")\\.(" + LowerCase + ")/$1" + mark_sigla + "$2/g");
        texto = JRegex.s(texto, "s/\\.\\.\\./" + mark_sigla + "" + mark_sigla + "" + mark_sigla + "/g");
        texto = JRegex.s(texto, "s/([0-9]+)\\.([0-9]+)/$1" + mark_sigla + "$2/g");
        texto = JRegex.s(texto, "s/(" + UpperCase + ")\\.(" + UpperCase + ")/$1" + mark_sigla + "$2/g");
        texto = JRegex.s(texto, "s/(" + mark_sigla + "" + UpperCase + ")\\.(" + UpperCase + ")/$1" + mark_sigla + "$2/g");
        texto = JRegex.s(texto, "s/(" + mark_sigla + "" + UpperCase + ")\\.([\\s]+)(" + LowerCase + ")/$1" + mark_sigla + "$2$3/g");
        texto = JRegex.s(texto, "s/\\.([^\"'])/\\.\n$1/g");
        texto = JRegex.s(texto, "s/$/\n/");
        texto = JRegex.s(texto, "s/(\n)[\\s]+/$1/g");
        texto = JRegex.s(texto, "s/" + mark_abr + "/\\./g");
        texto = JRegex.s(texto, "s/" + mark_sigla + "/\\./g");
        for (String sentence : perl.split("\n", texto)) {
            perl.push(saida, sentence);
        }

        ArrayList<String> output = (ArrayList<String>) saida;
        boc.emit("lang_en", new Values(output, tuple.getValueByField("rowkey")));
    }

}
