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

import com.cesarpomar.perldoop.*;
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

public class TokensBoltEnglish extends BaseBasicBolt {

    // private static String abs_path;
    // private static String UpperCase;
    // private static String LowerCase;
    private String Punct;
    private String Punct_urls;
    private String contr;
    private String w;
    private Perl perl;

    @Override
    public void prepare(Map config, TopologyContext context) {
        perl = Perl.getInstance();
        // abs_path = "";
        // UpperCase = "[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÑÇÜ]";
        // LowerCase = "[a-záéíóúàèìòùâêîôûñçü]";
        Punct = "[,;«»“”'\"&\\$#=\\(\\)<>!¡\\?¿\\\\[\\]\\{\\}\\|\\^\\*\\-€·¬…]";
        Punct_urls = "[:\\/~]";
        contr = "([Hh]e|[Hh]ere|[Hh]ow|[Ii]t|[Ss]he|[Tt]hat|[Tt]here|[Ww]hat|[Ww]hen|[Ww]here|[Ww]ho|[Ww]hy)";
        w = "[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÑÇÜa-záéíóúàèìòùâêîôûñçü]";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declareStream("lang_en", new Fields("tokens", "rowkey"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        ArrayList<String> sentences = (ArrayList<String>) tuple.getValueByField("normalizer");
        List<String> saida = new PerlList<String>();

        String token;

        String susp = "3SUSP012";
        String duplo1 = "2DOBR111";
        String duplo2 = "2DOBR222";
        String duplo3 = "2DOBR333";
        String duplo4 = "2DOBR444";
        String dot_quant = "44DOTQUANT77";
        String comma_quant = "44COMMQUANT77";
        String quote_quant = "44QUOTQUANT77";
        for (String sentence : sentences) {
            Ref<String> pd_f;
            sentence = perl.chomp(sentence);
            sentence = JRegex.s(sentence, "s/[ ]*$//");
            sentence = JRegex.s(sentence, "s/\\.\\.\\./ " + susp + " /g");
            sentence = JRegex.s(sentence, "s/<</ " + duplo1 + " /g");
            sentence = JRegex.s(sentence, "s/>>/ " + duplo2 + " /g");
            sentence = JRegex.s(sentence, "s/''/ " + duplo3 + " /g");
            sentence = JRegex.s(sentence, "s/``/ " + duplo4 + " /g");
            sentence = JRegex.s(sentence, "s/(^| )(" + contr + ")'s/$1$2APOTEMPs/g");
            sentence = JRegex.s(sentence, "s/(^| )([A-Z][a-z]+)'s/$1$2 GENs/g");
            sentence = JRegex.s(sentence, "s/([Ia-z])'(ve|ll|s|re|m|d|t)/$1APOTEMP$2/g");
            sentence = JRegex.s(sentence, "s/([0-9]+)\\.([0-9]+)/$1" + dot_quant + "$2 /g");
            sentence = JRegex.s(sentence, "s/([0-9]+),([0-9]+)/$1" + comma_quant + "$2 /g");
            sentence = JRegex.s(sentence, "s/([0-9]+)'([0-9]+)/$1" + quote_quant + "$2 /g");
            sentence = JRegex.s(sentence, "s/(" + Punct + ")/ $1 /g");
            sentence = JRegex.s(sentence, "s/(" + Punct_urls + ")(?:[\\s\n]|$)/ $1 /g");
            sentence = JRegex.s(sentence, "s/(\\w)- /$1 - /g");
            sentence = JRegex.s(sentence, "s/ -(\\w)/ - $1/g");
            sentence = JRegex.s(sentence, "s/(\\w)-$/$1 -/g");
            sentence = JRegex.s(sentence, "s/^-(\\w)/- $1/g");
            sentence = JRegex.s(sentence, "s/\\.$/ \\. /g");
            String[] tokens = perl.split(" ", sentence);
            for (String pd_i : tokens) {
                token = pd_i;
                token = JRegex.s(token, "s/^[\\s]*//");
                token = JRegex.s(token, "s/[\\s]*$//");
                token = JRegex.s(token, "s/" + susp + "/\\.\\.\\./");
                token = JRegex.s(token, "s/" + duplo1 + "/<</");
                token = JRegex.s(token, "s/" + duplo2 + "/>>/");
                token = JRegex.s(token, "s/" + duplo3 + "/''/");
                token = JRegex.s(token, "s/" + duplo4 + "/``/");
                token = JRegex.s(token, "s/" + dot_quant + "/\\./");
                token = JRegex.s(token, "s/" + comma_quant + "/,/");
                token = JRegex.s(token, "s/" + quote_quant + "/'/");
                token = JRegex.s(token, "s/GENs/'s/");
                if (JRegex.match(token, "m/APOTEMPt$/")) {
                    if (JRegex.match(token, "m/^([Cc]a|[Ww]o)nAPOTEMP/")) {
                        token = JRegex.s(token, "s/^(.)anAPOTEMPt/$1an\nnot/");
                        token = JRegex.s(token, "s/^(.)onAPOTEMPt/$1ill\nnot/");
                    } else if (JRegex.match(token, "m/nAPOTEMP/")) {
                        token = JRegex.s(token, "s/nAPOTEMPt/\nnot/");
                    } else {
                        token = JRegex.s(token, "s/([a-z])APOTEMP([a-z])/$1\nnot/g");
                    }
                } else if (JRegex.match(token, "m/APOTEMPve$/")) {
                    token = JRegex.s(token, "s/APOTEMPve/\nhave/g");
                } else if (JRegex.match(token, "m/APOTEMPre$/")) {
                    token = JRegex.s(token, "s/APOTEMPre/\nare/g");
                } else if (JRegex.match(token, "m/APOTEMPll$/")) {
                    token = JRegex.s(token, "s/APOTEMPll/\nwill/g");
                } else if (JRegex.match(token, "m/APOTEMPm$/")) {
                    token = JRegex.s(token, "s/APOTEMPm/\nam/");
                } else if (JRegex.match(token, "m/^[Ww]anna$/")) {
                    token = JRegex.s(token, "s/^(.).+$/1ant\nto/");
                } else if (JRegex.match(token, "m/^[Cc]annot$/")) {
                    token = JRegex.s(token, "s/^(.).+$/$1an\nnot/");
                } else if (JRegex.match(token, "m/^[Gg]onna$/")) {
                    token = JRegex.s(token, "s/^(.).+$/$1oing\nto/");
                } else if (JRegex.match(token, "m/^[Gg]otta$/")) {
                    token = JRegex.s(token, "s/^(.).+$/$1ot\nto/");
                } else if (JRegex.match(token, "m/^[Hh]afta$/")) {
                    token = JRegex.s(token, "s/^(.).+$/$1ave\nto/");
                } else if (JRegex.match(token, "m/^([Cc]oulda|[Ss]houlda|[Ww]oulda|[Mm]usta)$/")) {
                    token = JRegex.s(token, "s/^(.+)a$/$1\nhave/");
                } else {
                    token = JRegex.s(token, "s/([a-z])APOTEMP([a-z])/$1\n'2/g");
                }
                perl.push(saida, token);
            }
            perl.push(saida, "\n");
        }

        ArrayList<String> output = (ArrayList<String>) saida;
        boc.emit("lang_en", new Values(output, tuple.getValueByField("rowkey")));
    }

}
