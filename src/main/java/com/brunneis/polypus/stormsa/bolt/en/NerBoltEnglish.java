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

public class NerBoltEnglish extends BaseBasicBolt {

    private Map<String, String> Entry;
    private Map<String, String> Lex;
    private Map<String, String> StopWords;
    private Map<String, String> Ambig;
    private Map<String, Boolean> Noamb;
    private String UpperCase;
    private String LowerCase;
    private String Punct;
    private String Punct_urls;
    private String w;

    private final String DIC_FILE = "input/en/lexicon/dicc.src";
    private final String AMBIG_FILE = "input/en/lexicon/ambig.txt";

    private Perl perl;

    @Override
    public void prepare(Map config, TopologyContext context) {

        perl = Perl.getInstance();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(this.DIC_FILE)))) {

            Entry = new HashPerl<String, String>();
            Lex = new HashPerl<String, String>();
            StopWords = new HashPerl<String, String>();
            Ambig = new HashPerl<String, String>();
            Noamb = new HashPerl<String, Boolean>();
            {
                String line;
                while ((line = reader.readLine()) != null) {
                    Ref<String> pd_f;
                    line = perl.chomp(line);
                    String[] entry = perl.split(" ", line);
                    String token = entry[0];
                    String entry2;
                    {
                        String[] pd_me = (JRegex.matchAvc(line, "m/^[^ ]+ ([\\w\\W]+)$/"));
                        entry2 = pd_me[0];
                    };
                    Entry.put(token, entry2);
                    Integer i = 1;
                    while (i < entry.length) {
                        String lemma = entry[i];
                        i++;
                        String tag = entry[i];
                        if (!perl.defined(Lex.get(token))) {
                            Lex.put(token, String.valueOf(0));
                        }
                        Lex.put(token, String.valueOf(Double.parseDouble(Lex.get(token)) + 1));
                        if (JRegex.match(tag, "m/^(P|SP|R|D|I|C)/")) {
                            StopWords.put(token, tag);
                        }
                        i++;
                    }
                    if (entry.length > 2 && JRegex.match(entry[2], "m/^NP/")) {
                        Noamb.put(token, true);
                    }
                }
            }

        } catch (Exception ex) {
            perl.die("Error reading the file: " + ex);
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(this.AMBIG_FILE)))) {
            UpperCase = "[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÑÇÜ]";
            LowerCase = "[a-záéíóúàèìòùâêîôûñçü]";
            Punct = "[,;«»“”'\"&\\$#=\\(\\)<>!¡\\?¿\\\\[\\]\\{\\}\\|\\^\\*\\-€·¬…]";
            Punct_urls = "[:\\/~]";
            w = "[A-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÑÇÜa-záéíóúàèìòùâêîôûñçü]";
            {
                String t;
                while ((t = reader.readLine()) != null) {
                    t = Trim(t);
                    if (!perl.defined(Ambig.get(t))) {
                        Ambig.put(t, String.valueOf(0));
                    }
                    Ambig.put(t, String.valueOf(Double.parseDouble(Ambig.get(t)) + 1));
                }
            }
        } catch (Exception ex) {
            perl.die("Error reading the file: " + ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declareStream("lang_en", new Fields("ner", "rowkey"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {

        ArrayList<String> text = (ArrayList<String>) tuple.getValueByField("splitter");

        Integer N = 10;
        String saidaString;
        List<String> saida = new PerlList<String>();
        List<String> saida2 = new PerlList<String>();
        String Prep = "(de|del|of)";
        String Art = "(el|la|los|las|the)";
        String currency = "(euro|euros|peseta|pesetas|€|dollar|dollars|pound|pounds)";
        String measure = "(kg|kilogram|gram|g|centimeter|cm|hour|second|minute|ton|tn|meter|km|kilometer|%)";
        String quant = "(hundred|hundreds|thousand|thousands|million|millions|bilion|billions||trillion|trillions)";
        String cifra = "(two|three|four|five|six|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|eighteen|nineteen|twenty|hundred|thousand)";
        String meses = "(january|february|march|april|may|june|july|august|september|october|november|december)";
        String SEP = "_";
        Map<String, String> Tag = new HashPerl<String, String>();
        String Candidate;
        String Nocandidate;
        String linhaFinal;
        String token = "";
        String adiantar;
        String text2 = perl.join("\n", text);
        String[] tokens = perl.split("\n", text2);
        for (Integer i = 0; i < tokens.length; i++) {
            Ref<String> pd_f;
            tokens[i] = perl.chomp(tokens[i]);
            Tag.put(tokens[i], "");
            String lowercase = lowercase(tokens[i]);
            if (JRegex.match(tokens[i], "m/^[ ]*$/")) {
                tokens[i] = "#SENT#";
            }
            Integer k = i - 1;
            Integer j = i + 1;
            if (!perl.defined(Noamb.get(tokens[i]))) {
                Noamb.put(tokens[i], false);
            }
            if (tokens.length > j && JRegex.match(tokens[i], "m/^" + UpperCase + "+$/") && JRegex.match(tokens[j], "m/^" + UpperCase + "+$/") && Pd.eval(Lex.get(lowercase)) && Pd.eval(Lex.get(lowercase(tokens[j])))) {
                Tag.put(tokens[i], "UNK");
            } else if (tokens.length > j && k > -1 && JRegex.match(tokens[i], "m/^" + UpperCase + "+$/") && JRegex.match(tokens[k], "m/^" + UpperCase + "+$/") && Pd.eval(Lex.get(lowercase)) && Pd.eval(Lex.get(lowercase(tokens[k]))) && (JRegex.match(tokens[j], "m/^(#SENT#|<blank>|\"|»|”|\\.|\\-|\\s|\\?|!|:)$/") || i == (tokens.length - 1))) {
                Tag.put(tokens[i], "UNK");
            } else if (k > -1 && tokens.length > i + 2 && JRegex.match(tokens[k], "m/^(\"|“|«|')/") && JRegex.match(tokens[i], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 1], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 2], "m/[\"»”']/")) {
                Candidate = tokens[i] + SEP + tokens[i + 1];
                i = i + 1;
                tokens[i] = Candidate;
            } else if (k > -1 && tokens.length > i + 3 && JRegex.match(tokens[k], "m/^(\"|“|«|')/") && JRegex.match(tokens[i], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 1], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 2], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 3], "m/[\"»”']/")) {
                Candidate = tokens[i] + SEP + tokens[i + 1] + SEP + tokens[i + 2];
                i = i + 2;
                tokens[i] = Candidate;
            } else if (k > -1 && tokens.length > i + 4 && JRegex.match(tokens[k], "m/^(\"|“|«|')/") && JRegex.match(tokens[i], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 1], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 2], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 3], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 4], "m/[\"»”']/")) {
                Candidate = tokens[i] + SEP + tokens[i + 1] + SEP + tokens[i + 2] + SEP + tokens[i + 3];
                i = i + 3;
                tokens[i] = Candidate;
            } else if (k > -1 && tokens.length > i + 5 && JRegex.match(tokens[k], "m/^(\"|“|«|')/") && JRegex.match(tokens[i], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 1], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 2], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 3], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 4], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 5], "m/[\"»”']/")) {
                Candidate = tokens[i] + SEP + tokens[i + 1] + SEP + tokens[i + 2] + SEP + tokens[i + 3] + SEP + tokens[i + 4];
                i = i + 4;
                tokens[i] = Candidate;
            } else if (k > -1 && tokens.length > i + 6 && JRegex.match(tokens[k], "m/^(\"|“|«|')/") && JRegex.match(tokens[i], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 1], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 2], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 3], "m/^" + UpperCase + "/") && JRegex.match(tokens[i + 4], "m/^" + UpperCase + "/") && Pd.eval(tokens[i + 5]) && JRegex.match(tokens[i + 6], "m/[\"»”']/")) {
                Candidate = tokens[i] + SEP + tokens[i + 1] + SEP + tokens[i + 2] + SEP + tokens[i + 3] + SEP + tokens[i + 4] + SEP + tokens[i + 5];
                i = i + 5;
                tokens[i] = Candidate;
            } else if (JRegex.match(tokens[i], "m/^" + UpperCase + "/") && Noamb.get(tokens[i])) {
                Tag.put(tokens[i], "NNP");
            } else if (k > -1 && (JRegex.match(tokens[i], "m/^" + UpperCase + "/")) && !Pd.eval(StopWords.get(lowercase)) && !JRegex.match(tokens[k], "m/^(#SENT#|<blank>|\"|“|«|\\.|\\-|\\s|\\?|!|:|``)$/") && !JRegex.match(tokens[k], "m/^\\.\\.\\.$/") && i > 0) {
                Tag.put(tokens[i], "NNP");
            } else if (k > -1 && (JRegex.match(tokens[i], "m/^" + UpperCase + "/") && !Pd.eval(StopWords.get(lowercase)) && JRegex.match(tokens[k], "m/^(#SENT#|<blank>|\"|“|«|\\.|\\-|\\s|\\?|!|:|``)$/")) || (i == 0)) {
                if (!Pd.eval(Lex.get(lowercase)) || Pd.eval(Ambig.get(lowercase))) {
                    Tag.put(tokens[i], "NNP");
                }
            }
            if (k > -1 && (JRegex.match(tokens[i], "m/^" + UpperCase + "" + LowerCase + "+/") && Pd.eval(Lex.get(lowercase)) && !Pd.eval(Ambig.get(lowercase))) && (JRegex.match(tokens[k], "m/^(#SENT#|<blank>|\"|“|«|\\.|\\-|\\s|\\?|!|:|``)$/") || i == 0)) {
            } else if (Pd.cmp(tokens[i], "I") == 0) {
                Tag.put(tokens[i], "UNK");
            } else if (JRegex.match(tokens[i], "m/^" + UpperCase + "" + LowerCase + "+/")) {
                Candidate = tokens[i];
                Integer count = 1;
                Boolean found = false;
                while ((!found) && (i + count < tokens.length)) {
                    j = i + count;
                    if (tokens.length > j && (Pd.cmp(tokens[j], "") == 0 || (JRegex.match(tokens[j], "m/^(" + Art + ")$/i") && !JRegex.match(tokens[j - 1], "m/^(" + Prep + ")$/i")))) {
                        found = true;
                    } else if (tokens.length > j && (!JRegex.match(tokens[j], "m/^" + UpperCase + "" + LowerCase + "+/") || JRegex.match(Candidate, "m/(" + Punct + ")|(" + Punct + "_urls)/")) && (!JRegex.match(tokens[j], "m/^(" + Prep + ")$/i") && !JRegex.match(tokens[j], "m/^(" + Art + ")$/i"))) {
                        found = true;
                    } else {
                        if (tokens.length > j) {
                            Candidate += SEP + tokens[j];
                        } else {
                            Candidate += SEP + "";
                        }
                        count++;
                    }
                }
                if ((count > 1) && (count <= N) && (!JRegex.match(Candidate, "m/" + Punct + "" + SEP + "/") || !JRegex.match(Candidate, "m/" + Punct_urls + "" + SEP + "/")) && !JRegex.match(Candidate, "m/" + SEP + "(" + Prep + ")$/") && !JRegex.match(Candidate, "m/" + SEP + "(" + Prep + ")" + SEP + "(" + Art + ")$/")) {
                    i = i + count - 1;
                    tokens[i] = Candidate;
                } else if ((count > 1) && (count <= N) && (!JRegex.match(Candidate, "m/" + Punct + "" + SEP + "/") || !JRegex.match(Candidate, "m/" + Punct_urls + "" + SEP + "/")) && JRegex.match(Candidate, "m/" + SEP + "(" + Prep + ")$/i")) {
                    i = i + count - 2;
                    Candidate = JRegex.s(Candidate, "s/" + SEP + "(" + Prep + ")$//");
                    tokens[i] = Candidate;
                } else if ((count > 1) && (count <= N) && (!JRegex.match(Candidate, "m/" + Punct + "" + SEP + "/") || !JRegex.match(Candidate, "m/" + Punct_urls + "" + SEP + "/")) && JRegex.match(Candidate, "m/" + SEP + "(" + Prep + ")" + SEP + "(" + Art + ")$/i")) {
                    i = i + count - 3;
                    Candidate = JRegex.s(Candidate, "s/" + SEP + "(" + Prep + ")" + SEP + "(" + Art + ")$//i");
                    tokens[i] = Candidate;
                } else if ((count > 1) && (count <= N) && (!JRegex.match(Candidate, "m/" + Punct + "" + SEP + "/") || !JRegex.match(Candidate, "m/" + Punct_urls + "" + SEP + "/")) && JRegex.match(Candidate, "m/SEP(" + Art + ")$/i")) {
                    i = i + count - 2;
                    Candidate = JRegex.s(Candidate, "s/" + SEP + "(" + Art + ")$//i");
                    tokens[i] = Candidate;
                }
            }
            if (JRegex.match(tokens[i], "m/[^\\s]_[^\\s]/")) {
                Tag.put(tokens[i], "NNP");
            } else if (!Pd.eval(Tag.get(tokens[i]))) {
                Tag.put(tokens[i], "UNK");
            }
            if (Pd.cmp(Tag.get(tokens[i]), "UNK") == 0) {
                token = lowercase(tokens[i]);
                if (Pd.eval(Lex.get(token))) {
                    Tag.put(tokens[i], Entry.get(token));
                }
            } else if (Pd.cmp(Tag.get(tokens[i]), "NNP") == 0) {
                token = lowercase(tokens[i]);
            }
            adiantar = String.valueOf(0);
            if (JRegex.match(tokens[i], "m/[0-9]+/") || JRegex.match(tokens[i], "m/^" + cifra + "$/")) {
                token = tokens[i];
                Tag.put(tokens[i], "Z");
            }
            if ((tokens.length > i + 1) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + measure + "(s|\\.)?$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zu");
                adiantar = String.valueOf(1);
            } else if ((tokens.length > i + 2) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + quant + "$/i") && JRegex.match(tokens[i + 2], "m/^" + measure + "(s|\\.)?$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zu");
                adiantar = String.valueOf(2);
            } else if ((tokens.length > i + 1) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + currency + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zm");
                adiantar = String.valueOf(1);
            } else if ((tokens.length > i + 1) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^\\$$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zm");
                adiantar = String.valueOf(1);
            } else if ((tokens.length > i + 3) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + quant + "$/i") && JRegex.match(tokens[i + 2], "m/^de$/i") && JRegex.match(tokens[i + 3], "m/^" + currency + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2] + "_" + tokens[i + 3];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zm");
                adiantar = String.valueOf(3);
            } else if ((tokens.length > i + 2) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + quant + "$/i") && JRegex.match(tokens[i + 2], "m/^" + currency + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zm");
                adiantar = String.valueOf(2);
            } else if ((tokens.length > i + 2) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^de$/i") && JRegex.match(tokens[i + 2], "m/^" + currency + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Zm");
                adiantar = String.valueOf(2);
            } else if ((tokens.length > i + 1) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + quant + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "Z");
                adiantar = String.valueOf(1);
            } else if ((tokens.length > i + 4) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^of$/i") && JRegex.match(tokens[i + 2], "m/^" + meses + "$/i") && JRegex.match(tokens[i + 3], "m/^,$/i") && JRegex.match(tokens[i + 4], "m/[0-9][0-9][0-9][0-9]+/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2] + "_" + tokens[i + 3] + "_" + tokens[i + 4];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(4);
            } else if ((tokens.length > i + 3) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^of$/i") && JRegex.match(tokens[i + 2], "m/^" + meses + "$/i") && JRegex.match(tokens[i + 3], "m/[0-9][0-9][0-9][0-9]+/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2] + "_" + tokens[i + 3];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(3);
            } else if ((tokens.length > i + 2) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + meses + "$/i") && JRegex.match(tokens[i + 2], "m/^[0-9][0-9][0-9][0-9]+$/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(2);
            } else if ((tokens.length > i + 2) && ((JRegex.match(tokens[i], "m/^" + meses + "$/i") && JRegex.match(tokens[i + 1], "m/^of$/i") && JRegex.match(tokens[i + 2], "m/^[0-9][0-9][0-9][0-9]+$/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(2);
            } else if ((tokens.length > i + 2) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^of$/i") && JRegex.match(tokens[i + 2], "m/^" + meses + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(2);
            } else if ((tokens.length > i + 1) && ((JRegex.match(Tag.get(tokens[i]), "m/^Z/") && JRegex.match(tokens[i + 1], "m/^" + meses + "$/i")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(1);
            } else if (((tokens.length > i + 4) && ((Tag.get(tokens[i + 2]) != null))) && ((JRegex.match(tokens[i], "m/^" + meses + "$/i") && JRegex.match(tokens[i + 1], "m/^the$/i") && JRegex.match(Tag.get(tokens[i + 2]), "m/^Z/") && JRegex.match(tokens[i + 3], "m/^,$/i") && JRegex.match(tokens[i + 4], "m/^[0-9][0-9][0-9][0-9]+$/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2] + "_" + tokens[i + 3] + "_" + tokens[i + 4];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(4);
            } else if (((tokens.length > i + 3) && ((Tag.get(tokens[i + 2]) != null))) && ((JRegex.match(tokens[i], "m/^" + meses + "$/i") && JRegex.match(tokens[i + 1], "m/^the$/i") && JRegex.match(Tag.get(tokens[i + 2]), "m/^Z/") && JRegex.match(tokens[i + 3], "m/^[0-9][0-9][0-9][0-9]+$/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2] + "_" + tokens[i + 3];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(3);
            } else if (((tokens.length > i + 2) && ((Tag.get(tokens[i + 1]) != null))) && ((JRegex.match(tokens[i], "m/^" + meses + "$/i") && JRegex.match(Tag.get(tokens[i + 1]), "m/^Z/") && JRegex.match(tokens[i + 2], "m/^[0-9][0-9][0-9][0-9]+$/")))) {
                tokens[i] = tokens[i] + "_" + tokens[i + 1] + "_" + tokens[i + 2];
                token = perl.lc(tokens[i]);
                Tag.put(tokens[i], "W");
                adiantar = String.valueOf(2);
            }
            if (Pd.cmp(tokens[i], ".") == 0) {
                token = ".";
                Tag.put(tokens[i], "Fp");
            } else if (((tokens.length > i - 2) && (i > 0)) && ((Pd.cmp(tokens[i], "#SENT#") == 0 && Pd.cmp(tokens[i - 1], ".") != 0 && Pd.cmp(tokens[i - 1], "<blank>") != 0))) {
                tokens[i] = "<blank>";
                token = "<blank>";
                Tag.put(tokens[i], "Fp");
            } else if (JRegex.match(tokens[i], "m/^" + Punct + "$/") || JRegex.match(tokens[i], "m/^" + Punct_urls + "$/") || JRegex.match(tokens[i], "m/^(\\.\\.\\.|``|''|<<|>>|\\-\\-)$/")) {
                Tag.put(tokens[i], punct(tokens[i]));
                token = tokens[i];
            }
            if (Pd.cmp(tokens[i], "#SENT#") == 0) {
                continue;
            }
            if ((JRegex.match(Tag.get(tokens[i]), "m/^(UNK|F|NNP|Z)/") || Pd.cmp(Tag.get(tokens[i]), "W") == 0)) {
                Tag.put(tokens[i], token + " " + Tag.get(tokens[i]));
            }
            saidaString = tokens[i] + " " + Tag.get(tokens[i]);
            perl.push(saida, saidaString);
            if (i == (tokens.length - 1) && Pd.cmp(tokens[i], ".") != 0) {
                saidaString = "<blank> <blank> Fp";
                perl.push(saida, saidaString);
            }
            Tag.put(tokens[i], "");
            if (Pd.eval((adiantar))) {
                i = Pd.toInteger(i + Double.parseDouble(adiantar));
            }
        }
        for (String s : saida) {
            if (JRegex.match(s, "m/Fp$/")) {
                s += "\n";
            }
            perl.push(saida2, s);
        }

        ArrayList<String> output = (ArrayList<String>) saida2;
        boc.emit("lang_en", new Values(output, tuple.getValueByField("rowkey")));

    }

    public String punct(Object... pd_argv) {
        String p;
        p = ((String) pd_argv[0]);
        String result = "";
        if (Pd.cmp(p, ".") == 0) {
            result = "Fp";
        } else if (Pd.cmp(p, ",") == 0) {
            result = "Fc";
        } else if (Pd.cmp(p, ":") == 0) {
            result = "Fd";
        } else if (Pd.cmp(p, ";") == 0) {
            result = "Fx";
        } else if (JRegex.match(p, "m/^(\\-|\\-\\-)$/")) {
            result = "Fg";
        } else if (JRegex.match(p, "m/^('|\"|``|'')$/")) {
            result = "Fe";
        } else if (Pd.cmp(p, "...") == 0) {
            result = "Fs";
        } else if (JRegex.match(p, "m/^(<<|«|“)/")) {
            result = "Fra";
        } else if (JRegex.match(p, "m/^(>>|»|”)/")) {
            result = "Frc";
        } else if (Pd.cmp(p, "%") == 0) {
            result = "Ft";
        } else if (JRegex.match(p, "m/^(\\/|\\\\)$/")) {
            result = "Fh";
        } else if (Pd.cmp(p, "(") == 0) {
            result = "Fpa";
        } else if (Pd.cmp(p, ")") == 0) {
            result = "Fpt";
        } else if (Pd.cmp(p, "¿") == 0) {
            result = "Fia";
        } else if (Pd.cmp(p, "?") == 0) {
            result = "Fit";
        } else if (Pd.cmp(p, "¡") == 0) {
            result = "Faa";
        } else if (Pd.cmp(p, "!") == 0) {
            result = "Fat";
        } else if (Pd.cmp(p, "[") == 0) {
            result = "Fca";
        } else if (Pd.cmp(p, "]") == 0) {
            result = "Fct";
        } else if (Pd.cmp(p, "{") == 0) {
            result = "Fla";
        } else if (Pd.cmp(p, "}") == 0) {
            result = "Flt";
        } else if (Pd.cmp(p, "…") == 0) {
            result = "Fz";
        }
        return result;
    }

    public String lowercase(Object... pd_argv) {
        String x;
        x = ((String) pd_argv[0]);
        x = perl.lc(x);
        x = JRegex.tr(x, "tr/ÁÉÍÓÚÇÑ/áéíóúçñ/");
        return x;
    }

    public String Trim(Object... pd_argv) {
        String x;
        x = ((String) pd_argv[0]);
        x = JRegex.s(x, "s/^[\\s]*//");
        x = JRegex.s(x, "s/[\\s]$//");
        return x;
    }
}
