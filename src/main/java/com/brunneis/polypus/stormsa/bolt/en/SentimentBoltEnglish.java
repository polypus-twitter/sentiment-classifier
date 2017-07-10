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

import com.brunneis.polypus.stormsa.conf.Conf;
import com.brunneis.polypus.stormsa.dao.DigitalPostsDAO;
import com.brunneis.polypus.stormsa.dao.DigitalPostsSingletonFactoryDAO;
import com.brunneis.polypus.stormsa.spout.PolypusSpout;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import com.cesarpomar.perldoop.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentimentBoltEnglish extends BaseBasicBolt {

    // The map is filled with the setup of the bolt, but also modified during 
    // the execution, swaped by a deep copy at execute()
    private Map<String, Map<String, Double>> PriorProb = new HashPerl<String, Map<String, Double>>();
    private DigitalPostsDAO dpdao;

    // Only written to during the bolt setup
    private Map<String, String> Lex_contr = new HashPerl<String, String>();
    private Map<String, String> Lex = new HashPerl<String, String>();
    private Map<String, String> featFreq = new HashPerl<String, String>();
    private Map<String, String> ProbCat = new HashPerl<String, String>();
    private String light_words = "";
    private String quant_adj = "";
    // private String quant_noun = "";
    private String neg_noun = "";
    private String neg_verb = "";
    private Integer N = 0;

    private final String TRAIN_FILE = "input/en/model/sentiment/train_en";
    private final String LEX_FILE = "input/en/model/sentiment/lex_en";

    private final Logger LOG = LoggerFactory.getLogger(PolypusSpout.class);

    private Perl perl;

    @Override
    // train_en, lex_en load
    public void prepare(Map config, TopologyContext context) {
        dpdao = DigitalPostsSingletonFactoryDAO.getDigitalPostDAOinstance(Conf.OUTPUT_BUFFER_DB.value().CURRENT.value());
        perl = Perl.getInstance();

        List<String> train = new ArrayList<String>();
        List<String> lex = new ArrayList<String>();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(this.TRAIN_FILE)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                train.add(line);
            }
        } catch (Exception ex) {
            perl.die("Error reading the file: " + ex);
        }

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream(this.LEX_FILE)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // LOG.info(line);
                lex.add(line);
            }
        } catch (Exception ex) {
            perl.die("Error reading the file: " + ex);
        }

        Integer i = 0;
        for (String line : lex) {
            i++;
            // Ref<String> pd_f;
            line = perl.chomp(line);
            String word;
            String cat;
            {
                String[] pd_me = perl.split("\t", line);
                word = pd_me[0];
                cat = pd_me[1];
            };
            word = trim(word);
            cat = trim(cat);
            if (!perl.defined(Lex.get(word))) {
                Lex.put(word, "");
            }
            Lex.put(word, Lex.get(word) + cat + "|");
            if (!perl.defined(PriorProb.get(cat))) {
                PriorProb.put(cat, new HashPerl<String, Double>());
            }
            PriorProb.get(cat).put(word, 0.1);
        }

        for (String l : perl.keys(Lex)) {
            Integer positive = 0;
            Integer negative = 0;
            // Integer none = 0;
            String[] pols;
            // FIX NULL
            if (Lex.get(l) != null) {
                pols = perl.split("\\|", Lex.get(l));

                for (String p : pols) {
                    if ((Pd.cmp(p, "POSITIVE") == 0)) {
                        positive++;
                    }
                    if ((Pd.cmp(p, "NEGATIVE") == 0)) {
                        negative++;
                    }
                    if ((Pd.cmp(p, "NONE") == 0)) {
                        // none++;
                    }
                }
            }
            if (positive > negative) {
                Lex.put(l, "POSITIVE");
                Lex_contr.put(l, "NEGATIVE");
            } else if (negative > positive) {
                Lex.put(l, "NEGATIVE");
                Lex_contr.put(l, "POSITIVE");
            } else {
                perl.delete(Lex, l);
            }
        }

        Map<String, String> Pol = new HashPerl<String, String>();
        i = 0;
        for (String line : train) {
            String cat = "";
            String prob = "";
            String feat = "";
            String freq = "";
            i++;
            Ref<String> pd_f1;
            line = perl.chomp(line);
            if (i == 1) {
                {
                    String[] pd_me = (JRegex.matchAvc(line, "m/<number_of_docs>([0-9]*)</"));
                    N = Integer.parseInt(pd_me[0]);
                };
            } else if (JRegex.match(line, "m/<cat>/")) {
                String tmp;
                {
                    String[] pd_me = JRegex.matchAvc(line, "m/<cat>([^<]*)</");
                    tmp = pd_me[0];
                };
                {
                    String[] pd_me = perl.split(" ", tmp);
                    cat = pd_me[0];
                    prob = pd_me[1];
                };
                ProbCat.put(cat, prob);
            } else if (JRegex.match(line, "m/<list>light_words/")) {
                {
                    String[] pd_me = JRegex.matchAvc(line, "m/<list>light_words ([^<]*)</");
                    light_words = pd_me[0];
                };
            } else if (JRegex.match(line, "m/<list>quant_adj/")) {
                {
                    String[] pd_me = JRegex.matchAvc(line, "m/<list>quant_adj ([^<]*)</");
                    quant_adj = pd_me[0];
                };
                // } else if (JRegex.match(line, "m/<list>quant_noun/")) {
                // {
                // String[] pd_me = JRegex.matchAvc(line, "m/<list>quant_noun ([^<]*)</");
                // quant_noun = pd_me[0];
                // };
            } else if (JRegex.match(line, "m/<list>neg_noun/")) {
                {
                    String[] pd_me = JRegex.matchAvc(line, "m/<list>neg_noun ([^<]*)</");
                    neg_noun = pd_me[0];
                };
            } else if (JRegex.match(line, "m/<list>neg_verb/")) {
                {
                    String[] pd_me = JRegex.matchAvc(line, "m/<list>neg_verb ([^<]*)</");
                    neg_verb = pd_me[0];
                };
            } else if (!JRegex.match(line, "m/<cat>/") && !JRegex.match(line, "m/<list>/")) {
                {
                    String[] pd_me = perl.split(" ", line);
                    feat = pd_me[0];
                    cat = pd_me[1];
                    prob = pd_me[2];
                    freq = pd_me[3];
                };
            }
            if ((Pd.eval(cat) && !Pd.eval(PriorProb.get(cat).get(feat)))) {
                PriorProb.get(cat).put(feat, Double.parseDouble(prob));
            }
            featFreq.put(feat, freq);
            Pol.put(feat, cat);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        // This bolt does not emit anything
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        Map<String, Integer> Compound = new HashPerl<String, Integer>();
        Map<String, Map<String, Double>> PriorProbWritable
                = CloneHelper.getMapCopyKStringVMapKStringVDouble(this.PriorProb);

        ArrayList<String> text = (ArrayList<String>) tuple.getValueByField("tagger");
        String rowkey = (String) tuple.getValueByField("rowkey");

        String previous = "";
        String previous2 = "";
        Integer LEX = 0;
        Integer POS_EMOT = 0;
        Integer NEG_EMOT = 0;
        List<String> A = new PerlList<String>();
        String line;
        for (String pd_i : text) {
            line = pd_i;
            String token = "";
            String lemma = "";
            String tag = "";
            String lemma_orig = "";
            if (!JRegex.match(line, "m/\\w /")) {
                continue;
            }
            String[] pd_me = perl.split(" ", line);
            if (pd_me.length < 3) {
                continue;
            }
            token = pd_me[0];
            lemma = pd_me[1];
            tag = pd_me[2];
            if (Pd.cmp(token, "EMOT_POS") == 0) {
                POS_EMOT++;
            } else if (Pd.cmp(token, "EMOT_NEG") == 0) {
                NEG_EMOT++;
            }
            if (JRegex.match(tag, "m/^([FI])/")) {
                previous = "";
            } else if (JRegex.match(tag, "m/^(V|N|AQ|R|JJ)/") && !JRegex.match(lemma, "m/^(" + light_words + ")$/")) {
                if (Pd.eval(Lex.get(lemma))) {
                    LEX++;
                    lemma_orig = lemma;
                }
                if (Pd.eval(Lex.get(lemma)) && JRegex.match(tag, "m/^(AQ|JJ)/") && JRegex.match(previous, "m/^(" + quant_adj + ")$/")) {
                    lemma = previous + "_" + lemma;
                    if (JRegex.match(previous2, "m/^(" + neg_noun + ")$/") && JRegex.match(lemma, "m/^$quant_adj_/")) {
                        lemma = previous2 + "_" + lemma;
                        if (Pd.eval((Lex.get(lemma_orig)))) {
                            PriorProbWritable.get(Lex_contr.get(lemma_orig)).put(lemma, PriorProbWritable.get(Lex.get(lemma_orig)).get(lemma_orig));
                        }
                        if (Pd.eval((Lex.get(lemma_orig)))) {
                            PriorProbWritable.get(Lex.get(lemma_orig)).put(lemma, 0d);
                        }
                        perl.push(A, lemma);
                        if (perl.defined(Compound.get(lemma))) {
                            Compound.put(lemma, 0);
                        }
                        Compound.put(lemma, Compound.get(lemma) + 1);
                    } else {
                        if (Pd.eval((Lex.get(lemma_orig)))) {
                            PriorProbWritable.get(Lex.get(lemma_orig)).put(lemma, PriorProbWritable.get(Lex.get(lemma_orig)).get(lemma_orig));
                        }
                        if (!perl.defined(Compound.get(lemma))) {
                            Compound.put(lemma, 0);
                        }
                        Compound.put(lemma, Compound.get(lemma) + 1);
                        perl.push(A, lemma);
                    }
                } else if (Pd.eval(Lex.get(lemma)) && JRegex.match(tag, "m/(^N|^AQ|^JJ)/") && JRegex.match(previous, "m/^(" + neg_noun + ")$/")) {
                    lemma = previous + "_" + lemma;
                    if (Pd.eval((Lex.get(lemma_orig)))) {
                        PriorProbWritable.get(Lex_contr.get(lemma_orig)).put(lemma, PriorProbWritable.get(Lex.get(lemma_orig)).get(lemma_orig));
                    }
                    if (Pd.eval((Lex.get(lemma_orig)))) {
                        PriorProbWritable.get(Lex.get(lemma_orig)).put(lemma, 0d);
                    }
                    if (!perl.defined(Compound.get(lemma))) {
                        Compound.put(lemma, 0);
                    }
                    Compound.put(lemma, Compound.get(lemma) + 1);
                    perl.push(A, lemma);
                } else if (Pd.eval(Lex.get(lemma)) && JRegex.match(tag, "m/^V/") && JRegex.match(previous, "m/^(" + neg_verb + ")$/")) {
                    lemma = previous + "_" + lemma;
                    if (Pd.eval((Lex.get(lemma_orig)))) {
                        PriorProbWritable.get(Lex_contr.get(lemma_orig)).put(lemma, PriorProbWritable.get(Lex.get(lemma_orig)).get(lemma_orig));
                    }
                    if (Pd.eval((Lex.get(lemma_orig)))) {
                        PriorProbWritable.get(Lex.get(lemma_orig)).put(lemma, 0d);
                    }
                    if (!perl.defined(Compound.get(lemma))) {
                        Compound.put(lemma, 0);
                    }
                    Compound.put(lemma, Compound.get(lemma) + 1);
                    perl.push(A, lemma);
                } else {
                    perl.push(A, lemma);
                }
                previous2 = previous;
                previous = lemma;
            }
        }
        if (POS_EMOT > NEG_EMOT) {
            DigitalPostsSingletonFactoryDAO.getDigitalPostDAOinstance(Conf.OUTPUT_BUFFER_DB.value().CURRENT.value()).writeSentiment(rowkey, 1);
            return;
        } else if (POS_EMOT < NEG_EMOT) {
            DigitalPostsSingletonFactoryDAO.getDigitalPostDAOinstance(Conf.OUTPUT_BUFFER_DB.value().CURRENT.value()).writeSentiment(rowkey, -1);
            return;
        } else if (!Pd.eval(LEX)) {
            DigitalPostsSingletonFactoryDAO.getDigitalPostDAOinstance(Conf.OUTPUT_BUFFER_DB.value().CURRENT.value()).writeSentiment(rowkey, 0);
            return;
        }
        Double smooth = Pd.toDouble(1 / (float) N);
        Integer Normalizer = 0;
        Map<String, Double> PostProb = new HashPerl<String, Double>();
        Map<String, Boolean> found = new HashPerl<String, Boolean>();
        String cat;
        for (String pd_i1 : perl.keys(PriorProbWritable)) {
            cat = pd_i1;
            if (!Pd.eval(cat)) {
                continue;
            }
            PostProb.put(cat, Double.parseDouble(ProbCat.get(cat)));
            found.put(cat, false);
            String word;
            for (String pd_i2 : A) {
                word = pd_i2;
                if (!Pd.eval(featFreq.get(word)) && !Pd.eval(Compound.get(word)) && !Pd.eval(Lex.get(word))) {
                    continue;
                }
                if (!perl.defined(PriorProbWritable.get(cat))) {
                    PriorProbWritable.put(cat, new HashPerl<String, Double>());
                }
                if (!perl.defined(PriorProbWritable.get(cat).get(word))) {
                    PriorProbWritable.get(cat).put(word, 0d);
                }
                if ((PriorProbWritable.get(cat).get(word) == 0)) {
                    PriorProbWritable.get(cat).put(word, smooth);
                }
                found.put(cat, true);
                PostProb.put(cat, PostProb.get(cat) * PriorProbWritable.get(cat).get(word));
            }
            PostProb.put(cat, PostProb.get(cat) * Double.parseDouble(ProbCat.get(cat)));
            if ((!found.get(cat))) {
                PostProb.put(cat, 0d);
            }
            Normalizer = Pd.toInteger(Normalizer + PostProb.get(cat));
        }
        Boolean found2 = false;
        String c;
        for (String pd_i3 : perl.keys(PostProb)) {
            c = pd_i3;
            if (Pd.eval((Normalizer))) {
                PostProb.put(c, PostProb.get(c) / Normalizer);
            }
            if ((found.get(c))) {
                found2 = true;
            }
        }
        if (!found2) {
            dpdao.writeSentiment(rowkey, 0);
            return;
        }

        Double np = PostProb.get("NEGATIVE");
        Double pp = PostProb.get("POSITIVE");
        if (pp > np) {
            dpdao.writeSentiment(rowkey, 1);
        } else if (np > pp) {
            dpdao.writeSentiment(rowkey, -1);
        } else {
            dpdao.writeSentiment(rowkey, 0);
        }
    }

    public String trim(Object... pd_argv) {
        String str;
        str = ((String) pd_argv[0]);
        str = JRegex.s(str, "s/^\\s*(.*\\S)\\s*$/$1/");
        return str;
    }
}
