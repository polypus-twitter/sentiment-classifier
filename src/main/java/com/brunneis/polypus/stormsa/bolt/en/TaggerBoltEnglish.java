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

public class TaggerBoltEnglish extends BaseBasicBolt {

    // Only written to during the setup of the bolt
    private String w;
    private String Border;
    private String[] cat_open;
    private Integer N;
    private Map<String, String> featFreq;

    // The maps are filled with the setup of the bolt, but also modified during 
    // the execution, swaped by deep copies at execute() and classif() respectively
    private Map<String, Map<String, Double>> PriorProb;
    private Map<String, Double> ProbCat;

    private final String TRAIN_FILE = "input/en/model/tagger/train-en";

    private Perl perl;

    @Override
    public void prepare(Map config, TopologyContext context) {
        perl = Perl.getInstance();

        List<String> train = new ArrayList<String>();

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

        w = String.valueOf(1);
        Border = "(Fp|<blank>)";
        cat_open = new Ref<>(new String[]{"NN", "NNP", "VB", "RB", "JJ"}).get();
        N = 0;
        PriorProb = new HashPerl<String, Map<String, Double>>();
        ProbCat = new HashPerl<String, Double>();
        featFreq = new HashPerl<String, String>();

        Integer count = 0;
        for (String line : train) {
            String cat = "";
            String prob = "0";
            String feat = "";
            String freq = "";
            count++;
            line = perl.chomp(line);
            if (count == 1) {
                {
                    String[] pd_me = (JRegex.matchAvc(line, "m/<number_of_docs>([0-9]*)</"));
                    N = Integer.parseInt(pd_me[0]);
                };
                continue;
            }
            if (JRegex.match(line, "m/<cat>/")) {
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
                ProbCat.put(cat, Double.parseDouble(prob));
            }
            if ((!JRegex.match(line, "m/<cat>/"))) {
                {
                    String[] pd_me = perl.split(" ", line);
                    feat = pd_me[0];
                    cat = pd_me[1];
                    prob = pd_me[2];
                    freq = pd_me[3];
                };
            }
            if (Pd.eval(cat)) {
                if (!perl.defined(PriorProb.get(cat))) {
                    PriorProb.put(cat, new HashPerl<String, Double>());
                }
                PriorProb.get(cat).put(feat, Double.parseDouble(prob));
            }
            featFreq.put(feat, freq);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declareStream("lang_en", new Fields("tagger", "rowkey"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {

        Map<String, Map<String, Double>> PriorProbWritable
                = CloneHelper.getMapCopyKStringVMapKStringVDouble(this.PriorProb);

        Map<String, Map<String, String>> TagHash = new HashPerl<String, Map<String, String>>();
        Map<String, Map<String, String>> LemaHash = new HashPerl<String, Map<String, String>>();

        ArrayList<String> text = (ArrayList<String>) tuple.getValueByField("ner");
        List<String> saida = new PerlList<String>();

        String result;
        Integer pos = 0;
        // Integer s = 0;
        String tag;
        Boolean[] noamb;

        List<String> Feat = new PerlList<String>();
        List<String> Cat = new PerlList<String>();
        String amb;
        noamb = new Boolean[text.size()];

        // Cannot be global, should be passed to classif() as an argument
        // due to the shared resources between the instances of the bolt
        String[] unk = new String[text.size()];

        String[] Token = new String[text.size()];
        String[] Tag = new String[text.size()];
        String[] Lema = new String[text.size()];

        for (String line : text) {
            if (!JRegex.match(line, "m/\\w/") || JRegex.match(line, "m/^[ ]$/")) {
                continue;
            }
            String[] entry = perl.split(" ", line);
            if ((entry.length <= 2) || (!JRegex.match(entry[2], "m/^" + Border + "$/"))) {
                Token[pos] = entry[0];
                Integer i = 1;
                while (i < entry.length) {
                    Lema[pos] = entry[i];
                    i++;
                    tag = entry[i];
                    if (JRegex.match(tag, "m/^V/") || JRegex.match(tag, "m/^NNP/") || JRegex.match(tag, "m/^PRP/")) {
                        {
                            String[] pd_me = JRegex.matchAvc(tag, "m/([A-Z][A-Z][A-Z]?[A-Za-z\\$]?)/");
                            Tag[pos] = pd_me[0];
                        };
                    } else {
                        {
                            String[] pd_me = JRegex.matchAvc(tag, "m/([A-Z][A-Za-z\\$0-9]?)/");
                            Tag[pos] = pd_me[0];
                        };
                    }
                    if (!perl.defined(TagHash.get(String.valueOf(pos)))) {
                        TagHash.put(String.valueOf(pos), new HashPerl<String, String>());
                    }
                    if (!perl.defined(LemaHash.get(String.valueOf(pos)))) {
                        LemaHash.put(String.valueOf(pos), new HashPerl<String, String>());
                    }
                    TagHash.get(String.valueOf(pos)).put(Tag[pos], tag);
                    LemaHash.get(String.valueOf(pos)).put(tag, Lema[pos]);
                    if (entry.length == 3 && Pd.cmp(entry[2], "UNK") != 0) {
                        noamb[pos] = true;
                    }
                    if (entry.length == 3 && Pd.cmp(entry[2], "UNK") == 0) {
                        unk[pos] = String.valueOf(1);
                    }
                    i++;
                }
                if (Pd.eval(unk[pos]) && JRegex.match(Token[pos], "m/[\\w]+ly$/")) {
                    String cat = "RB";
                    TagHash.get(String.valueOf(pos)).put(cat, cat);
                    LemaHash.get(String.valueOf(pos)).put(cat, Lema[pos]);
                    unk[pos] = String.valueOf(0);
                } else if (Pd.eval(unk[pos]) && JRegex.match(Token[pos], "m/[\\w]+ing$/")) {
                    String cat = "VB";
                    TagHash.get(String.valueOf(pos)).put(cat, "VBG");
                    LemaHash.get(String.valueOf(pos)).put(cat, Lema[pos]);
                    unk[pos] = String.valueOf(0);
                }
                pos++;
            } else {
                // s++;
                entry = perl.split(" ", line);
                String last_entry = entry[0] + " ";
                last_entry = last_entry + entry[1];
                last_entry = last_entry + " ";
                last_entry = last_entry + entry[2];
                for (pos = 0; (pos < Tag.length) && (perl.defined(Token[pos])); pos++) {
                    if ((perl.defined(noamb[pos])) && (noamb[pos])) {
                        if (!perl.defined(TagHash.get(String.valueOf(pos)))) {
                            TagHash.put(String.valueOf(pos), new HashPerl<String, String>());
                        }
                        result = Token[pos] + " ";
                        result = result + Lema[pos];
                        result = result + " ";
                        result = result + TagHash.get(String.valueOf(pos)).get(Tag[pos]);
                        perl.push(saida, result);
                    } else {
                        if ((!perl.defined(unk[pos])) || (!Pd.eval(unk[pos]))) {
                            if (!perl.defined(TagHash.get(String.valueOf(pos)))) {
                                TagHash.put(String.valueOf(pos), new HashPerl<String, String>());
                            }
                            for (String cat : perl.keys(TagHash.get(String.valueOf(pos)))) {
                                perl.push(Cat, cat);
                            }
                        } else {
                            for (String cat : cat_open) {
                                perl.push(Cat, cat);
                            }
                        }
                        Integer k = 0;
                        if (pos == 0) {
                            for (Integer j = 1; j <= Double.parseDouble(w); j++) {
                                if (((j < noamb.length) && (perl.defined(noamb[j]))) && (noamb[j])) {
                                    amb = "noamb";
                                } else {
                                    amb = "amb";
                                }
                                if (!perl.defined(TagHash.get(String.valueOf(j)))) {
                                    TagHash.put(String.valueOf(j), new HashPerl<String, String>());
                                }
                                for (String feat : perl.keys(TagHash.get(String.valueOf(j)))) {
                                    if (!Pd.eval(feat)) {
                                        continue;
                                    }
                                    k++;
                                    String featAux = feat;
                                    feat = amb + "_";
                                    feat = feat + k;
                                    feat = feat + "_";
                                    feat = feat + w;
                                    feat = feat + "_R_";
                                    feat = feat + featAux;
                                    String new_token = lowercase(Token[pos]);
                                    String featL = feat + "_";
                                    featL = featL + new_token;
                                    perl.push(Feat, feat);
                                    perl.push(Feat, featL);
                                }
                                String feat = "noamb" + "_";
                                feat = feat + "1";
                                feat = feat + "_";
                                feat = feat + w;
                                feat = feat + "_L_";
                                feat = feat + "BEGIN";
                                perl.push(Feat, feat);
                                k = 0;
                                amb = "";
                            }
                        } else if (pos == Tag.length - 1) {
                            Integer end = Pd.toInteger(Tag.length - 1 - Double.parseDouble(w));
                            for (Integer j = Tag.length - 2; j >= end; j--) {
                                if (((j < noamb.length) && (perl.defined(noamb[j]))) && (noamb[j])) {
                                    amb = "noamb";
                                } else {
                                    amb = "amb";
                                }
                                if (Pd.eval(TagHash.get(String.valueOf(j)))) {
                                    if (!perl.defined(TagHash.get(String.valueOf(j)))) {
                                        TagHash.put(String.valueOf(j), new HashPerl<String, String>());
                                    }
                                    for (String feat : perl.keys(TagHash.get(String.valueOf(j)))) {
                                        if (!Pd.eval(feat)) {
                                            continue;
                                        }
                                        k++;
                                        String featAux = feat;
                                        feat = amb + "_";
                                        feat = feat + k;
                                        feat = feat + "_";
                                        feat = feat + w;
                                        feat = feat + "_L_";
                                        feat = feat + featAux;
                                        String new_token = Token[pos];
                                        String featL = feat + "_" + new_token;
                                        perl.push(Feat, feat);
                                        perl.push(Feat, featL);
                                    }
                                }
                                String feat = "noamb" + "_";
                                feat = feat + "1";
                                feat = feat + "_";
                                feat = feat + w;
                                feat = feat + "_R_";
                                feat = feat + "END";
                                perl.push(Feat, feat);
                                k = 0;
                                amb = "";
                            }
                        } else {
                            Integer end = Pd.toInteger(pos + Double.parseDouble(w));
                            for (Integer j = pos + 1; j <= end; j++) {
                                if (((j < noamb.length) && (perl.defined(noamb[j]))) && (noamb[j])) {
                                    amb = "noamb";
                                } else {
                                    amb = "amb";
                                }
                                if (!perl.defined(TagHash.get(String.valueOf(j)))) {
                                    TagHash.put(String.valueOf(j), new HashPerl<String, String>());
                                }
                                for (String feat : perl.keys(TagHash.get(String.valueOf(j)))) {
                                    if (!Pd.eval(feat)) {
                                        continue;
                                    }
                                    k++;
                                    String featAux = feat;
                                    feat = amb + "_";
                                    feat = feat + k;
                                    feat = feat + "_";
                                    feat = feat + w;
                                    feat = feat + "_R_";
                                    feat = feat + featAux;
                                    String new_token = Token[pos];
                                    String featL = feat + "_" + new_token;
                                    perl.push(Feat, feat);
                                    perl.push(Feat, featL);
                                }
                                k = 0;
                                amb = "";
                            }
                            end = Pd.toInteger(pos - Double.parseDouble(w));
                            for (Integer j = pos - 1; j >= end; j--) {
                                if (((j < noamb.length) && (perl.defined(noamb[j]))) && (noamb[j])) {
                                    amb = "noamb";
                                } else {
                                    amb = "amb";
                                }
                                if (!perl.defined(TagHash.get(String.valueOf(j)))) {
                                    TagHash.put(String.valueOf(j), new HashPerl<String, String>());
                                }
                                for (String feat : perl.keys(TagHash.get(String.valueOf(j)))) {
                                    if (!Pd.eval(feat)) {
                                        continue;
                                    }
                                    k++;
                                    String featAux = feat;
                                    feat = amb + "_";
                                    feat = feat + k;
                                    feat = feat + "_";
                                    feat = feat + w;
                                    feat = feat + "_L_";
                                    feat = feat + featAux;
                                    String new_token = Token[pos];
                                    String featL = feat + "_" + new_token;
                                    perl.push(Feat, feat);
                                    perl.push(Feat, featL);
                                }
                                k = 0;
                                amb = "";
                            }
                        }
                        tag = classif(
                                pos, new Ref<List<String>>(Feat),
                                new Ref<List<String>>(Cat),
                                new Ref<String[]>(unk),
                                new Ref<Map<String, Map<String, Double>>>(PriorProbWritable)
                        );

                        if (Pd.eval(unk[pos])) {
                            Tag[pos] = tag;
                            if (!perl.defined(LemaHash.get(String.valueOf(pos)))) {
                                LemaHash.put(String.valueOf(pos), new HashPerl<String, String>());
                            }
                            LemaHash.get(String.valueOf(pos)).put(Tag[pos], Token[pos]);
                        } else {
                            if (!perl.defined(TagHash.get(String.valueOf(pos)))) {
                                TagHash.put(String.valueOf(pos), new HashPerl<String, String>());
                            }
                            Tag[pos] = TagHash.get(String.valueOf(pos)).get(tag);
                        }
                        if (!perl.defined(LemaHash.get(String.valueOf(pos)))) {
                            LemaHash.put(String.valueOf(pos), new HashPerl<String, String>());
                        }
                        if (!perl.defined(LemaHash.get(String.valueOf(pos)).get(Tag[pos]))) {
                            LemaHash.get(String.valueOf(pos)).put(Tag[pos], "");
                        }
                        result = Token[pos] + " ";
                        result = result + LemaHash.get(String.valueOf(pos)).get(Tag[pos]);
                        result = result + " ";
                        result = result + Tag[pos];
                        perl.push(saida, result);
                        for (String t : perl.keys(TagHash.get(String.valueOf(pos)))) {
                            if (Pd.cmp(t, tag) != 0) {
                                perl.delete(TagHash.get(String.valueOf(pos)), t);
                            }
                        }
                        Feat = new PerlList<String>();
                        Cat = new PerlList<String>();
                    }
                }
                result = "" + last_entry;
                result = result + "";
                perl.push(saida, result);
                TagHash = new HashPerl<String, Map<String, String>>();
                LemaHash = new HashPerl<String, Map<String, String>>();
                Tag = new String[text.size()];
                Token = new String[text.size()];
                Lema = new String[text.size()];
                noamb = new Boolean[text.size()];
                unk = new String[text.size()];
                pos = 0;
            }
        }

        ArrayList<String> output = (ArrayList<String>) saida;
        boc.emit("lang_en", new Values(output, tuple.getValueByField("rowkey")));
    }

    public Boolean rules_neg(Object... pd_argv) {
        String cat;
        String feat;
        cat = ((String) pd_argv[0]);
        feat = ((String) pd_argv[1]);
        Boolean result = false;
        if (JRegex.match(cat, "m/^PRP/") && (Pd.cmp(feat, "L_VB_that") == 0 || Pd.cmp(feat, "L_VBd_that") == 0)) {
            result = true;
        } else if (JRegex.match(cat, "m/^DT/") && (Pd.cmp(feat, "R_END") == 0 || Pd.cmp(feat, "R_IN") == 0 || Pd.cmp(feat, "R_CC") == 0)) {
            result = true;
        }
        return result;
    }

    public String classif(Object... pd_argv) {
        Integer pos;
        Ref<List<String>> refF;
        Ref<List<String>> refC;
        pos = ((Integer) pd_argv[0]);
        refF = ((Ref<List<String>>) pd_argv[1]);
        refC = ((Ref<List<String>>) pd_argv[2]);
        List<String> F = refF.get();
        List<String> C = refC.get();

        Map<String, Double> ProbCatWritable
                = CloneHelper.getMapCopyKStringVDouble(ProbCat);

        // unk array is now passed as an argument
        String[] unk = ((Ref<String[]>) pd_argv[3]).get();

        // PriorProb map is now passed as an argument
        Map<String, Map<String, Double>> PriorProbWritable
                = ((Ref<Map<String, Map<String, Double>>>) pd_argv[4]).get();

        String result = "";
        Map<String, Boolean> found = new HashPerl<String, Boolean>();
        Double smooth = Pd.toDouble(1 / (float) N);
        // String cat_restr = "";
        String feat_restr = "";
        // String n = "";
        String cat = "";
        String feat = "";
        Map<String, Double> PostProb = new HashPerl<String, Double>();
        Map<String, Integer> count = new HashPerl<String, Integer>();
        for (String pd_i : C) {
            cat = pd_i;
            if (!Pd.eval(cat)) {
                continue;
            }
            if (!(perl.defined(count.get(cat)))) {
                count.put(cat, 0);
            }
            count.put(cat, count.get(cat) + 1);
            if (!perl.defined(ProbCatWritable.get(cat))) {
                ProbCatWritable.put(cat, 0d);
            }
            PostProb.put(cat, ProbCatWritable.get(cat));
            found.put(cat, false);
            for (String pd_i1 : F) {
                feat_restr = pd_i1;
                {
                    String[] pd_me = JRegex.matchAvc(feat_restr, "m/^[a-z]+_[0-9]+_[0-9]_([RL]_[^ ]+)/");
                    feat = pd_me[0];
                };
                if ((!perl.defined(featFreq.get(feat))) || (!Pd.eval(featFreq.get(feat)))) {
                    continue;
                }
                if (!perl.defined(PriorProbWritable.get(cat))) {
                    PriorProbWritable.put(cat, new HashPerl<String, Double>());
                }
                if (!perl.defined(PriorProbWritable.get(cat).get(feat))) {
                    PriorProbWritable.get(cat).put(feat, 0d);
                }
                if ((PriorProbWritable.get(cat).get(feat) == 0)) {
                    PriorProbWritable.get(cat).put(feat, smooth);
                }
                if (rules_neg(cat, feat)) {
                    PriorProbWritable.get(cat).put(feat, 0d);
                } else if (!Pd.eval(unk[pos]) && rules_pos(cat, feat)) {
                    PriorProbWritable.get(cat).put(feat, 1d);
                }
                found.put(cat, true);
                if (!perl.defined(PostProb.get(cat))) {
                    PostProb.put(cat, 0d);
                }
                if (!perl.defined(PriorProbWritable.get(cat))) {
                    PriorProbWritable.put(cat, new HashPerl<String, Double>());
                }
                PostProb.put(cat, PostProb.get(cat) * PriorProbWritable.get(cat).get(feat));
            }
            if (!perl.defined(PostProb.get(cat))) {
                PostProb.put(cat, 0d);
            }
            if (!perl.defined(ProbCatWritable.get(cat))) {
                ProbCatWritable.put(cat, 0d);
            }
            PostProb.put(cat, PostProb.get(cat) * ProbCatWritable.get(cat));
            if ((!found.get(cat))) {
                PostProb.put(cat, 0d);
            }
        }
        Boolean First = false;
        for (String c : perl.sort(perl.keys(PostProb), (String a, String b) -> {
            return Pd.cmp(PostProb.get(b), PostProb.get(a));
        })) {
            if (!First) {
                // Double score = PostProb.get(c);
                result = "" + c + "";
                First = true;
            }
        }
        return result;
    }

    public Boolean rules_pos(Object... pd_argv) {
        // String cat = ((String) pd_argv[0]);
        // String feat = ((String) pd_argv[1]);
        Boolean result = false;
        return result;
    }

    public String lowercase(Object... pd_argv) {
        String x;
        x = ((String) pd_argv[0]);
        x = perl.lc(x);
        x = JRegex.tr(x, "tr/ÁÉÍÓÚÇÑ/áéíóúçñ/");
        return x;
    }

}
