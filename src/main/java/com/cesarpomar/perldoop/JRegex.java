package com.cesarpomar.perldoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import jregex.MatchResult;
import jregex.Matcher;
import jregex.Pattern;
import jregex.PerlSubstitution;
import jregex.Replacer;
import jregex.Substitution;
import jregex.TextBuffer;
import org.jtr.transliterate.CharacterParseException;
import org.jtr.transliterate.CharacterReplacer;
import org.jtr.transliterate.Perl5Parser;

/**
 * Clase para definir los metodos relacionados con expresiones regulares
 * ejecutadas mediante la libreria Jregex
 *
 * @author César Pomar
 */
public class JRegex {

    /**
     * Comprueba si una cadena cumple una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return True si coincide, False en otro caso
     */
    public static Boolean match(String cad, String regex) {
        int options = regex.lastIndexOf('/');
        Pattern pattern = new Pattern(regex.substring(2, options), regex.substring(options + 1));
        return pattern.matcher(cad).find();
    }

    /**
     * Compreuba si una cadena cumple una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return True si coincide, False en otro caso
     */
    public static String[] matchAvc(String cad, String regex) {
        int options = regex.lastIndexOf('/');
        String soptions = regex.substring(options + 1).replace("o", "");
        if (soptions.indexOf('g') != -1) {
            soptions = regex.substring(options + 1).replace("g", "");
            Pattern pattern = new Pattern(regex.substring(2, options), soptions);
            Matcher matcher = pattern.matcher(cad);
            List<String> list = new ArrayList<>(100);
            while (matcher.find()) {
                String[] groups = matcher.groups();
                list.addAll(Arrays.asList(groups).subList(1, groups.length));
            }
            return list.toArray(new String[list.size()]);
        } else {
            Pattern pattern = new Pattern(regex.substring(2, options), soptions);
            Matcher matcher = pattern.matcher(cad);
            if (matcher.find()) {
                String[] groups = matcher.groups();
                return Arrays.copyOfRange(groups, 1, groups.length);
            }
        }
        return new String[0];
    }

    /**
     * Realiza una substitucion mediante una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return Scadena
     */
    public static String s(String cad, String regex) {
        int options = regex.lastIndexOf("/");
        int remplace = regex.replaceAll("\\\\/", "..").indexOf('/', 2);
        String soptions = regex.substring(options + 1).replace("o", "");
        if (soptions.indexOf('g') != -1) {
            soptions = regex.substring(options + 1).replace("g", "");
            Pattern pattern = new Pattern(regex.substring(2, remplace), soptions);
            Replacer replacer = pattern.replacer(regex.substring(remplace + 1, options));
            return replacer.replace(cad);
        } else {
            Pattern pattern = new Pattern(regex.substring(2, remplace), soptions);
            Substitution subs = new PerlSubstitution(regex.substring(remplace + 1, options)) {
                private boolean first = true;

                @Override
                public void appendSubstitution(MatchResult match, TextBuffer dest) {
                    if (first) {
                        super.appendSubstitution(match, dest);
                        first = false;
                    } else {
                        dest.append(match.toString());
                    }
                }
            };
            Replacer replacer = pattern.replacer(subs);
            return replacer.replace(cad);
        }
    }

    /**
     * Realiza una transliteracion mediante una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return True si coincide, False en otro caso
     */
    public static String tr(String cad, String regex) {
        try {
            CharacterReplacer replacer = Perl5Parser.makeReplacer(regex);
            return replacer.doReplacement(cad);
        } catch (CharacterParseException ex) {
            Logger.getLogger(JRegex.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }

    /**
     * Expande los generadoes dentro de la cadena
     *
     * @param cad Cadena con generadores
     * @return Cadena expandida
     */
    public static String generadores(String cad) {
        return null;
    }

}
