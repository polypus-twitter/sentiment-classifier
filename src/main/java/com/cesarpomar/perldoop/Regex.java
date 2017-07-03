package com.cesarpomar.perldoop;

/**
 * Clase para definir los metodos relacionados con expresiones regulares
 * ejecutadas por perl
 *
 * @author César Pomar
 */
public class Regex {

    /**
     * Comprueba si una cadena cumple una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return True si coincide, False en otro caso
     */
    public static Boolean match(String cad, String regex) {
        regex = regex.replaceAll("\"", "\\\\\\\\\\\\\"");
        cad = cad.replaceAll("\"", "\\\\\\\\\\\\\"");
        StringBuilder sb = new StringBuilder(100);
        sb.append("perl -e ");
        sb.append('"');
        sb.append("print(\\\"").append(cad).append("\\\" =~ ").append(regex).append(");");
        sb.append('"');
        return !Pd.cmd(sb.toString()).isEmpty();
    }

    /**
     * Compreuba si una cadena cumple una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return True si coincide, False en otro caso
     */
    public static String[] matchAvc(String cad, String regex) {
        regex = regex.replaceAll("\"", "\\\\\\\\\\\\\"");
        cad = cad.replaceAll("\"", "\\\\\\\\\\\\\"");
        String separador = "@#1#@";
        StringBuilder sb = new StringBuilder(100);
        sb.append("perl -e ");
        sb.append('"');
        sb.append("print join(\\\"").append(separador).append("\\\",\\\"").append(cad).append("\\\" =~ ").append(regex).append(");");
        sb.append('"');
        return Pd.cmd(sb.toString()).split(separador);
    }

    /**
     * Realiza una substitucion mediante una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return Scadena
     */
    public static String s(String cad, String regex) {
        regex = regex.replaceAll("\"", "\\\\\\\\\\\\\"");
        cad = cad.replaceAll("\"", "\\\\\\\\\\\\\"");
        StringBuilder sb = new StringBuilder(100);
        sb.append("perl -e ");
        sb.append("\"");
        sb.append("$x=\\\"").append(cad).append("\\\";\n");
        sb.append("$x =~ ").append(regex).append(";\n");
        sb.append("print $x;");
        sb.append('"');
        String t = Pd.cmd(sb.toString());
        return t;
    }

    /**
     * Realiza una transliteracion mediante una expresion regular
     *
     * @param cad Cadena
     * @param regex Expresion regular
     * @return True si coincide, False en otro caso
     */
    public static String tr(String cad, String regex) {
        regex = regex.replaceAll("\"", "\\\\\\\\\\\\\"");
        cad = cad.replaceAll("\"", "\\\\\\\\\\\\\"");
        StringBuilder sb = new StringBuilder(100);
        sb.append("perl -e ");
        sb.append('"');
        sb.append("$x=\\\"").append(cad).append("\\\";\n");
        sb.append("$x =~ ").append(regex).append(";\n");
        sb.append("print $x;");
        sb.append('"');
        return Pd.cmd(sb.toString());
    }

}
