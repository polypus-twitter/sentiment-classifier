package com.cesarpomar.perldoop;

import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import perldoop.util.Sequence;

/**
 * Clase para imitar comportamientos nativos de perl
 *
 * @author César Pomar
 */
public class Pd {

    /**
     * Convierte un numero a entero
     *
     * @param n Numero
     * @return Entero
     */
    public static Integer toInteger(Number n) {
        return n.intValue();
    }

    /**
     * Convierte un numero a long
     *
     * @param n Numero
     * @return Long
     */
    public static Long toLong(Number n) {
        return n.longValue();
    }

    /**
     * Convierte un numero a float
     *
     * @param n Numero
     * @return Float
     */
    public static Float toFloat(Number n) {
        return n.floatValue();
    }

    /**
     * Convierte un numero a double
     *
     * @param n Numero
     * @return Double
     */
    public static Double toDouble(Number n) {
        return n.doubleValue();
    }

    /**
     * Compara dos expresiones
     *
     * @param exp1 expresion 1
     * @param exp2 expresion 2
     * @return 0 si son iguales, 1 mayor y -1 menor
     */
    public static Integer cmp(Comparable exp1, Comparable exp2) {
        if (exp1 == null) {
            if (exp2 == null) {
                return 0;
            } else {
                return -1;
            }
        } else if (exp2 == null) {
            return 1;
        } else {
            int cmp = exp1.compareTo(exp2);
            if (cmp == 0) {
                return 0;
            } else if (cmp > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    /**
     * Calcula el tamaño en funcion del objeto pasado
     *
     * @param obj Objeto
     * @return tamaño
     */
    public static int len(Object obj) {
        if (obj == null) {
            return 1;
        } else if (obj instanceof Object[]) {
            return ((Object[]) obj).length;
        }
        return 1;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Boolean exp) {
        return exp != null && exp;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Integer exp) {
        return exp != null && exp != 0;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Long exp) {
        return exp != null && exp != 0;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Float exp) {
        return exp != null && exp != 0;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Double exp) {
        return exp != null && exp != 0;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(String exp) {
        return exp != null && !exp.isEmpty() && !exp.equals("0");
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Object[] exp) {
        return exp != null && exp.length != 0;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(List exp) {
        return exp != null && !exp.isEmpty();
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Map exp) {
        return exp != null && !exp.isEmpty();
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Ref exp) {
        return exp != null;
    }

    /**
     * Evalua una expresion
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(PerlFile exp) {
        return exp != null;
    }

    /**
     * Evalua una expresion generica
     *
     * @param exp Expresion
     * @return True si es cierto, False en otro caso
     */
    public static Boolean eval(Object exp) {
        switch (exp.getClass().getSimpleName()) {
            case "Boolean":
                return eval((Boolean) exp);
            case "Integer":
                return eval((Integer) exp);
            case "Long":
                return eval((Long) exp);
            case "Float":
                return eval((Float) exp);
            case "Double":
                return eval((Double) exp);
            case "String":
                return eval((String) exp);
            case "PerlList":
                return eval((List) exp);
            case "HashPerl":
                return eval((Map) exp);
            case "Ref":
                return eval((Ref) exp);
            case "PerlFile":
                return eval((PerlFile) exp);
            default:
                if (exp.getClass().isArray()) {
                    return eval((Object[]) exp);
                }
        }
        return null;
    }

    /**
     * Repite una cadena
     *
     * @param cad Cadena a repetir
     * @param num Numero de repeticiones
     * @return Cadena final
     */
    public static String repeat(String cad, Integer num) {
        StringBuilder r = new StringBuilder(cad.length() * num + 1);
        for (int i = 0; i < num; i++) {
            r.append(cad);
        }
        return r.toString();
    }

    /**
     * Realiza la operacion and sobre tipos no booleanos
     *
     * @param <T> Tipo
     * @param exp1 Primera expresion
     * @param exp2 Segunda expresion
     * @return Si exp1 es False retorna exp1 en caso contrario exp2
     */
    public static <T> T and(T exp1, T exp2) {
        return (eval(exp1)) ? exp2 : exp1;
    }

    /**
     * Realiza la operacion or sobre tipos no booleanos
     *
     * @param <T> Tipo
     * @param exp1 Primera expresion
     * @param exp2 Segunda expresion
     * @return Si exp1 es True retorna exp1 en caso contrario exp2
     */
    public static <T> T or(T exp1, T exp2) {
        return (eval(exp1)) ? exp1 : exp2;
    }

    /**
     * Realiza la operacion logica XOR
     *
     * @param exp1 Primera expresion
     * @param exp2 Segunda expresion
     * @return Si solo una de las expresiones es cierta retorna True, false en
     * cualquier otro caso.
     */
    public static Boolean xor(Boolean exp1, Boolean exp2) {
        return (exp1 && !exp2) || (!exp1 && exp2);
    }

    /**
     * Metodo auxiliar para poder ejecutar una asignacion despues de ejecutar un
     * metodo.
     *
     * @param <T> Tipo del valor devuelto
     * @param f Valor devuelto por la funcion
     * @param e Valor de actulizaciones de las variables
     * @return valor de la funcion
     */
    public static <T> T fe(T f, Object... e) {
        return f;
    }

    /**
     * Crea un hash a partir de un array de claves y otro de valores
     *
     * @param <V> Tipo del valor
     * @param key Array de claves
     * @param value Array de valores
     * @return hash
     */
    public static <V> Map<String, V> hash(String[] key, V[] value) {
        HashPerl map = new HashPerl(key.length);
        for (int i = 0; i < key.length; i++) {
            map.put(key[i], value[i]);
        }
        return map;
    }

    /**
     * Crea un array desde init hasta end, ambos incluidos. Si end > init el
     * array devuleto estará vacio.
     *
     * @param init Numero de inicio del array
     * @param end Numero final del array
     * @return Array del rango
     */
    public static Integer[] range(Integer init, Integer end) {
        int tam = end - init + 1;
        if (end * init < 0) {//Numero 0
            tam++;
        }
        if (tam > 0) {
            Integer[] array = new Integer[tam];
            for (int i = init, p = 0; i <= end; i++, p++) {
                array[p] = i;
            }
            return array;
        } else {
            return new Integer[0];
        }
    }

    /**
     * Crea un array desde init hasta end, ambos incluidos. Si end > init el
     * array devuleto estará vacio.
     *
     * @param init Cadena de inicio del array
     * @param end Cadena final del array
     * @return Array del rango
     */
    public static String[] range(String init, String end) {
        String rex = "[+-]?(([0-9]+(\\.[0-9]*)?)|([0-9]*\\.[0-9]+))";
        if (init.matches(rex) && end.matches(rex)) {
            int initN = Integer.parseInt(init);
            int endN = Integer.parseInt(end);
            int tam = endN - initN + 1;
            if (endN * initN < 0) {
                tam++;
            }
            if (tam > 0) {
                String[] array = new String[tam];
                for (int i = initN, p = 0; i <= endN; i++, p++) {
                    array[p] = String.valueOf(i);
                }
                return array;
            } else {
                return new String[0];
            }
        } else if (end.length() >= init.length()) {
            if (end.compareToIgnoreCase(init) > 0 || end.length() > init.length()) {
                ArrayList<String> seq = new ArrayList<>();
                Sequence[] buffer = Sequence.getSequence(init, end);
                while (true) {
                    String actual = buffer[0].getString();
                    seq.add(actual);
                    if (actual.equalsIgnoreCase(end)) {
                        break;
                    }
                    for (int i = 0; buffer[i].next(); i++);
                }
                return seq.toArray(new String[0]);
            }
        }
        return new String[0];
    }

    /**
     * Realiza una copia superficial
     *
     * @param <T> Tipo del objeto a copiar
     * @param object Objeto a copiar
     * @return Copia del objeto
     */
    public static <T> T[] copy(T[] object) {
        return Arrays.copyOf(object, object.length);
    }

    /**
     * Realiza una copia superficial
     *
     * @param <T> Tipo del objeto a copiar
     * @param object Objeto a copiar
     * @return Copia del objeto
     */
    public static <T> List<T> copy(List<T> object) {
        return new PerlList<>(object);
    }

    /**
     * Realiza una copia superficial
     *
     * @param <T> Tipo del objeto a copiar
     * @param object Objeto a copiar
     * @return Copia del objeto
     */
    public static <T> Map<String, T> copy(Map<String, T> object) {
        return new HashPerl<>(object);
    }

    /**
     * Crea una lista a partir del array pasado como argumento
     *
     * @param <T> Tipo del array
     * @param array Array
     * @return Lista
     */
    public static <T> List<T> list(T[] array) {
        return new PerlList<>(Arrays.asList(array));
    }

    /**
     * Comprueba si el objeto tiene representacion
     *
     * @param obj Objeto
     * @return Representacion perl o null si es el propio objeto
     */
    private static String repr(Object obj) {
        switch (obj.getClass().getSimpleName()) {
            case "PerlList":
                return "ARRAY(0x1)";
            case "HashPerl":
                return "HASH(0x1)";
            case "Ref":
                if (((Ref) obj).get() instanceof Ref) {
                    return "REF(0x1)";
                } else {
                    return "SCALAR(0x1)";
                }
            case "Fread":
            case "Fwrite":
                return "GLOB(0x1)";
            default:
                if (obj.getClass().isArray()) {
                    return "ARRAY(0x1)";
                }
        }
        return null;
    }

    /**
     *
     * @param array Array
     * @return Cadena de valores
     */
    public static String repr(Object[] array) {
        if (array.length == 0) {
            return "";
        }
        if (repr(array[0]) == null) {
            StringBuilder sb = new StringBuilder(array.length * 10);
            for (Object obj : array) {
                sb.append(obj);
            }
            return sb.toString();
        } else {
            String print = repr(array[0]);
            StringBuilder sb = new StringBuilder(print.length() * array.length);
            for (Object obj : array) {
                sb.append(print);
            }
            return sb.toString();
        }
    }

    /**
     * Convierte una lista en una cadena
     *
     * @param l Lista
     * @return Cadena de valores
     */
    public static String repr(List l) {
        if (l.isEmpty()) {
            return "";
        }
        if (l.get(0) == null) {
            StringBuilder sb = new StringBuilder(l.size() * 10);
            for (Object obj : l) {
                sb.append(obj);
            }
            return sb.toString();
        } else {
            String print = repr(l.get(0));
            StringBuilder sb = new StringBuilder(print.length() * l.size());
            for (Object obj : l) {
                sb.append(print);
            }
            return sb.toString();
        }
    }

    /**
     * Convierte un hash en una cadena
     *
     * @param <V> Tipo del valor del hash
     * @param m Hash
     * @return Cadena de clave valor
     */
    public static <V> String repr(Map<String, V> m) {
        if (m.isEmpty()) {
            return "";
        }
        String print = repr(m.values().iterator().next());
        if (print == null) {
            StringBuilder sb = new StringBuilder(m.size() * 20);
            for (Map.Entry<String, V> entry : m.entrySet()) {
                sb.append(entry.getKey());
                sb.append(entry.getValue());
            }
            return sb.toString();
        } else {
            StringBuilder sb = new StringBuilder((print.length() + 10) * m.size());
            for (Map.Entry<String, V> entry : m.entrySet()) {
                sb.append(entry.getKey());
                sb.append(print);
            }
            return sb.toString();
        }
    }

    /**
     * Ejecuta un comando en el shell del sistema
     *
     * @param cmd Comando
     * @return Salida del comando
     */
    public static String cmd(String cmd) {
        try {
            Process exec = Runtime.getRuntime().exec(cmd);
            exec.waitFor();
            Scanner s = new Scanner(exec.getInputStream()).useDelimiter("\\A");
            return s.hasNext() ? s.next() : "";
        } catch (IOException | InterruptedException ex) {
            return "";
        }
    }

    /**
     * Lee una linea de la entrada por teclado
     *
     * @return
     */
    public static String read() {
        Scanner sc = new Scanner(System.in);
        return sc.nextLine() + "\n";
    }

    /**
     * Lee un conjunto de leas de la entrada por teclado
     *
     * @return
     */
    public static String[] readLines() {
        FileReader file = new FileReader(FileDescriptor.in);
        BufferedReader buffer = new BufferedReader(file);
        return buffer.lines().map(line -> line + "\n").toArray(String[]::new);
    }

}
