package com.cesarpomar.perldoop;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Clase para las funciones definidas en Perl
 *
 * @author César Pomar
 */
public class Perl {

    // private static Perl instance;
    protected Perl() {
        // Exists only to defeat instantiation.
    }

    public static Perl getInstance() {
//        if (instance == null) {
//            instance = new Perl();
//        }
//        return instance;
        return new Perl();
    }

    /**
     * Imprime un array de valores
     *
     * @param values valores
     * @return 1
     */
    public int print(Object... values) {
        for (Object value : values) {
            System.out.print(value);
        }
        return 1;
    }

    /* Imprime un array de valores
     *
     * @param values valores
     * @return 1
     */
    public int say(Object... values) {
        for (Object value : values) {
            System.out.println(value);
        }
        return 1;
    }

    /**
     * Separa una cadena usando regex
     *
     * @param cad Cadena
     * @param regex Separador
     * @return cadena partida
     */
    public String[] split(String regex, String cad) {
        if (regex.equals(" ")) {
            return cad.split("(\\s)+");
        }
        return cad.split("(" + regex + ")+");
    }

    /**
     * Elimina el ultimo caracter de una funcion
     *
     * @param cad Puntero a la cadena
     * @return caracter eliminado
     */
    public String chop(Ref<String> cad) {
        int pos = cad.get().length() - 1;
        String c = cad.get().charAt(pos) + "";
        cad.set(cad.get().substring(0, pos));
        return c;
    }

    /**
     * Elimina el ultimo caracter de una funcion, en este caso ese caracter no
     * se lee y se usa el retorno para actualizar el string
     *
     * @param cad Cadena
     * @return Cadena
     */
    public String chop(String cad) {
        return cad.substring(0, cad.length() - 1);
    }

    /**
     * Limpia el final de una cadena
     *
     * @param cad Puntero a la cadena
     * @return numero de caracteres eliminados
     */
    public Integer chomp(Ref<String> cad) {
        int len = cad.get().length();
        cad.set(cad.get().trim());
        return len - cad.get().length();
    }

    /**
     * Limpia el final de una cadena, en este caso ese caracter no se lee y se
     * usa el retorno para actualizar el string
     *
     * @param cad Cadena
     * @return Cadena
     */
    public String chomp(String cad) {
        return cad.trim();
    }

    /**
     * Comprueba si un Objeto tiene asignado un valor
     *
     * @param obj Objeto
     * @return True si el objeto es distinto de null, False en otro caso
     */
    public Boolean defined(Object obj) {
        return obj != null;
    }

    /**
     * Simula la funcion each, realizando las operaciones en cada parametro
     *
     * @param it Coge la siguiente entrada del iterador
     * @param key Actualiza la variable clave
     * @param value Actualiza la variable valor
     * @return true
     */
    public Boolean each(Object it, Object key, Object value) {
        return true;
    }

    /**
     * Funcion llamada al sistema que ejecuta un comando en el shell
     *
     * @param cmd Comando
     * @return Entero que representa el estado de salida de la ejecucion.
     */
    public Integer system(String cmd) {
        try {
            Process exec = Runtime.getRuntime().exec(cmd);
            return exec.waitFor();
        } catch (IOException | InterruptedException ex) {
            return -1;
        }
    }

    /**
     * Abre un fichero
     *
     * @param file Descriptor del fichero
     * @param mode Finalidad de apertura del fichero
     * @param path Ruta del fichero
     * @return 1 si tuvo existe, 0 en otro caso
     */
    public Integer open(PerlFile file, String mode, String path) {
        return file.open(path, mode);
    }

    /**
     * Cierra un fichero
     *
     * @param file Descriptor del fichero
     * @return 1 si tuvo existe, 0 en otro caso
     */
    public Integer close(PerlFile file) {
        return file.close();
    }

    /**
     * Ordena una lista
     *
     * @param <T> Tipo de la Lista
     * @param list Lista
     * @return Lista ordenada
     */
    public <T extends Comparable> List<T> sort(List<T> list) {
        list = Pd.copy(list);
        Collections.sort(list);
        return list;
    }

    /**
     * Ordena un array
     *
     * @param <T> Tipo del Array
     * @param array Array
     * @return Array ordenado
     */
    public <T extends Comparable> T[] sort(T[] array) {
        array = Pd.copy(array);
        Arrays.sort(array);
        return array;
    }

    /**
     * Ordena una lista de forma personalizada
     *
     * @param <T> Tipo de la Lista
     * @param list Lista
     * @param comp Comparador para ordenar
     * @return Lista ordenada
     */
    public <T> List<T> sort(List<T> list, Comparator<T> comp) {
        list = Pd.copy(list);
        Collections.sort(list, comp);
        return list;
    }

    /**
     * Ordena un array de forma personalizada
     *
     * @param <T> Tipo del Array
     * @param array Array
     * @param comp Comparador para ordenar
     * @return Array ordenado
     */
    public <T> T[] sort(T[] array, Comparator<T> comp) {
        array = Pd.copy(array);
        Arrays.sort(array, comp);
        return array;
    }

    /**
     * Convierte una cadena a mayusculas
     *
     * @param s cadena
     * @return cadena en mayusculas
     */
    public String uc(String s) {
        return s.toUpperCase();
    }

    /**
     * Convierte la primera letra en mayusculas
     *
     * @param s cadena
     * @return cadena tranformada
     */
    public String ucfirst(String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    /**
     * Convierte una cadena a minusculas
     *
     * @param s cadena
     * @return cadena en minusculas
     */
    public String lc(String s) {
        return s.toLowerCase();
    }

    /**
     * Convierte la primera letra en minusculas
     *
     * @param s cadena
     * @return cadena tranformada
     */
    public String lcfirst(String s) {
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }

    /**
     * Aborta el programa y mustra un mensaje de error
     *
     * @param msg Mensaje de error
     * @return Nunca va a retornar
     */
    public Boolean die(String msg) {
        System.err.println(msg);
        System.exit(-1);
        return true;
    }

    /**
     * Borra el valor de un hash
     *
     * @param <T> Tipo del valor
     * @param hash Hashmap
     * @param key Clave
     * @return Elemento borrado
     */
    public <T> T delete(Map<String, T> hash, String key) {
        return hash.remove(key);
    }

    /**
     * Une todo los elementos de una lista separandolos con un separador.
     *
     * @param sep Separador
     * @param list Lista
     * @return Cadena resultante de la union
     */
    public String join(String sep, List<String> list) {
        if (list.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(list.size() * 16);
        for (String e : list) {
            sb.append(e);
            sb.append(sep);
        }
        return sb.substring(0, sb.length() - sep.length());
    }

    /**
     * Une todo los elementos de un array separandolos con un separador.
     *
     * @param sep Separador
     * @param array Array
     * @return Cadena resultante de la union
     */
    public String join(String sep, String[] array) {
        if (array.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(array.length * 16);
        for (String e : array) {
            sb.append(e);
            sb.append(sep);
        }
        return sb.substring(0, sb.length() - sep.length());
    }

    /**
     * Retorna un array con las claves de un map
     *
     * @param <T> Tipo valores
     * @param hash Map
     * @return Array de claves
     */
    public <T> String[] keys(Map<String, T> hash) {
        return hash.keySet().toArray(new String[0]);
    }

    /**
     * Retorna un array con los valores de un map
     *
     * @param <T> Tipo valores
     * @param hash Map
     * @return Array de valores
     */
    public <T> List<T> values(Map<String, T> hash) {
        return new PerlList<>(hash.values());
    }

    /**
     * Termina la ejecucion del programa
     *
     * @param status Codigo de salida
     * @return Nunca retorna
     */
    public Integer exit(Integer status) {
        System.exit(status);
        return 1;
    }

    /**
     * Calcula la longitud de una codena
     *
     * @param string Cadena
     * @return Longitud
     */
    public Integer length(String string) {
        return string.length();
    }

    /**
     * Crea una subcadena de la cadena actual remplazando un intervalo con otra
     * cadena.
     *
     * @param string Puntero a la cadena original
     * @param init Inicio del corte
     * @param len tamaño del corte
     * @param repl Cadena de remplazo
     * @return Cadena
     */
    public String substr(Ref<String> string, Integer init, Integer len, String repl) {
        String org = string.get();
        string.set(org.substring(0, init) + repl + org.substring(init + len));
        return org.substring(init, init + len);
    }

    /**
     * Crea una subcadena de la cadena actual remplazando un intervalo con otra
     * cadena.En este caso la salida no se lee y se usa el retorno para
     * actualizar el string
     *
     * @param string Cadena original
     * @param init Inicio del corte
     * @param len tamaño del corte
     * @param repl Cadena de remplazo
     * @return Cadena
     */
    public String substr(String string, Integer init, Integer len, String repl) {
        return string.substring(0, init) + repl + string.substring(init + len);
    }

    /**
     * Crea una subcadena de la cadena actual.
     *
     * @param string Cadena original
     * @param init Inicio del corte
     * @param len tamaño del corte
     * @return Cadena
     */
    public String substr(String string, Integer init, Integer len) {
        return string.substring(init, init + len);
    }

    /**
     * Crea una subcadena de la cadena actual.
     *
     * @param string Cadena original
     * @param init Inicio del corte
     * @return Cadena
     */
    public String substr(String string, Integer init) {
        return string.substring(init);
    }

    /**
     * Añade un elemento a un array
     *
     * @param <T> Tipo de la coleccion
     * @param array Referencia al Array
     * @param elem Elemento a añadir
     * @return Numero de elementos añadidos
     */
    public <T> Integer push(Ref<T[]> array, T elem) {
        int len = array.get().length;
        T[] result = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len + 1);
        System.arraycopy(array.get(), 0, result, 0, len);
        result[len] = elem;
        array.set(result);
        return 1;
    }

    /**
     * Añade un elemento a un array, en este caso ese elemento no se lee y se
     * usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param elem Elemento a añadir
     * @return Array actualizado
     */
    public <T> T[] push(T[] array, T elem) {
        T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length + 1);
        System.arraycopy(array, 0, result, 0, array.length);
        result[array.length] = elem;
        return result;
    }

    /**
     * Añade un elemento a una lista
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param elem Elemento a añadir
     * @return Numero de elementos añadidos
     */
    public <T> Integer push(List<T> list, T elem) {
        list.add(elem);
        return 1;
    }

    /**
     * Concatena dos arrays
     *
     * @param <T> Tipo de la coleccion
     * @param array Referencia al Array 1
     * @param elem Array 2
     * @return Numero de elementos añadidos
     */
    public <T> Integer push(Ref<T[]> array, T[] elem) {
        if (elem.length == 0) {
            return 0;
        }
        int len = array.get().length;
        T[] result = (T[]) Array.newInstance(elem.getClass().getComponentType(), len + elem.length);
        System.arraycopy(array.get(), 0, result, 0, len);
        System.arraycopy(elem, 0, result, len, elem.length);
        array.set(result);
        return elem.length;
    }

    /**
     * Concatena dos arrays, en este caso ese elemento no se lee y se usa el
     * retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array 1
     * @param elem Array 2
     * @return Array actualizado
     */
    public <T> T[] push(T[] array, T[] elem) {
        if (elem.length == 0) {
            return array;
        }
        T[] result = (T[]) Array.newInstance(elem.getClass().getComponentType(), array.length + elem.length);
        System.arraycopy(array, 0, result, 0, array.length);
        System.arraycopy(elem, 0, result, array.length, elem.length);
        return result;
    }

    /**
     * Concatena un array a una lista
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param elem Array
     * @return Numero de elementos añadidos
     */
    public <T> Integer push(List<T> list, T[] elem) {
        Collections.addAll(list, elem);
        return elem.length;
    }

    /**
     * Concatena una lista a un array
     *
     * @param <T> Tipo de la coleccion
     * @param array Referencia al Array
     * @param elem Lista
     * @return Numero de elementos añadidos
     */
    public <T> Integer push(Ref<T[]> array, List<T> elem) {
        int len = array.get().length;
        T[] result = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len + elem.size());
        System.arraycopy(array.get(), 0, result, 0, len);
        int p = len;
        for (T e : elem) {
            result[p++] = e;
        }
        array.set(result);
        return elem.size();
    }

    /**
     * Concatena una lista a un array, en este caso ese elemento no se lee y se
     * usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param elem Lista
     * @return Array actualizado
     */
    public <T> T[] push(T[] array, List<T> elem) {
        if (elem.isEmpty()) {
            return array;
        }
        T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length + elem.size());
        System.arraycopy(array, 0, result, 0, array.length);
        int p = array.length;
        for (T e : elem) {
            result[p++] = e;
        }
        return result;
    }

    /**
     * Concatena dos listas
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista 1
     * @param elem Lista 2
     * @return Numero de elementos añadidos
     */
    public <T> Integer push(List<T> list, List<T> elem) {
        list.addAll(elem);
        return elem.size();
    }

    /**
     * Añade un elemento a un array por delante
     *
     * @param <T> Tipo de la coleccion
     * @param array Referencia al Array
     * @param elem Elemento a añadir
     * @return Numero de elementos añadidos
     */
    public <T> Integer unshift(Ref<T[]> array, T elem) {
        int len = array.get().length;
        T[] result = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len + 1);
        System.arraycopy(array.get(), 0, result, 1, len);
        result[0] = elem;
        array.set(result);
        return 1;
    }

    /**
     * Añade un elemento a un array por delante, en este caso ese elemento no se
     * lee y se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param elem Elemento a añadir
     * @return Array actualizado
     */
    public <T> T[] unshift(T[] array, T elem) {
        T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length + 1);
        System.arraycopy(array, 0, result, 1, array.length);
        result[0] = elem;
        return result;
    }

    /**
     * Añade un elemento a una lista por delante
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param elem Elemento a añadir
     * @return Numero de elementos añadidos
     */
    public <T> Integer unshift(List<T> list, T elem) {
        list.add(0, elem);
        return 1;
    }

    /**
     * Concatena dos arrays por delante
     *
     * @param <T> Tipo de la coleccion
     * @param array Referencia al Array 1
     * @param elem Array 2
     * @return Numero de elementos añadidos
     */
    public <T> Integer unshift(Ref<T[]> array, T[] elem) {
        if (elem.length == 0) {
            return 0;
        }
        int len = array.get().length;
        T[] result = (T[]) Array.newInstance(elem.getClass().getComponentType(), len + elem.length);
        System.arraycopy(array.get(), 0, result, elem.length, len);
        System.arraycopy(elem, 0, result, 0, elem.length);
        array.set(result);
        return elem.length;
    }

    /**
     * Concatena dos arrays por delante, en este caso ese elemento no se lee y
     * se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array 1
     * @param elem Array 2
     * @return Array actualizado
     */
    public <T> T[] unshift(T[] array, T[] elem) {
        if (elem.length == 0) {
            return array;
        }
        T[] result = (T[]) Array.newInstance(elem.getClass().getComponentType(), array.length + elem.length);
        System.arraycopy(array, 0, result, elem.length, array.length);
        System.arraycopy(elem, 0, result, 0, elem.length);
        return result;
    }

    /**
     * Concatena un array a una lista por delante
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param elem Array
     * @return Numero de elementos añadidos
     */
    public <T> Integer unshift(List<T> list, T[] elem) {
        list.addAll(0, Arrays.asList(elem));
        return elem.length;
    }

    /**
     * Concatena una lista a un array por delante
     *
     * @param <T> Tipo de la coleccion
     * @param array Referencia al Array
     * @param elem Lista
     * @return Numero de elementos añadidos
     */
    public <T> Integer unshift(Ref<T[]> array, List<T> elem) {
        int len = array.get().length;
        T[] result = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len + elem.size());
        System.arraycopy(array.get(), 0, result, elem.size(), len);
        int p = 0;
        for (T e : elem) {
            result[p++] = e;
        }
        array.set(result);
        return elem.size();
    }

    /**
     * Concatena una lista a un array por delante, en este caso ese elemento no
     * se lee y se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param elem Lista
     * @return Array actualizado
     */
    public <T> T[] unshift(T[] array, List<T> elem) {
        if (elem.isEmpty()) {
            return array;
        }
        T[] result = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length + elem.size());
        System.arraycopy(array, 0, result, elem.size(), array.length);
        int p = 0;
        for (T e : elem) {
            result[p++] = e;
        }
        return result;
    }

    /**
     * Concatena dos listas por delante
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista 1
     * @param elem Lista 2
     * @return Numero de elementos añadidos
     */
    public <T> Integer unshift(List<T> list, List<T> elem) {
        list.addAll(0, elem);
        return elem.size();
    }

    /**
     * Quita el primer elemento de una lista y lo devuelve
     *
     * @param <T> Tipo de la lista
     * @param list Lista
     * @return Caracter eliminado de la primera posicion
     */
    public <T> T shift(List<T> list) {
        return list.remove(0);
    }

    /**
     * Quita el primer elemento de un array y lo devuelve.
     *
     * @param <T> Tipo del array
     * @param array Puntero al Array
     * @return Caracter eliminado de la primera posicion
     */
    public <T> T shift(Ref<T[]> array) {
        int len = array.get().length - 1;
        T[] src = array.get();
        T[] copia = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len);
        System.arraycopy(src, 1, copia, 0, len);
        array.set(copia);
        return src[0];
    }

    /**
     * Quita el primer elemento de un array y lo devuelve, en este caso ese
     * elemento no se lee y se usa el retorno para actualizar el array
     *
     * @param <T> Tipo del array
     * @param array Array
     * @return Array actualizado
     */
    public <T> T[] shift(T[] array) {
        T[] copia = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
        System.arraycopy(array, 1, copia, 0, array.length - 1);
        return copia;
    }

    /**
     * Quita el ultimo elemento de una lista y lo devuelve
     *
     * @param <T> Tipo de la lista
     * @param list Lista
     * @return Caracter eliminado de la primera posicion
     */
    public <T> T pop(List<T> list) {
        return list.remove(list.size() - 1);
    }

    /**
     * Quita el ultimo elemento de un array y lo devuelve.
     *
     * @param <T> Tipo del array
     * @param array Puntero al Array
     * @return Caracter eliminado de la primera posicion
     */
    public <T> T pop(Ref<T[]> array) {
        int len = array.get().length;
        T[] src = array.get();
        T[] copia = (T[]) Array.newInstance(src.getClass().getComponentType(), len - 1);
        System.arraycopy(src, 0, copia, 0, len - 1);
        array.set(copia);
        return src[len - 1];
    }

    /**
     * Quita el ultimo elemento de un array y lo devuelve, en este caso ese
     * elemento no se lee y se usa el retorno para actualizar el array
     *
     * @param <T> Tipo del array
     * @param array Array
     * @return Array actualizado
     */
    public <T> T[] pop(T[] array) {
        T[] copia = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - 1);
        System.arraycopy(array, 0, copia, 0, array.length - 1);
        return copia;
    }

    /**
     * Segmenta el array en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @return Segmento eliminado
     */
    public <T> T[] splice(Ref<T[]> array, Integer init) {
        T[] cut = (T[]) Array.newInstance(array.get().getClass().getComponentType(), array.get().length - init);
        T[] dest = (T[]) Array.newInstance(array.get().getClass().getComponentType(), init);
        System.arraycopy(array.get(), init, cut, 0, array.get().length - init);
        System.arraycopy(array.get(), 0, dest, 0, init);
        array.set(dest);
        return cut;
    }

    /**
     * Segmenta el array en un intervalo, en este caso ese elemento no se lee y
     * se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @return Array actualizado
     */
    public <T> T[] splice(T[] array, Integer init) {
        T[] dest = (T[]) Array.newInstance(array.getClass().getComponentType(), init);
        System.arraycopy(array, 0, dest, 0, init);
        return dest;
    }

    /**
     * Segmenta la lista en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param init Inicio
     * @return Segmento eliminado
     */
    public <T> List<T> splice(List<T> list, Integer init) {
        List<T> cut = new PerlList<>(list.size());
        List<T> range = list.subList(init, list.size());
        for (T e : range) {
            cut.add(e);
        }
        range.clear();
        return cut;
    }

    /**
     * Segmenta el array en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @param len tamaño del corte
     * @return Segmento eliminado
     */
    public <T> T[] splice(Ref<T[]> array, Integer init, Integer len) {
        T[] cut = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len);
        T[] dest = (T[]) Array.newInstance(array.get().getClass().getComponentType(), array.get().length - len);
        System.arraycopy(array.get(), init, cut, 0, len);
        System.arraycopy(array.get(), 0, dest, 0, init);
        System.arraycopy(array.get(), init + len, dest, init, array.get().length - len - init);
        array.set(dest);
        return cut;
    }

    /**
     * Segmenta el array en un intervalo, en este caso ese elemento no se lee y
     * se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @param len tamaño del corte
     * @return Array actualizado
     */
    public <T> T[] splice(T[] array, Integer init, Integer len) {
        T[] dest = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - len);
        System.arraycopy(array, 0, dest, 0, init);
        System.arraycopy(array, init + len, dest, init, array.length - len - init);
        return dest;
    }

    /**
     * Segmenta la lista en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param init Inicio
     * @param len tamaño del corte
     * @return Segmento eliminado
     */
    public <T> List<T> splice(List<T> list, Integer init, Integer len) {
        List<T> cut = new PerlList<>(list.size());
        List<T> range = list.subList(init, init + len);
        for (T e : range) {
            cut.add(e);
        }
        range.clear();
        return cut;
    }

    /**
     * Segmenta el array en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @param len tamaño del corte
     * @param repl Segmento de remplazo
     * @return Segmento eliminado
     */
    public <T> T[] splice(Ref<T[]> array, Integer init, Integer len, T[] repl) {
        T[] cut = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len);
        T[] dest = (T[]) Array.newInstance(array.get().getClass().getComponentType(), array.get().length - len + repl.length);
        System.arraycopy(array.get(), init, cut, 0, len);
        System.arraycopy(array.get(), 0, dest, 0, init);
        System.arraycopy(repl, 0, dest, init, repl.length);
        System.arraycopy(array.get(), init + len, dest, init + repl.length, array.get().length - len - init);
        array.set(dest);
        return cut;
    }

    /**
     * Segmenta el array en un intervalo, en este caso ese elemento no se lee y
     * se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @param len tamaño del corte
     * @param repl Segmento de remplazo
     * @return Array actualizado
     */
    public <T> T[] splice(T[] array, Integer init, Integer len, T[] repl) {
        T[] dest = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - len + repl.length);
        System.arraycopy(array, 0, dest, 0, init);
        System.arraycopy(repl, 0, dest, init, repl.length);
        System.arraycopy(array, init + len, dest, init + repl.length, array.length - len - init);
        return dest;
    }

    /**
     * Segmenta la lista en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param init Inicio
     * @param len tamaño del corte
     * @param repl Segmento de remplazo
     * @return Segmento eliminado
     */
    public <T> List<T> splice(List<T> list, Integer init, Integer len, T[] repl) {
        List<T> cut = new PerlList<>(list.size());
        List<T> range = list.subList(init, init + len);
        for (T e : range) {
            cut.add(e);
        }
        range.clear();
        list.addAll(init, Arrays.asList(repl));
        return cut;
    }

    /**
     * Segmenta el array en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @param len tamaño del corte
     * @param repl Segmento de remplazo
     * @return Segmento eliminado
     */
    public <T> T[] splice(Ref<T[]> array, Integer init, Integer len, List<T> repl) {
        T[] cut = (T[]) Array.newInstance(array.get().getClass().getComponentType(), len);
        T[] dest = (T[]) Array.newInstance(array.get().getClass().getComponentType(), array.get().length - len + repl.size());
        System.arraycopy(array.get(), init, cut, 0, len);
        System.arraycopy(array.get(), 0, dest, 0, init);
        for (int i = 0, j = init, n = 0; n < repl.size(); i++, j++, n++) {
            dest[j] = repl.get(i);
        }
        System.arraycopy(array.get(), init + len, dest, init + repl.size(), array.get().length - len - init);
        array.set(dest);
        return cut;
    }

    /**
     * Segmenta el array en un intervalo, en este caso ese elemento no se lee y
     * se usa el retorno para actualizar el array
     *
     * @param <T> Tipo de la coleccion
     * @param array Array
     * @param init Inicio
     * @param len tamaño del corte
     * @param repl Segmento de remplazo
     * @return Array actualizado
     */
    public <T> T[] splice(T[] array, Integer init, Integer len, List<T> repl) {
        T[] dest = (T[]) Array.newInstance(array.getClass().getComponentType(), array.length - len + repl.size());
        System.arraycopy(array, 0, dest, 0, init);
        for (int i = 0, j = init, n = 0; n < repl.size(); i++, j++, n++) {
            dest[j] = repl.get(i);
        }
        System.arraycopy(array, init + len, dest, init + repl.size(), array.length - len - init);
        return dest;
    }

    /**
     * Segmenta la lista en un intervalo
     *
     * @param <T> Tipo de la coleccion
     * @param list Lista
     * @param init Inicio
     * @param len tamaño del corte
     * @param repl Segmento de remplazo
     * @return Segmento eliminado
     */
    public <T> List<T> splice(List<T> list, Integer init, Integer len, List<T> repl) {
        List<T> cut = new PerlList<>(list.size());
        List<T> range = list.subList(init, init + len);
        for (T e : range) {
            cut.add(e);
        }
        range.clear();
        list.addAll(init, repl);
        return cut;
    }

}
