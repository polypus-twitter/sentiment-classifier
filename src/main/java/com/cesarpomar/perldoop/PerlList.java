package com.cesarpomar.perldoop;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Clase para imitar las listas de perl, donde al añadir un valor se devuelve el
 * mismo.
 *
 * @author César Pomar
 * @param <E> Tipo de la lista
 */
public class PerlList<E> extends ArrayList<E> {

    /**
     * Crear una lista de un determinado tamaño
     *
     * @param initialCapacity Tamaño
     */
    public PerlList(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Crea una lista con un tamaño por defecto
     */
    public PerlList() {
        super();
    }

    /**
     * Crea una lista a partir de otra
     *
     * @param c
     */
    public PerlList(Collection<? extends E> c) {
        super(c);
    }

    /**
     * Guarda un elemento en la posicion indicada
     *
     * @param index Posicion del elemento
     * @param element Elemento a guardar
     * @return Elemento que se acaba de guardar
     */
    @Override
    public E set(int index, E element) {
        super.set(index, element);
        return element;
    }

}
