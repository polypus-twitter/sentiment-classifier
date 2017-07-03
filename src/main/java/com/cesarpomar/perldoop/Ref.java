package com.cesarpomar.perldoop;

/**
 * Clase para emular los punteros de Perl
 *
 * @author César Pomar
 * @param <T> Tipo del Objeto apuntado
 */
public final class Ref<T> {

    private T value;//Objeto apuntado por el puntero

    /**
     * Objeto apuntado por el puntero
     *
     * @param value
     */
    public Ref(T value) {
        this.value = value;
    }

    /**
     * Obtiene el objeto apuntado por el puntero
     *
     * @return Objeto apuntado
     */
    public T get() {
        return value;
    }

    /**
     * Cambia el objeto apuntado y devuelve el mismo valor
     *
     * @param value Objeto
     * @return Objeto value
     */
    public T set(T value) {
        this.value = value;
        return value;
    }

    /**
     * Crea una copia de esta referencia
     *
     * @return Copia
     */
    public Ref copy() {
        return new Ref(value);
    }

}
