package com.cesarpomar.perldoop;

import java.io.IOException;
import perldoop.io.Fread;
import perldoop.io.Fwrite;

/**
 * Clase que representa un fichero en Perl
 *
 * @author César Pomar
 */
public class PerlFile {

    private Fread read;
    private Fwrite write;

    /**
     *
     * @param path Ruta del fichero
     * @param mode Finalidad de apertura del fichero
     * @return 1 si tuvo existe, 0 en otro caso
     */
    public Integer open(String path, String mode) {
        try {
            switch (mode) {
                case "<":
                    read = new Fread(path);
                    break;
                case ">":
                    write = new Fwrite(path, false);
                    break;
                case ">>":
                    write = new Fwrite(path, true);
                    break;
            }
        } catch (IOException ex) {
            return 0;
        }
        return 1;
    }

    /**
     * Cierra el fichero
     *
     * @return 1 si tuvo existe, 0 en otro caso
     */
    public Integer close() {
        try {
            if (read != null) {
                read.close();
                read = null;
            }
            if (write != null) {
                write.close();
                write = null;
            }
        } catch (IOException ex) {
            return 0;
        }
        return 1;
    }

    /**
     * Escribe valores en el fichero
     *
     * @param values valores
     * @return 1 si tiene exito, 0 en caso contrario
     */
    public int print(Object... values) {
        if (write != null) {
            return write.print(values);
        }
        return 0;
    }

    /**
     * Escribe valores en el fichero
     *
     * @param values valores
     * @return 1 si tiene exito, 0 en caso contrario
     */
    public int say(Object... values) {
        if (write != null) {
            return write.println(values);
        }
        return 0;
    }

    /**
     * lee una linea del fichero
     *
     * @return linea
     */
    public String read() {
        if (read != null) {
            return read.read();
        }
        return null;
    }

    /**
     * Lee todo el fichero
     *
     * @return array de lineas
     */
    public String[] readLines() {
        if (read != null) {
            return read.readLines();
        }
        return new String[0];
    }

}
