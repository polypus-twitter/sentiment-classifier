package perldoop.io;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clase para almacenar un fichero de lectura.
 *
 * @author César Pomar
 */
public class Fread implements Closeable {

    private BufferedReader buffer;
    private FileReader file;

    /**
     * Abre un fichero para lectura
     *
     * @param path Ruta del fichero
     * @throws java.io.FileNotFoundException Si el fichero no existe
     */
    public Fread(String path) throws FileNotFoundException {
        file = new FileReader(path);
        buffer = new BufferedReader(file);
    }

    /**
     * Cierra el fichero de escritura
     */
    @Override
    public void close() throws IOException {
        buffer.close();
    }

    /**
     * lee una linea del fichero
     *
     * @return linea
     */
    public String read() {
        try {
            String line = buffer.readLine();
            if (line != null) {
                return line + "\n";
            }
        } catch (IOException ex) {
            Logger.getLogger(Fread.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * Lee todo el fichero
     *
     * @return array de lineas
     */
    public String[] readLines() {
        return buffer.lines().map(line -> line + "\n").toArray(String[]::new);
    }

}
