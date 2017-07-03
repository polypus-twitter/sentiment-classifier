package perldoop.io;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clase para almacenar un fichero de escritura.
 *
 * @author César Pomar
 */
public class Fwrite implements Closeable {

    private BufferedWriter buffer;
    private FileWriter file;

    /**
     * Abre el fichero para la escritura
     *
     * @param path Ruta del fichero
     * @param append true para añadir al fichero, false para borrarlo
     * @throws java.io.IOException Si no se puede abrir
     */
    public Fwrite(String path, boolean append) throws IOException {
        file = new FileWriter(path, append);
        buffer = new BufferedWriter(file);

    }

    /**
     * Cierra el fichero de escritura
     */
    @Override
    public void close() throws IOException {
        buffer.close();
        file.close();
    }

    /**
     * Escribe valores en el fichero
     *
     * @param values valores
     * @return 1 si tiene exito, 0 en caso contrario
     */
    public int print(Object... values) {
        try {
            for (Object value : values) {
                buffer.write(value.toString());
            }
        } catch (IOException ex) {
            Logger.getLogger(Fwrite.class.getName()).log(Level.SEVERE, null, ex);
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
    public int println(Object... values) {
        try {
            for (Object value : values) {
                buffer.write(value.toString());
                buffer.newLine();
            }
        } catch (IOException ex) {
            Logger.getLogger(Fwrite.class.getName()).log(Level.SEVERE, null, ex);
            return 0;
        }
        return 1;
    }

}
