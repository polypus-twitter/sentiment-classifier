package perldoop.util;

/**
 * Clase Auxiliar para la generacion de cadenas con range
 *
 * @author César Pomar
 */
public class Sequence {

    private final static char[] upper = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
    private final static char[] down = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
    private final static char[] number = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private char[] rtype;
    private char[] buffer;
    private int bufferPos;
    private int index;

    /**
     * Crea las secuencias para generar el intervalo entre begin y end
     *
     * @param begin Inicio de la secuencia
     * @param end Final de la secuencia
     * @return Secuencias
     */
    public static Sequence[] getSequence(String begin, String end) {
        char[] seq = end.toCharArray();
        Sequence[] res = new Sequence[seq.length];
        for (int i = 0, j = res.length - 1; i < res.length; i++, j--) {
            res[i] = new Sequence(seq, j, begin);
        }
        return res;
    }

    /**
     * Crea la secuencia para una posición
     *
     * @param buffer Elementos donde generar
     * @param bufferPos Posición asignada a la secuencia
     * @param begin Inicio de la secuencia actual
     */
    private Sequence(char[] buffer, int bufferPos, String begin) {
        char c = begin.charAt(begin.length() - 1);
        int desp = buffer.length - begin.length();
        if (c >= 'A' && c <= 'Z') {
            rtype = upper;
        } else if (c >= 'a' && c <= 'z') {
            rtype = down;
        } else {
            rtype = number;
        }
        this.buffer = buffer;
        this.bufferPos = bufferPos;
        if (desp > bufferPos) {
            index = -1;
            buffer[bufferPos] = 0;
        } else {
            for (int i = 0; i < rtype.length; i++) {
                if (rtype[i] == begin.charAt(bufferPos - desp)) {
                    index = i;
                    buffer[bufferPos] = begin.charAt(bufferPos - desp);
                    break;
                }
            }
        }
    }

    /**
     * Avanza la secuencia
     *
     * @return Secuencia actual terminada
     */
    public boolean next() {
        boolean flag = false;
        index++;
        if (index == rtype.length) {
            index = 0;
            flag = true;
        }
        buffer[bufferPos] = rtype[index];
        return flag;
    }

    /**
     * Obtiene la cadena que representa la secuencia actual
     *
     * @return Cadena
     */
    public String getString() {
        return new String(buffer);
    }

}
