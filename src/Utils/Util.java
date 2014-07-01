package Utils;

import static java.lang.System.out;

import java.io.File;
import java.io.IOException;

public class Util {

    public static void log(String message) {
        out.println("-------------------------");
        out.println(message);
        out.println("-------------------------");
    }

    public static String checkSourceFileCurrentDirecotry(String sourceFileString) {
        String current;
        if (!(sourceFileString.contains("/") || sourceFileString.contains("\\"))) {
            try {
                current = new java.io.File(".").getCanonicalPath();
                out.println("Current dir:" + current);
                sourceFileString = current + File.separator + sourceFileString;
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        out.println("source file is " + sourceFileString);
        return sourceFileString;
    }
}
