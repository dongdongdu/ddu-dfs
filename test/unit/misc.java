package unit;

import static java.lang.System.out;

import java.io.File;
import java.io.IOException;

public class misc {

    public static void main(String[] args) {

        String s1 = "aa.txt";

        if (!(s1.contains("/") || s1.contains("\\"))) {

            String current;
            try {
                current = new java.io.File(".").getCanonicalPath();
                out.println("Current dir:" + current);
                out.println(current + File.separator + s1);
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        String s2 = "c:\\abc\\11.txt";
        File s22 = new File(s2);
        out.println("s22 exist is " + s22.exists());

    }

}
