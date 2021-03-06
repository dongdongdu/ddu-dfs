package unit;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import naming.PathNode;
import storage.Storage;

import common.Path;

public class misc {

    public static void main(String[] args) throws FileNotFoundException {

        Set<Storage> hasFile = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());

        String s1 = "c:/aa/bb/cc.txt";
        File fp = new File(s1);
        try {
            if (!fp.exists()) {
                fp.mkdirs();
            }
        } catch (Exception e) {
            // TODO: handle exception
        }

        Path p = new Path(s1);
        Path parent = p.parent();
        Iterator<String> iterator = p.iterator();

        String string = p.toString();
        String last = p.last();

        Path p2 = new Path("c:/a/b/c");

        File file = p.toFile(new File("/aa"));
        System.out.println(file.toString());

        PathNode root = new PathNode();
        root.setCurrPath(new Path("/a/bc/c"));

        PathNode parentNode = root.getLastCompNode(p.parent());

        PathNode pN = new PathNode();
        pN.setCurrPath(p);
        pN.setIsDir(false);
        parentNode.getChildrenMap().put(p.last(), pN);

        // String s1 = "aa.txt";
        //
        // if (!(s1.contains("/") || s1.contains("\\"))) {
        //
        // String current;
        // try {
        // current = new java.io.File(".").getCanonicalPath();
        // out.println("Current dir:" + current);
        // out.println(current + File.separator + s1);
        // } catch (IOException e1) {
        // // TODO Auto-generated catch block
        // e1.printStackTrace();
        // }
        // }
        //
        // String s2 = "c:\\abc\\11.txt";
        // File s22 = new File(s2);
        // out.println("s22 exist is " + s22.exists());

    }

}
