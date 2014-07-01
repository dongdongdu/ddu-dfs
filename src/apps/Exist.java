package apps;

import java.io.FileNotFoundException;

import naming.NamingStubs;
import naming.Service;
import rmi.RMIException;
import storage.Storage;

import common.Path;

public class Exist extends ClientApplication {

    @Override
    protected void coreLogic(String[] arguments) throws ApplicationFailure {
        // TODO Auto-generated method stub
        if (arguments.length != 1) {
            throw new ApplicationFailure("usage: exist filename");
        }

        String filename = arguments[0];

        RemotePath source;

        try {
            source = new RemotePath(filename);
        } catch (IllegalArgumentException e) {
            throw new ApplicationFailure("cannot parse source path: " + e.getMessage());
        }

        // Make sure the source file is not the root directory.
        if (source.path.isRoot())
            throw new ApplicationFailure("source is the root directory");

        // Get a stub for the naming server and lock the source file.
        Service naming_server = NamingStubs.service(source.hostname);

        Path file = source.path;

        try {
            if (naming_server.isDirectory(file)) {
                throw new ApplicationFailure(source + " is a " + "directory");
            }
        } catch (FileNotFoundException e) {
        } catch (RMIException e) {
            e.printStackTrace();
        }

        boolean ret = false;
        try {
            Storage storage = naming_server.getStorage(file);
            ret = true;
        } catch (FileNotFoundException e1) {
            ret = false;
        } catch (RMIException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        if (ret) {
            System.out.print(" :-) Yes, the file " + filename + " does exist!");
        } else {
            System.out.print(" :-( No I cannot find " + filename);
        }

    }

}
