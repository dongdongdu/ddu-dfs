package apps;

import java.io.FileNotFoundException;

import naming.NamingStubs;
import naming.Service;
import rmi.RMIException;
import storage.Storage;

import common.Path;

public class SizeOf extends ClientApplication {

    @Override
    protected void coreLogic(String[] arguments) throws ApplicationFailure {
        // TODO Auto-generated method stub
        if (arguments.length != 1) {
            throw new ApplicationFailure("usage: sizeof filename");
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
            throw new ApplicationFailure("The file " + filename + " does not exists!");
        } catch (RMIException e) {
            e.printStackTrace();
        }

        try {
            naming_server.lock(file.parent(), false);
        } catch (Throwable t) {
            throw new ApplicationFailure("cannot lock " + source + ": " + t.getMessage());
        }

        try {
            Storage storage = naming_server.getStorage(file);
            long filesize = storage.size(file);
            System.out.print("The size of " + filename + " is " + filesize);
        } catch (FileNotFoundException e) {
            throw new ApplicationFailure("The file " + filename + " does not exists!");
        } catch (RMIException e) {
            e.printStackTrace();
        }

        try {
            naming_server.unlock(file.parent(), false);
        } catch (Throwable t) {
            // Print a warning if the remote file cannot be unlocked.
            fatal("could not unlock " + source + ": " + t.getMessage());
        }

    }

}
