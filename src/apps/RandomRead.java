package apps;

import static Utils.Util.checkSourceFileCurrentDirecotry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import naming.NamingStubs;
import naming.Service;
import rmi.RMIException;
import storage.Storage;

import common.Path;

/**
 * Retrieves a file stored on the distributed filesystem.
 * 
 * <p>
 * The <code>get</code> command expects two arguments. The first is the source, which must be a full path to a remote file. The
 * second is the destination, which must be a path to a local file or directory. If the application is able to contact the remote
 * server and create the local file, the source file is copied to the destination file. If the destination is a directory, a new
 * file is created in the directory with the same name as the source file.
 */
public class RandomRead extends ClientApplication {
    /**
     * The size of each request for data cannot exceed <code>BLOCK_SIZE</code>.
     */
    private static final int BLOCK_SIZE = 1024 * 1024;

    /** Application entry point. */
    public static void main(String[] arguments) {
        new RandomRead().run(arguments);
    }

    /**
     * Main method.
     * 
     * @param arguments
     *            Command line arguments.
     */
    @Override
    public void coreLogic(String[] arguments) throws ApplicationFailure {
        if (arguments.length != 4) {
            throw new ApplicationFailure("usage: rndr upper_source_file offset length local_destination_file \n"
                    + "example: rndr 1.txt 5 10 c:/rndr.txt(rndr.txt)");
        }

        int offset = Integer.parseInt(arguments[1]);
        int lenth = Integer.parseInt(arguments[2]);

        // Parse the source and destination paths.
        RemotePath source;
        File destination;

        try {
            source = new RemotePath(arguments[0]);
        } catch (IllegalArgumentException e) {
            throw new ApplicationFailure("cannot parse source path: " + e.getMessage());
        }

        // Make sure the source file is not the root directory.
        if (source.path.isRoot())
            throw new ApplicationFailure("source is the root directory");

        String localString = arguments[3];
        localString = checkSourceFileCurrentDirecotry(localString);
        destination = new File(localString);
        System.out.println("destination is " + destination);

        // If the destination file is a directory, get a path to a file in that
        // directory with the same name as the source file.
        if (destination.isDirectory()) {
            destination = new File(destination, source.path.last());
            System.out.println("destination is " + destination);
        }

        // Get a stub for the naming server and lock the source file.
        Service naming_server = NamingStubs.service(source.hostname);

        Path source_file = source.path;

        try {
            if (!naming_server.isFileExist(source_file)) {
                throw new ApplicationFailure("cannot find your file on servers: " + source_file);
            }
        } catch (RMIException e) {
            throw new ApplicationFailure("RMI error when checking file existence on servers: " + source_file + "\n"
                    + e.getMessage());
        }

        try {
            naming_server.lock(source_file, false);
        } catch (Throwable t) {
            throw new ApplicationFailure("cannot lock " + source + ": " + t.getMessage());
        }

        // Create an input stream reading bytes from the remote file, and an
        // output stream for writing bytes to a local copy of the file.
        // Repeatedly read up to BLOCK_SIZE bytes from the remote file, and
        // write them to the local file.
        byte[] data_buffer;
        OutputStream output_stream = null;

        try {
            output_stream = new FileOutputStream(destination);
            Storage aStorage = naming_server.getStorage(source_file);
            data_buffer = aStorage.randomRead(source_file, offset, lenth);
            output_stream.write(data_buffer);
            System.out.println("Done, the random reading has been stored at: " + destination.toString());

        } catch (Throwable t) {
            throw new ApplicationFailure("cannot transfer " + source + ": " + t.getMessage());
        } finally {
            // In all cases, make an effort to close all streams, and to
            // unlock the file.
            if (output_stream != null) {
                try {
                    output_stream.close();
                } catch (Throwable t) {
                }
            }

            try {
                naming_server.unlock(source.path, false);
            } catch (Throwable t) {
                // Print a warning if the remote file cannot be unlocked.
                fatal("could not unlock " + source + ": " + t.getMessage());
            }
        }
    }
}
