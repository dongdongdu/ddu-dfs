package apps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import naming.NamingStubs;
import naming.Service;
import storage.Storage;
import Utils.Util;

/**
 * Retrieves a file stored on the distributed filesystem.
 * 
 * <p>
 * The <code>get</code> command expects two arguments. The first is the source, which must be a full path to a remote file. The
 * second is the destination, which must be a path to a local file or directory. If the application is able to contact the remote
 * server and create the local file, the source file is copied to the destination file. If the destination is a directory, a new
 * file is created in the directory with the same name as the source file.
 */
public class Get extends ClientApplication {
    /**
     * The size of each request for data cannot exceed <code>BLOCK_SIZE</code>.
     */
    private static final int BLOCK_SIZE = 10;

    /** Application entry point. */
    public static void main(String[] arguments) {
        new Get().run(arguments);
    }

    /**
     * Main method.
     * 
     * @param arguments
     *            Command line arguments.
     */
    @Override
    public void coreLogic(String[] arguments) throws ApplicationFailure {
        if (arguments.length != 2) {
            throw new ApplicationFailure("usage: get upper_source_file " + "local_destination_file");
        }

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

        String localString = arguments[1];
        localString = Util.checkSourceFileCurrentDirecotry(localString);

        destination = new File(localString);

        // If the destination file is a directory, get a path to a file in that
        // directory with the same name as the source file.
        if (destination.isDirectory())
            destination = new File(destination, source.path.last());

        // Get a stub for the naming server and lock the source file.
        Service naming_server = NamingStubs.service(source.hostname);

        try {
            naming_server.lock(source.path, false);
        } catch (Throwable t) {
            throw new ApplicationFailure("cannot lock " + source + ": " + t.getMessage());
        }

        // Create an input stream reading bytes from the remote file, and an
        // output stream for writing bytes to a local copy of the file.
        // Repeatedly read up to BLOCK_SIZE bytes from the remote file, and
        // write them to the local file.
        OutputStream output_stream = null;

        try {
            Storage storage = naming_server.getStorage(source.path);
            byte[] buffer = storage.read(source.path);
            output_stream = new FileOutputStream(destination);
            output_stream.write(buffer);
            System.out.println("Get file successfully");

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
