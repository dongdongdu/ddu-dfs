package apps;

import static Utils.Util.checkSourceFileCurrentDirecotry;
import static java.lang.System.out;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import naming.NamingStubs;
import naming.Service;

import common.Path;

/**
 * Uploads a file to the distributed filesystem.
 * 
 * <p>
 * The <code>put</code> command expects two arguments. The first is the source, which must be a path to a local file. The second
 * is the destination, which must be a path to a remote file or directory.
 */
public class Append extends ClientApplication {
    /**
     * At most <code>BLOCK_SIZE</code> bytes of data are sent in a single write request.
     */
    private static final int BLOCK_SIZE = 10 * 10;

    /** Application entry point. */
    public static void main(String[] arguments) {
        new Append().run(arguments);
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
            throw new ApplicationFailure("usage: append local_source_file upper_destination_file");
        }

        // Parse the source and destination paths.
        File source;
        RemotePath destination;

        String sourceFileString = arguments[0];
        String destiFileString = arguments[1];

        sourceFileString = checkSourceFileCurrentDirecotry(sourceFileString);

        source = new File(sourceFileString);
        // The source must refer to an existing file.
        if (!source.exists())
            throw new ApplicationFailure("source file does not exist");

        if (source.isDirectory()) {
            throw new ApplicationFailure("source path refers to a " + "directory");
        }

        try {
            destination = new RemotePath(destiFileString);
        } catch (IllegalArgumentException e) {
            throw new ApplicationFailure("cannot parse destination path: " + e.getMessage());
        }

        // If the destination path is the root directory, then a new file will
        // be created in the root directory - but the root directory must be
        // locked. Otherwise, it is safe to try to lock the parent of the
        // destination path.
        Path path_to_lock;

        if (destination.path.isRoot())
            path_to_lock = destination.path;
        else
            path_to_lock = destination.path.parent();

        // Obtain a stub for the remote naming server.
        Service naming_server = NamingStubs.service(destination.hostname);

        // Lock the parent of the destination path on the remote server.
        try {
            naming_server.lock(path_to_lock, true);
        } catch (Throwable t) {
            throw new ApplicationFailure("cannot lock " + path_to_lock + ": " + t.getMessage());
        }

        byte[] read_buffer;
        InputStream input_stream = null;

        try {

            // Path to receive the new file. This will either be the destination
            // path as provided, or if the path refers to a directory, then a
            // new file within that directory.
            Path destination_path = destination.path;
            out.println("destination_path is: " + destination_path);

            // Obtain the size of the source file.
            long souce_size = source.length();

            // Allocate the temporary read buffer and open streams.
            read_buffer = new byte[(int) souce_size];
            input_stream = new FileInputStream(source);
            input_stream.read(read_buffer);
            naming_server.appendFile(destination_path, read_buffer);
            out.println("Done, all " + destination_path + " files on servers has been appended with " + sourceFileString);
        } catch (Throwable t) {
            throw new ApplicationFailure("cannot transfer " + source + ": " + t.getMessage());
        } finally {
            // In all cases, make an effort to close all streams and unlock
            // the parent directory.
            if (input_stream != null) {
                try {
                    input_stream.close();
                } catch (Throwable t) {
                }
            }

            try {
                naming_server.unlock(path_to_lock, true);
            } catch (Throwable t) {
                fatal("could not unlock " + path_to_lock + ": " + t.getMessage());
            }
        }
    }

}
