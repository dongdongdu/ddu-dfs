package apps;

import static java.lang.System.out;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import naming.NamingStubs;
import naming.Service;
import client.DFSOutputStream;

import common.Path;

/**
 * Uploads a file to the distributed filesystem.
 * 
 * <p>
 * The <code>put</code> command expects two arguments. The first is the source, which must be a path to a local file. The second
 * is the destination, which must be a path to a remote file or directory.
 */
public class Put extends ClientApplication {
    /**
     * At most <code>BLOCK_SIZE</code> bytes of data are sent in a single write request.
     */
    private static final int BLOCK_SIZE = 1024 * 1024;

    /** Application entry point. */
    public static void main(String[] arguments) {
        new Put().run(arguments);
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
            throw new ApplicationFailure("usage: put source_file " + "destination_file");
        }

        // Parse the source and destination paths.
        File source;
        RemotePath destination;

        String sourceFileString = arguments[0];
        String destiFileString = arguments[1];
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
        DFSOutputStream output_stream = null;

        try {
            // Path to receive the new file. This will either be the destination
            // path as provided, or if the path refers to a directory, then a
            // new file within that directory.
            Path destination_path = destination.path;

            // If the destination path refers to a directory, create a new path
            // to point to a new file in that directory.
            try {
                if (naming_server.isDirectory(destination.path)) {
                    String filename = source.getName();
                    if (filename.length() == 0)
                        throw new ApplicationFailure("source filename empty");

                    destination_path = new Path(destination.path, filename);

                    // If, after modifying the path, the path still refers to a
                    // directory, then the application must terminate.
                    if (naming_server.isDirectory(destination_path)) {
                        throw new ApplicationFailure(destination + " is a " + "directory");
                    }
                }

                // If control has reached here, the destination path exists, and
                // it is a regular file. Attempt to delete it.
                naming_server.delete(destination_path);
            } catch (FileNotFoundException e) {
            }

            // Create a new file with the name of the destination file.
            boolean createRes = naming_server.createFile(destination_path);
            System.out.println("The destination file" + destination_path + "creation result is " + createRes);

            // Obtain the size of the source file.
            long souce_size = source.length();

            // Allocate the temporary read buffer and open streams.
            read_buffer = new byte[(int) souce_size];
            input_stream = new FileInputStream(source);
            output_stream = new DFSOutputStream(naming_server, destination_path);

            input_stream.read(read_buffer);
            output_stream.write(read_buffer);

        } catch (ApplicationFailure e) {
            throw e;
        } catch (Throwable t) {
            throw new ApplicationFailure("cannot transfer " + source + ": " + t.getMessage());
        } finally {
            // In all cases, make an effort to close all streams and unlock
            // the parent directory.
            if (output_stream != null) {
                try {
                    output_stream.close();
                } catch (Throwable t) {
                }
            }

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
