package apps;

import static Utils.Util.checkSourceFileCurrentDirecotry;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import naming.NamingStubs;
import naming.Service;
import client.DFSOutputStream;

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
public class RandomWrite extends ClientApplication {
    /**
     * The size of each request for data cannot exceed <code>BLOCK_SIZE</code>.
     */
    private static final int BLOCK_SIZE = 1024 * 1024;

    /** Application entry point. */
    public static void main(String[] arguments) {
        new RandomWrite().run(arguments);
    }

    /**
     * Main method.
     * 
     * @param arguments
     *            Command line arguments.
     */
    @Override
    public void coreLogic(String[] arguments) throws ApplicationFailure {
        if (arguments.length != 3) {

            throw new ApplicationFailure("usage: rndw local_source_file upper_destination_file offset \n"
                    + "example: rndr local.txt server.txt 5 ");
        }

        int offset = Integer.parseInt(arguments[2]);

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

        // Obtain a stub for the remote naming server.
        Service naming_server = NamingStubs.service(destination.hostname);

        try {
            if (naming_server.isDirectory(destination.path)) {
                throw new ApplicationFailure("Your destination is a direcotry, give me a file!");
            }
        } catch (Throwable t) {
            throw new ApplicationFailure("Error when checking destination file: " + t.getMessage());
        }

        Path path_to_lock;

        if (destination.path.isRoot())
            path_to_lock = destination.path;
        else
            path_to_lock = destination.path.parent();

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
            // Obtain the size of the source file.
            long souce_size = source.length();
            // Allocate the temporary read buffer and open streams.
            read_buffer = new byte[(int) souce_size];
            input_stream = new FileInputStream(source);
            input_stream.read(read_buffer);

            Path destination_path = destination.path;
            output_stream = new DFSOutputStream(naming_server, destination_path);
            output_stream.randomWrite(read_buffer, offset);

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
