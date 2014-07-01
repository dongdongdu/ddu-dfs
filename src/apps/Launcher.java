package apps;

import static java.lang.System.out;

import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Application launcher.
 * 
 * <p>
 * This class is provided as an option in case the filesystem is packaged as a single JAR file. In this case, this class provides
 * a single entry point and a convenient way to run any of the utilities provided for accessing the filesystem.
 */
public class Launcher {
    /** Exit status indicating failure. */
    public static final int EXIT_FAILURE = 2;

    /**
     * Map from application names to application objects for individual applications.
     */
    private static java.util.LinkedHashMap<String, Application> applications;

    /**
     * Main entry point.
     * 
     * @param arguments
     *            Command line arguments. The first argument should be the name of the application to be run. Following arguments
     *            are passed to the application.
     */
    public static void main(String[] arguments) {
        // Create and fill the map from application names to application
        // objects.
        applications = new LinkedHashMap<String, Application>();

        applications.put("naming", new NamingServerApp());
        applications.put("storage", new StorageServerApp());
        applications.put("cd", new ChangeDirectoryDummy());
        applications.put("ls", new List());
        applications.put("touch", new Touch());
        applications.put("mkdir", new MakeDirectory());
        applications.put("rm", new Remove());

        applications.put("sizeof", new SizeOf());
        applications.put("exist", new Exist());

        applications.put("get", new Get());
        applications.put("put", new Put());
        applications.put("parse", new Parse());
        applications.put("pwd", new PrintWorkingDirectory());

        // Check that at least an application name is present. If not, print a
        // help message and exit.
        if (arguments.length < 1)
            usage();

        // Find the application to be run. If there is no application with the
        // given name, print a help message and exit.
        Application application = applications.get(arguments[0]);

        if (application == null)
            usage();

        // Create the array of application arguments - these are all the
        // arguments passed to this program, except the application name itself.
        String[] application_arguments = new String[arguments.length - 1];

        for (int index = 0; index < application_arguments.length; ++index)
            application_arguments[index] = arguments[index + 1];

        // Run the application.
        application.run(application_arguments);
    }

    /** Prints a help message and terminates the application. */
    private static void usage() {
        // Display a sorted list of application names.
        out.println();
        out.println("---------------------------------------------------");
        System.out.println("first argument must be one of:");

        Set<String> name_set = applications.keySet();
        String[] names = new String[name_set.size()];

        name_set.toArray(names);

        for (int index = 0; index < names.length; ++index)
            System.out.print(" " + names[index]);

        System.out.println();

        // Display additional help information and exit.
        out.println("\nTo start the naming server:");
        out.println("  naming");

        out.println("\nTo start a storage server:");
        out.println("usage: storage local_hostname naming_server directory");
        out.println("  storage 127.0.0.1 127.0.0.1 storage-test");

        out.println("\nrun cd first to change the direcotry and set the system variable");
        out.println("  cd 127.0.0.1 /");

        out.println("---------------------------------------------------");
        out.println("\n");

        System.exit(EXIT_FAILURE);
    }
}
