package naming;

import static Utils.Util.log;
import static java.lang.System.out;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import rmi.RMIException;
import rmi.Skeleton;
import storage.Command;
import storage.Storage;
import Utils.Util;

import common.Path;

/**
 * Naming server.
 * 
 * <p>
 * Each instance of the filesystem is centered on a single naming server. The naming server maintains the filesystem directory
 * tree. It does not store any file data - this is done by separate storage servers. The primary purpose of the naming server is
 * to map each file name (path) to the storage server which hosts the file's contents.
 * 
 * <p>
 * The naming server provides two interfaces, <code>Service</code> and <code>Registration</code>, which are accessible through
 * RMI. Storage servers use the <code>Registration</code> interface to inform the naming server of their existence. Clients use
 * the <code>Service</code> interface to perform most filesystem operations. The documentation accompanying these interfaces
 * provides details on the methods supported.
 * 
 * <p>
 * Stubs for accessing the naming server must typically be created by directly specifying the remote network address. To make this
 * possible, the client and registration interfaces are available at well-known ports defined in <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration {
    PathNode root;
    Set<Storage> storageSets;

    ConcurrentHashMap<Path, Set<Storage>> pathStorageMap;
    ConcurrentHashMap<Storage, Command> storageCmdMap;
    Skeleton<Registration> regisSkel;
    Skeleton<Service> servSkel;

    /**
     * Creates the naming server object.
     * 
     * <p>
     * The naming server is not started.
     */
    public NamingServer() {
        PathNode rootnode = new PathNode();
        rootnode.setCurrPath(new Path("/"));
        root = rootnode;

        storageSets = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());
        pathStorageMap = new ConcurrentHashMap<Path, Set<Storage>>();
        storageCmdMap = new ConcurrentHashMap<Storage, Command>();
        regisSkel = new Skeleton<Registration>(Registration.class, this, new InetSocketAddress(NamingStubs.REGISTRATION_PORT));
        servSkel = new Skeleton<Service>(Service.class, this, new InetSocketAddress(NamingStubs.SERVICE_PORT));
    }

    /**
     * Starts the naming server.
     * 
     * <p>
     * After this method is called, it is possible to access the client and registration interfaces of the naming server remotely.
     * 
     * @throws RMIException
     *             If either of the two skeletons, for the client or registration server interfaces, could not be started. The
     *             user should not attempt to start the server again if an exception occurs.
     */
    public synchronized void start() throws RMIException {
        regisSkel.start();
        servSkel.start();
    }

    /**
     * Stops the naming server.
     * 
     * <p>
     * This method commands both the client and registration interface skeletons to stop. It attempts to interrupt as many of the
     * threads that are executing naming server code as possible. After this method is called, the naming server is no longer
     * accessible remotely. The naming server should not be restarted.
     */
    public void stop() {
        regisSkel.stop();
        servSkel.stop();
        stopped(null);
    }

    /**
     * Indicates that the server has completely shut down.
     * 
     * <p>
     * This method should be overridden for error reporting and application exit purposes. The default implementation does
     * nothing.
     * 
     * @param cause
     *            The cause for the shutdown, or <code>null</code> if the shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause) {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException {
        checkForNull(path, exclusive);
        if (!isValidPath(path))
            throw new FileNotFoundException("Path does not point to a valid " + "file/directory");
        root.lock(path, exclusive);
    }

    @Override
    public void unlock(Path path, boolean exclusive) {
        checkForNull(path, exclusive);
        if (!isValidPath(path))
            throw new IllegalArgumentException("Path does not point to a " + "valid file/directory");
        Path toCopy = root.unlock(path, exclusive);
        // replication done in unlock (when access is given to a reader or
        // writer
        Set<Storage> hasFile = pathStorageMap.get(path);
        // writer request, so select one storage to keep copy of the file,
        // delete file elsewhere
        if (exclusive) {
            if (hasFile != null) {
                // choose one storage with the file to keep the file on
                Iterator<Storage> iter = hasFile.iterator();
                Storage keptCopy = iter.next();
                // delete on all other storages with the file
                while (iter.hasNext()) {
                    Command command_stub = storageCmdMap.get(iter.next());
                    try {
                        command_stub.delete(path);
                    } catch (RMIException e) {
                    }
                }
                Set<Storage> updatedHasFiles = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());
                updatedHasFiles.add(keptCopy);
                pathStorageMap.put(path, updatedHasFiles);
            }
        } else {
            // read request, so if time to make a copy, file to copy is returned
            // by read
            if (toCopy != null) {
                Set<Storage> allServers = storageCmdMap.keySet();
                Iterator<Storage> iter = allServers.iterator();
                Storage copyFrom = hasFile.iterator().next();
                while (iter.hasNext()) {
                    Storage s = iter.next();
                    if (!hasFile.contains(s)) {
                        Command command_stub = storageCmdMap.get(s);
                        try {
                            command_stub.copy(toCopy, copyFrom);
                        } catch (Exception e) {
                        }
                        pathStorageMap.get(toCopy).add(s);
                        break;
                    }
                }

            }
        }
    }

    // returns whether a given path is valid
    private boolean isValidPath(Path p) {
        try {
            root.getLastCompNode(p);
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException {
        checkForNull(path);
        if (path.isRoot())
            return true;
        return root.getLastCompNode(path).isDirectory();
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException {
        ArrayList<String> contents = new ArrayList<String>();
        // pN is the node of the last component of the path
        PathNode pN = root.getLastCompNode(directory);

        if (pN == null || !pN.isDirectory())
            throw new FileNotFoundException("Given path does not refer to a " + "directory");

        HashMap<String, PathNode> map = pN.getChildrenMap();

        // adds all the children of pN as contents of the directory
        for (String entry : map.keySet()) {
            // add a additional slash in front of the directory
            int size = map.get(entry).getChildrenMap().size();
            if (size > 0) {
                // a directory has more than 0 childrenMap size
                contents.add("/" + entry);
            } else {
                contents.add(entry);
            }
        }
        String[] ret = new String[contents.size()];
        int index = 0;

        for (String s : contents) {
            ret[index] = s;
            index++;
        }
        return ret;
    }

    @Override
    public boolean createFile(Path file) throws RMIException, FileNotFoundException {

        checkForNull(file);
        if (file.isRoot()) {
            return false;
        }
        Path parentPath = file.parent();
        // get the node of the parent directory
        PathNode parentNode = root.getLastCompNode(parentPath);

        if (parentNode == null || !parentNode.isDirectory()) {
            throw new FileNotFoundException();
        }
        // add the file as a child to the parent
        if (parentNode.getChildrenMap().get(file.last()) != null) {
            return false;
        }
        PathNode pN = new PathNode();
        pN.setCurrPath(file);
        pN.setIsDir(false);
        parentNode.getChildrenMap().put(file.last(), pN);
        // adds the file to random storage server
        if (storageCmdMap.size() >= 1) {
            Storage aStorage = storageSets.iterator().next();
            Command cmd = storageCmdMap.get(aStorage);
            try {
                cmd.create(file);
            } catch (RMIException e) {
                return false;
            }

            Storage secondStorage = null;
            if (storageSets.size() > 1) {
                // If we have more than 1 storage server, make the 2nd copy.
                secondStorage = getADiffStorage(aStorage);
                Command secondCommand = storageCmdMap.get(secondStorage);
                try {
                    secondCommand.create(file);
                } catch (RMIException e) {
                    return false;
                }
            }

            Set<Storage> fileStorageSets = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());
            fileStorageSets.add(aStorage);
            if (secondStorage != null) {
                fileStorageSets.add(secondStorage);
            }

            // Then we added new created file to pathStorageMap
            pathStorageMap.put(file, fileStorageSets);

            return true;
        } else {
            throw new IllegalStateException();
        }

    }

    @Override
    public boolean createDirectory(Path directory) throws FileNotFoundException {
        log("now in createDirecotry!");

        checkForNull(directory);
        if (directory.isRoot()) {
            return false;
        }
        // get the node of the parent directory
        PathNode parentNode = root.getLastCompNode(directory.parent());
        if (!parentNode.isDirectory()) {
            throw new FileNotFoundException();
        }
        // add the directory as a child to the parent
        if (parentNode.getChildrenMap().get(directory.last()) != null) {
            return false;
        }
        PathNode pN = new PathNode();
        pN.setCurrPath(directory);
        parentNode.getChildrenMap().put(directory.last(), pN);
        return true;
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException {
        boolean result = true;
        if (path.isRoot())
            return false;
        // get node of parent directory
        PathNode parentNode = root.getLastCompNode(path.parent());
        if (parentNode == null)
            throw new FileNotFoundException("Parent directory does not exist");
        // if parent directory contains the last component of path
        if (parentNode.getChildrenMap().containsKey(path.last())) {
            if (isDirectory(path)) {
                // deletes from tree
                Set<Storage> storagesToDeleteFrom = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());
                // get all file paths in the directory
                ArrayList<Path> files = new ArrayList<Path>();
                Iterator<String> pathIter = path.iterator();
                root.getFilesWithin(pathIter, files);
                // deletes file from all storages that contain it
                for (Path f : files) {
                    storagesToDeleteFrom.addAll(pathStorageMap.get(f));
                }
                for (Storage s : storagesToDeleteFrom) {
                    Command command_stub = storageCmdMap.get(s);
                    try {
                        if (!command_stub.delete(path)) {
                            result = false;
                        }
                    } catch (RMIException e) {
                        return false;
                    }
                    pathStorageMap.remove(path);
                }
                parentNode.getChildrenMap().remove(path.last());
                return result;
            } else {
                // deletes from tree
                parentNode.getChildrenMap().remove(path.last());
                // deletes from storage servers
                Set<Storage> hasFile = pathStorageMap.get(path);
                if (hasFile.isEmpty()) {
                    return true;
                }
                for (Storage s : hasFile) {
                    Command cmd = storageCmdMap.get(s);
                    try {
                        result = cmd.delete(path);
                    } catch (RMIException e) {
                        return false;
                    }
                    pathStorageMap.remove(path);
                }
            }
        } else
            throw new FileNotFoundException("Given file/directory does not " + "exist");
        return result;
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException {
        checkForNull(file);

        out.println("input file is " + file);

        out.println("existing path keys are : ----------");
        for (Path path : pathStorageMap.keySet()) {
            out.println(path);
        }
        out.println("-----------");

        out.print("pathStorageMap contains key is " + pathStorageMap.containsKey(file));
        System.out.println("pathStorageMap get storage set size is " + pathStorageMap.get(file).size());

        if (!pathStorageMap.containsKey(file) || pathStorageMap.get(file).isEmpty()) {
            Util.log("File does not exist!");
            throw new FileNotFoundException("File does not exist");
        }
        // retrieves storage stub from the pathStorageMap
        Set<Storage> hasFile = pathStorageMap.get(file);

        if (!hasFile.iterator().hasNext()) {
            throw new FileNotFoundException();
        } else {
            return hasFile.iterator().next();
        }
    }

    public void register(Storage client_stub, Command command_stub, Path[] files) {
        checkForNull(client_stub, command_stub, files);
        if (storageCmdMap.containsKey(client_stub)) {
            throw new IllegalStateException("Storage server is " + "already registered");
        }
        out.println("new coming storage is " + client_stub.toString());
        out.println("new coming command is " + command_stub.toString());

        int size = storageSets.size();
        out.println("current storage server count is: " + size);
        if (size == 0) {
            // The first registration comes here
            for (Path p : files) {
                if (!p.isRoot()) {
                    boolean ret = root.addFile(p.iterator());
                    if (!ret) {
                        out.println("Erros: add path iterator to root fails!");
                    }
                    if (pathStorageMap.containsKey(p)) {
                        out.println("Error: we already had a key????");
                    } else {
                        Set<Storage> hasFile = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());
                        hasFile.add(client_stub);
                        pathStorageMap.put(p, hasFile);
                    }
                }
            }
        } else {

            Storage existStorage = getADiffStorage(client_stub);
            out.println("existStorage is " + existStorage);
            Command existCommand = getExistCommand(existStorage);
            out.println("existCommand is " + existCommand);

            // The new coming storage server comes here, first we will check each file from new server, if exist server does not
            // have, copy from new to exist
            for (Path path : files) {
                if (!path.isRoot()) {
                    if (root.addFile(path.iterator())) {
                        // exists storage server does not have new file. copy from new to exist
                        try {
                            existCommand.copy(path, client_stub);
                        } catch (Throwable e) {
                            log("error when copy file from the new storage server to existing storage server!\n" + e.getMessage());
                        }
                        Set<Storage> hasFile = Collections.newSetFromMap(new ConcurrentHashMap<Storage, Boolean>());
                        hasFile.add(existStorage);
                        hasFile.add(client_stub);
                        pathStorageMap.put(path, hasFile);
                    } else { // exist storage server already had 1 copy of file. so now we have total 2 copies of file
                        // just update the path storage map from 1 to 2
                        pathStorageMap.get(path).add(client_stub);
                    }
                }
            }

            // Only if the new coming server is the second storage server, we will do a reverse check to make sure each file has 2
            // copies.
            // if this is the 3rd coming server. we don't need to do below steps.
            if (size == 1) {
                // Now we need to check each path file in pathStorageMap, if only 1 storage has file, copy to new storage server.
                out.println("copy file from exists to new coming start!");
                for (Path path : pathStorageMap.keySet()) {
                    Set<Storage> storages = pathStorageMap.get(path);
                    if (storages.size() == 1) {
                        try {
                            command_stub.copy(path, existStorage);
                        } catch (Throwable e) {
                            log("error when copy file from the exist storage server to new coming storage server!\n"
                                    + e.getMessage());
                        }
                        storages.add(client_stub);
                    }
                }
                out.println("copy file from exists to new coming done!");
            }
        }

        storageSets.add(client_stub);
        storageCmdMap.put(client_stub, command_stub);
        out.println("New storage server has been registered!----------------------------\n");
    }

    // checks parameters for null values, throws NullPointerException if nulls
    private void checkForNull(Object... objs) {
        for (Object obj : objs) {
            if (obj == null) {
                Util.log("input object is null");
                throw new NullPointerException("cannot have a null parameter");
            }
        }
    }

    @Override
    public int getStorageServerCount() {
        return storageSets.size();
    }

    private Storage getADiffStorage(Storage aStorage) {

        Storage ret = null;

        int size = storageSets.size();
        if (size < 1) {
            throw new NullPointerException("Error cannot find any storage registered!");
        }
        if (size == 1) {
            ret = storageSets.iterator().next();
            if (ret.equals(aStorage)) {
                throw new NullPointerException("Error, input storage is the same as existing!");
            }
            return ret;
        }

        boolean flag = true;
        while (flag) {
            Random rnd = new Random();
            int idx = rnd.nextInt(size);
            Iterator<Storage> iterator = storageSets.iterator();
            while (idx >= 0) {
                ret = iterator.next();
                idx--;
            }
            if (!ret.equals(aStorage)) {
                flag = false;
            }
        }

        if (ret == null) {
            throw new NullPointerException("Error when get a existing storage!");
        }
        return ret;
    }

    private Command getExistCommand(Storage aStorage) {

        Command ret = null;
        if (storageCmdMap.containsKey(aStorage)) {
            ret = storageCmdMap.get(aStorage);
        }

        if (ret == null) {
            throw new NullPointerException("Error when get a existing command!");
        }
        return ret;
    }

}
