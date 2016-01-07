import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import spout.ColumnUpdate;
import spout.TableUpdate;
import walentry.WALEntry;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by milya on 15.11.15.
 */
public class FileSyncer implements Runnable {

    public static FileSystem FILE_SYSTEM;
    public static Configuration CONF;
    public static final Path LOGS_PATH = new Path("/hbase/WALs");
    public static Map<String, Long> positionsToRead;
    public static WALFactory WAL_FACTORY;

    private static final String DIRECTORY_SKIP_NAME = "splitting";
    private static final String FILE_SKIP_NAME = "meta";
    private static final String ENTRY_SKIP_COLUMN_FAMILY_NAME = "METAFAMILY";
    private static final String ENTRY_SKIP_COLUMN_NAME = "HBASE::REGION_EVENT";
    private static final String ENTRY_SKIP_TABLE_NAME = "meta";


    public static ConcurrentLinkedQueue<walentry.WALEntry> entries;

    public FileSyncer() {
        init();
    }

    private void init() {
        try {
            CONF = new Configuration();
            CONF.addResource(new Path("file:///srv/hadoop-2.7.0/etc/hadoop/core-site.xml"));
            FILE_SYSTEM = FileSystem.get(CONF);
            WAL_FACTORY = WALFactory.getInstance(CONF);
            positionsToRead = new HashMap<String, Long>();
            entries = new ConcurrentLinkedQueue<WALEntry>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            checkDirectories();
        }
    }

    private void checkDirectories() {
        try {
            FileStatus[] regionDirs = FILE_SYSTEM.listStatus(LOGS_PATH);
            for (FileStatus regionDir : regionDirs) {
                if (!regionDir.isDirectory()) {
                    System.out.println("File in WALs directory found: " + regionDir.getPath().getName());
                    continue;
                }
                if (regionDir.getPath().getName().contains(DIRECTORY_SKIP_NAME)) continue;
                checkFiles(regionDir.getPath());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }

    private void checkFiles(Path regionDir) {
        try {
            FileStatus[] regionFiles = FILE_SYSTEM.listStatus(regionDir);
            for (FileStatus regionFile : regionFiles) {
                Path regionFilePath = regionFile.getPath();
                if (!regionFile.isFile()) {
                    System.out.println("In region logs directory something wrong happened: " + regionFilePath.getName());
                    continue;
                }
                if (regionFilePath.getName().contains(FILE_SKIP_NAME)) continue;
                checkFile(regionFilePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkFile(Path regionFile) {
        try {
            WAL.Reader reader = getReader(regionFile);

            WAL.Entry entry = reader.next();
            if (entry != null)
                System.out.println(">>> Processing " + regionFile.getParent() + "/" + regionFile.getName());
            while (entry != null) {
                processEntry(regionFile, entry);
                setPositionToRead(regionFile, reader.getPosition());
                entry = reader.next();
            }
            closeReader(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private long getPositionToRead(Path path) {
        if (positionsToRead.containsKey(path.getName())) return positionsToRead.get(path.getName());
        return 0L;
    }

    private void setPositionToRead(Path path, long position) {
        positionsToRead.put(path.getName(), position);
    }

    private WAL.Reader getReader(Path path) throws IOException {
        /*WAL.Reader reader;
        if (readers.containsKey(path.getName())) {
            long position = getPositionToRead(path);
            reader = readers.get(path.getName());
            if (position != 0) reader.seek(position);
            return reader;
        }
        reader = WAL_FACTORY.createReader(FILE_SYSTEM, path);
        readers.put(path.getName(), reader);
        return reader;*/

        WAL.Reader reader = WAL_FACTORY.createReader(FILE_SYSTEM, path);
        long position = getPositionToRead(path);
        if (position != 0) reader.seek(position);
        return reader;
    }

    private void closeReader(WAL.Reader reader) {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processEntry(Path path, WAL.Entry entry) {

        String tableName = Bytes.toString(entry.getKey().getTablename().getName());

        if (tableName.endsWith(ENTRY_SKIP_TABLE_NAME)) {
            //    System.out.println("    meta table " + tableName + " => break processing");
            return;
        }
        if (tableName.startsWith("vt_") || tableName.startsWith("select_")) {
            //   System.out.println("    view table " + tableName + " => break processing");
            return;
        }

        List<walentry.Cell> columnUpdates = new ArrayList<walentry.Cell>();
        for (Cell c : entry.getEdit().getCells()) {
            if (Bytes.toString(c.getFamily()).contains(ENTRY_SKIP_COLUMN_FAMILY_NAME)
                    || Bytes.toString(c.getQualifier()).contains(ENTRY_SKIP_COLUMN_NAME)) continue;

            if (walentry.Cell.getPkFromCell(c).equals("")) continue;

            System.out.println("    Entry read: "
                    + "table: " + Bytes.toString(entry.getKey().getTablename().getName())
                    + "; pk: " + walentry.Cell.getPkFromCell(c)
                    + "; column: " + Bytes.toString(c.getFamily()) + " : " + Bytes.toString(c.getQualifier())
                    + "; value: " + Bytes.toString(CellUtil.cloneValue(c)));

            if (CellUtil.isDelete(c))
                System.out.println("it is delete! " + CellUtil.getCellKeyAsString(c));

            columnUpdates.add(new walentry.Cell(c));

        }

        synchronized (entries) {
            if (columnUpdates.size() > 0) {
                boolean wasEmpty = (entries.isEmpty()) ? true : false;
                entries.offer(new WALEntry(tableName, columnUpdates));

                if (wasEmpty) entries.notifyAll();
            }
        }
    }

    public void start() {
        Thread t = new Thread(this, Integer.toString(new Random(1000).nextInt()));
        t.start();
    }
}
