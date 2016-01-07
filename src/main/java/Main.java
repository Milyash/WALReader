import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.inotify.Event;

import java.io.IOException;

/**
 * Created by milya on 01.11.15.
 */
public class Main {

    public static FileSystem FILE_SYSTEM;
    public static Configuration CONF;
    public static Path LOGS_PATH;

    public static void main(String[] args) throws IOException {

        CONF = new Configuration();
        CONF.addResource(new Path("file:///srv/hadoop-2.7.0/etc/hadoop/core-site.xml"));
        System.out.println(CONF.get("fs.default.name"));

        FILE_SYSTEM = FileSystem.get(CONF);
        LOGS_PATH = new Path("/hbase/WALs/milya,16201,1447533040729/milya%2C16201%2C1447533040729.default.1447533063025");
        Path p = LOGS_PATH;
        if (!p.toString().contains("splitting") && !p.toString().contains("meta")) {
            WALFactory walFactory = WALFactory.getInstance(CONF);
            WAL.Reader reader = walFactory.createReader(FILE_SYSTEM, p);
//            while (true) {
            WAL.Entry e = reader.next();
            while (e != null) {
                for (Cell c : e.getEdit().getCells()) {
                    if (!Bytes.toString(c.getFamily()).contains("METAFAMILY") && !Bytes.toString(e.getKey().getTablename().getName()).endsWith("meta")) {

                        System.out.println(p.getName() + "   " + Bytes.toString(e.getKey().getTablename().getName()) + " **** " + Bytes.toString(CellUtil.cloneValue(c)) + " * " + Bytes.toString(c.getFamily()) + " : " + Bytes.toString(c.getQualifier()));

                    }

                }
                System.out.println("====");
                e = reader.next();
//                }
            }
        }


//        FileStatus[] s = FILE_SYSTEM.listStatus(LOGS_PATH);
//
//        for (int i = 0; i < s.length; i++) {
//            Path p = s[i].getPath();
//            if (!p.toString().contains("splitting") && !p.toString().contains("meta")) {
//                WALFactory walFactory = WALFactory.getInstance(CONF);
//                WAL.Reader reader = walFactory.createReader(FILE_SYSTEM, p);
//                WAL.Entry e = reader.next();
//                while (e != null) {
//                    for (Cell c : e.getEdit().getCells()) {
//                        if (!Bytes.toString(c.getFamily()).contains("METAFAMILY") && !Bytes.toString(e.getKey().getTablename().getName()).endsWith("meta")) {
//
//                            System.out.println(p.getName() + "   " + Bytes.toString(e.getKey().getTablename().getName()) + " **** " + Bytes.toString(CellUtil.cloneValue(c)) + " * " + Bytes.toString(c.getFamily()) + " : " + Bytes.toString(c.getQualifier()));
//
//
//                        }
//
//                    }
//                    System.out.println("====");
//                    e = reader.next();
//                }
//            }
//        }

//        System.out.println(FILE_SYSTEM.getContentSummary(LOGS_PATH));
//
//        WALFactory walFactory = WALFactory.getInstance(CONF);
//
////        while (true) {
//        FileStatus[] s1 = FILE_SYSTEM.listStatus(LOGS_PATH);
//        for (int i = 0; i < s1.length; i++) {
//            Path p = s1[i].getPath();
//            if (!p.toString().contains("splitting")) {
//
//
//                FileStatus[] s2 = FILE_SYSTEM.listStatus(s1[i].getPath());
//                for (int j = 0; j < s2.length; j++) {
//                    Path file = s2[j].getPath();
//                    if (FILE_SYSTEM.isFile(file) && !file.getName().contains("meta")) {
//                        System.out.println("========" + file.getName());
//                        WAL.Reader reader = walFactory.createReader(FILE_SYSTEM, file);
//                        WAL.Entry e = reader.next();
//                        while (e != null) {
//                            for (Cell c : e.getEdit().getCells()) {
//                                if (!Bytes.toString(c.getFamily()).contains("METAFAMILY")) {
//                                    System.out.println(Bytes.toString(CellUtil.cloneValue(c)) + " * " + Bytes.toString(c.getFamily()) + " : " + Bytes.toString(c.getQualifier()));
//                                    System.out.println(CellUtil.cloneValue(c).getClass().getComponentType());
//                                }
//                            }
//                            System.out.println("====");
//                            e = reader.next();
//                        }
//                    }
//                }
//            }
////            }
//        }
    }
}
