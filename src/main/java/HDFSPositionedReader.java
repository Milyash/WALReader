import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import spout.TableUpdate;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by milya on 14.11.15.
 */
public class HDFSPositionedReader {


    public static void main(String[] args) {


        System.out.println("start listening to a port 60000");

        FileSyncer fileSyncer = new FileSyncer();
        ConnectionAcceptor connectionAcceptor = new ConnectionAcceptor();

        connectionAcceptor.start();
        fileSyncer.start();

    }


}

