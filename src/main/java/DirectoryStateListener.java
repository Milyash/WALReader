import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import java.io.IOException;
import java.net.URI;

/**
 * Created by milya on 13.11.15.
 */
public class DirectoryStateListener {

    public static void main(String[] args) throws IOException, MissingEventsException, InterruptedException {

        String DIR_NAME = "/test";
        Path dir = new Path(DIR_NAME);

        Configuration CONF = new Configuration();
        CONF.addResource(new Path("file:///srv/hadoop-2.7.0/etc/hadoop/core-site.xml"));
//        System.out.println(CONF.get("fs.default.name"));

        FileSystem FILE_SYSTEM = FileSystem.get(CONF);
        System.out.printf(String.valueOf(FILE_SYSTEM.exists(dir)));

        System.out.println(FILE_SYSTEM.getUri());


        DFSClient hdfsClient = new DFSClient(FILE_SYSTEM.getUri(), CONF);
        DFSInotifyEventInputStream inputStream = hdfsClient.getInotifyEventStream(0);
        while (true) {
            EventBatch eventBatch = inputStream.take();
            for (Event event : eventBatch.getEvents()) {
                switch (event.getEventType()) {
                    case CREATE:
                        Event.CreateEvent createEvent = (Event.CreateEvent) event;
                        if (!createEvent.getPath().startsWith(DIR_NAME))
                            break;
                        System.out.println("event type = " + event.getEventType());
                        System.out.println(" path = " + createEvent.getPath());
                        System.out.println(" owner = " + createEvent.getOwnerName());
                        System.out.println(" ctime = " + createEvent.getCtime());
                        break;
                    case APPEND:
                        Event.AppendEvent appendEvent = (Event.AppendEvent) event;
                        if (!appendEvent.getPath().startsWith(DIR_NAME))
                            break;
                        System.out.println("event type = " + event.getEventType());
                        System.out.println(" path = " + appendEvent.getPath());

                    case UNLINK: break;
                    case CLOSE: break;
                    case RENAME: break;

                    default:
                        break;
                }
            }
        }
    }
}
