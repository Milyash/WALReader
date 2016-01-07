import walentry.WALEntry;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * Created by milya on 15.11.15.
 */
public class ConnectionAcceptor implements Runnable {

    private static boolean serverRunning;
    private static ServerSocket server = null;

    public ConnectionAcceptor() {
        init();
    }

    private void init() {
        try {
            server = new ServerSocket(63000);
            serverRunning = true;
        } catch (IOException e) {
            System.out.println(e);
            serverRunning = false;
        }
    }

    public void run() {

        while (serverRunning) {
            if (serverRunning) {
                Socket client = null;
                try {
                    client = server.accept();
                    ObjectOutputStream ous = new ObjectOutputStream(client.getOutputStream());
                    WALEntry e = null;
                    synchronized (FileSyncer.entries) {
                        // TODO synchronized(entries) while isEmpty + poll not atomic
                        while (FileSyncer.entries.isEmpty()) {
                            FileSyncer.entries.wait();
                        }
                        e = FileSyncer.entries.poll();
                        ous.writeObject(e);
                        System.out.println("write " + e);
                        ous.flush();
                        ous.close();
                        client.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    serverRunning = false;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void start() {
        Thread t = new Thread(this, Integer.toString(new Random(1000).nextInt()));
        t.start();
    }
}
