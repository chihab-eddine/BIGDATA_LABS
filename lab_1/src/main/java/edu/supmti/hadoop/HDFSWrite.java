package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: HDFSWrite <hdfs-path> [text]");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = null;
        FSDataOutputStream outStream = null;
        try {
            fs = FileSystem.get(conf);
            Path nomcomplet = new Path(args[0]);
            if (!fs.exists(nomcomplet)) {
                outStream = fs.create(nomcomplet);
                outStream.writeUTF("Bonjour tout le monde !");
                if (args.length >= 2) {
                    outStream.writeUTF(args[1]);
                }
                outStream.close();
            } else {
                System.out.println("File already exists: " + args[0]);
            }
        } finally {
            try { if (outStream != null) outStream.close(); } catch (IOException e) { /* ignore */ }
            try { if (fs != null) fs.close(); } catch (IOException e) { /* ignore */ }
        }
    }
}
