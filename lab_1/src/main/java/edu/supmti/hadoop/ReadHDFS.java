package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <hdfs-path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = null;
        FSDataInputStream inStream = null;
        BufferedReader br = null;
        try {
            fs = FileSystem.get(conf);
            Path nomcomplet = new Path(args[0]);

            if (!fs.exists(nomcomplet)) {
                System.err.println("File does not exist: " + args[0]);
                System.exit(1);
            }

            inStream = fs.open(nomcomplet);
            br = new BufferedReader(new InputStreamReader(inStream));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try { if (br != null) br.close(); } catch (IOException e) { /* ignore */ }
            try { if (inStream != null) inStream.close(); } catch (IOException e) { /* ignore */ }
            try { if (fs != null) fs.close(); } catch (IOException e) { /* ignore */ }
        }
    }
}
