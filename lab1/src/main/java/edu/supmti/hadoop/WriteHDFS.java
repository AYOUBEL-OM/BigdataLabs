package edu.supmti.hadoop;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {

  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      System.err.println("Usage: <chemin_fichier> <contenu>");
      System.exit(1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    Path nomcomplet = new Path(args[0]);

    FSDataOutputStream out = fs.create(nomcomplet, true);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

    bw.write(args[1]);
    bw.newLine();

    bw.close();
    fs.close();

    System.out.println("Écriture terminée dans " + args[0]);
  }
}