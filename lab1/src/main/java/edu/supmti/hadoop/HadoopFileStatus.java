package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;

public class HadoopFileStatus {
  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: <chemin> <nom_fichier> <nouveau_nom>");
      System.exit(1);
    }
    String chemin = args[0];
    String nom = args[1];
    String nouveau = args[2];

    Configuration conf = new Configuration();
    FileSystem fs = null;
    try {
      fs = FileSystem.get(conf);
      Path filepath = new Path(chemin, nom);
      if (!fs.exists(filepath)) {
        System.out.println("File does not exists: " + filepath.toString());
        System.exit(1);
      }
      FileStatus status = fs.getFileStatus(filepath);

      System.out.println("File Name: " + filepath.getName());
      System.out.println("File Size: " + status.getLen() + " bytes");
      System.out.println("File owner: " + status.getOwner());
      System.out.println("File permission: " + status.getPermission());
      System.out.println("File Replication: " + status.getReplication());
      System.out.println("File Block Size: " + status.getBlockSize());

      BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
      for (BlockLocation blockLocation : blockLocations) {
        String[] hosts = blockLocation.getHosts();
        System.out.println("Block offset: " + blockLocation.getOffset());
        System.out.println("Block length: " + blockLocation.getLength());
        System.out.print("Block hosts: ");
        for (String host : hosts) {
          System.out.print(host + " ");
        }
        System.out.println();
      }

      // rename
      Path newPath = new Path(chemin, nouveau);
      boolean ok = fs.rename(filepath, newPath);
      System.out.println("Rename " + (ok ? "successful" : "failed") + ": " + filepath + " -> " + newPath);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fs != null) {
        try { fs.close(); } catch (IOException ignored) {}
      }
    }
  }
}
