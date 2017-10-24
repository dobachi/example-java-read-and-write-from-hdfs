package io.saagie.example.hdfs;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.logging.Logger;

public class NaiveMultipleFilesystemMain {

   private static final Logger logger = Logger.getLogger("io.saagie.example.hdfs.NaiveMultipleFilesystemMain");

   public static void main(String[] args) throws Exception {
      //HDFS URI

      if (args.length<1) {
         logger.severe("1 arg is required :\n\t- hdfsmasteruri (8020 port) ex: hdfs://namenodeserver:8020");
         System.err.println("1 arg is required :\n\t- hdfsmasteruri (8020 port) ex: hdfs://namenodeserver:8020");
         System.exit(128);
      }
      String hdfsuri = args[0];
      String mode = args[1];

      String path="/user/hdfs/example/hdfs/";
      String fileName1="hello1.csv";
      String fileName2="hello2.csv";
      String fileContent="hello;world";

      // ====== Init HDFS File System Object
      Configuration conf = new Configuration();
      // Set FileSystem URI
      conf.set("fs.defaultFS", hdfsuri);
      // Because of Maven
      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
      // Set HADOOP user
      System.setProperty("HADOOP_USER_NAME", "hdfs");
      System.setProperty("hadoop.home.dir", "/");

      //Get the filesystem - HDFS
      FileSystem fs1;
      FileSystem fs2;
      if(mode.equals("naive")) {
         fs1 = FileSystem.get(URI.create(hdfsuri), conf);
         fs2 = FileSystem.get(URI.create(hdfsuri), conf);
      } else {
         fs1 = FileSystem.newInstance(URI.create(hdfsuri), conf);
         fs2 = FileSystem.newInstance(URI.create(hdfsuri), conf);

      }

      //==== Create folder if not exists
      Path workingDir=fs1.getWorkingDirectory();
      Path newFolderPath= new Path(path);
      if(!fs1.exists(newFolderPath)) {
         // Create new Directory
         fs1.mkdirs(newFolderPath);
         logger.info("Path "+path+" created.");
      }

      //==== Write file1
      logger.info("Begin Write file into hdfs");
      //Create a path
      Path hdfswritepath1 = new Path(newFolderPath + "/" + fileName1);
      //Init output stream
      FSDataOutputStream outputStream1 = fs1.create(hdfswritepath1);
      //Cassical output stream usage
      outputStream1.writeBytes(fileContent);
      outputStream1.close();
      fs1.close();
      logger.info("End Write file1 into hdfs");

      //==== Write file2
      logger.info("Begin Write file2 into hdfs");
      //Create a path
      Path hdfswritepath2 = new Path(newFolderPath + "/" + fileName2);
      //Init output stream
      FSDataOutputStream outputStream2 = fs2.create(hdfswritepath2);
      //Cassical output stream usage
      outputStream2.writeBytes(fileContent);
      outputStream2.close();
      fs2.close();
      logger.info("End Write file into hdfs");

   }
}
