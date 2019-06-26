package com.hwx.rsfinder;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;

public class Regionfinder {
        private static void usage(String[] args) {
                if (args.length < 2) {
                        System.out.println("$JAVA_HOME/bin/java -cp `hbase classpath`: Regionfinder <tablename> <rowkey>");
                        System.exit(-1);
                }
        }
 
        public static void main(String[] args) throws IOException {
                usage(args);
                TableName tablename = TableName.valueOf(args[0]);
                String rowkey = args[1];
                int bucketNum = Integer.parseInt(args[2]);


                Configuration conf = HBaseConfiguration.create();
                Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(tablename);
                RegionLocator regionLocater = connection.getRegionLocator(tablename);

                // For Salted tables first byte of key should be left empty as a place holder for the salting byte.
                byte[] rowKeyBytes = Bytes.toBytes(rowkey);
                byte[] updatedRowKeyBytes = new byte[rowKeyBytes.length + 1];
                System.arraycopy(rowKeyBytes, 0, updatedRowKeyBytes, 1, rowKeyBytes.length);

                byte[] saltedKey = SaltingUtil.getSaltedKey(new ImmutableBytesWritable(updatedRowKeyBytes),bucketNum);
                HRegionLocation regionLocation = regionLocater.getRegionLocation(saltedKey);

                String saltedKeyString = Bytes.toStringBinary(saltedKey);

                Result result = table.get(new Get(saltedKey));
                if(result.isEmpty()){
                        System.out.println("Rowkey "+rowkey+" is not exist in any region. It will be placed in region : "+regionLocation.getRegionInfo().getRegionNameAsString());
                }else{
                        System.out.println("Table Name = " + tablename + "\n" + "Row Key = " + rowkey + "\n" + "Region Server = "
                                        + regionLocation.getServerName() + "\n" + "Region Name = "
                                        + regionLocation.getRegionInfo().getRegionNameAsString() + "\n" + "Encoded Region Name = "
                                        + regionLocation.getRegionInfo().getEncodedName());
                }
        }
}
