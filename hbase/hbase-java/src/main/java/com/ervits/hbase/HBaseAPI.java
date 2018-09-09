/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ervits.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.opencsv.CSVReader;

/**
 *
 * @author aervits
 */
public class HBaseAPI {

    private static final Logger LOG = Logger.getLogger(HBaseAPI.class.getName());

    private static final String TABLE_NAME = "table1";
    private static final String CF_DEFAULT = "cf";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.SNAPPY));

            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    public static void modifySchema(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            HTableDescriptor table = new HTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
            admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        }
    }

    public static void write(Configuration config) {

        TableName tableName = TableName.valueOf(TABLE_NAME);
        /**
         * a callback invoked when an asynchronous write fails.
         */
        final BufferedMutator.ExceptionListener listener = 
                (RetriesExhaustedWithDetailsException e, BufferedMutator mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                LOG.log(Level.INFO, "Failed to send put {0}.", e.getRow(i));
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                .listener(listener);
        FileSystem fs;
		
        try (Connection connection = ConnectionFactory.createConnection(config);
                final BufferedMutator mutator = connection.getBufferedMutator(params)) {
	        	fs = FileSystem.get(config);

	        	FileStatus[] fileStatus = fs.listStatus(new Path("hdfs://anarasimham-hdp-1.field.hortonworks.com:8020/tmp/txn_new"));
	        	int count = 0;

	        	for (FileStatus st : fileStatus) {
	        		Path filePath = st.getPath();
	        		Put put;
	        		BufferedReader buf = new BufferedReader(new InputStreamReader(fs.open(filePath)));
	        		List<Put> puts = new ArrayList<>();
	        		CSVReader csv = new CSVReader(buf);
	        		
	        		while (buf.ready()) {
	        			String[] line = csv.readNext();
	        			String rowKey = String.join("~", line[7], line[6], line[1], line[4], line[3]);
	        			put = new Put(Bytes.toBytes(rowKey));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("trxn_amt"), Bytes.toBytes(line[8]));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("discount_amt"), Bytes.toBytes(line[9]));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("store_id"), Bytes.toBytes(line[10]));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("rep_id"), Bytes.toBytes(line[11]));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("part_sku"), Bytes.toBytes(line[12]));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("id"), Bytes.toBytes(line[0]));
	        			put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("qty"), Bytes.toBytes(line[13]));
	        			puts.add(put);
	        			count++;
	        			if((count % 10000) == 0) {
        					mutator.mutate(puts);
        					LOG.log(Level.INFO, "Count: {0}", count);
        					puts.clear();
        				}
	        		}
	        		mutator.mutate(puts);
	        		puts.clear();
	        	}
	        	

            
//            String rowKey = UUID.randomUUID().toString();
//            Put put = new Put(Bytes.toBytes(rowKey));
//            put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("cnt"), Bytes.toBytes(""));

//            try(Table table = connection.getTable(tableName)) {
//                LOG.info("WRITING");
//                
//                
//            }
//            mutator.mutate(put);

        } catch (IllegalArgumentException | IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
            ex.printStackTrace();
        }
    }

    public static void main(String... args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "anarasimham-hdp-1.field.hortonworks.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)

          config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
          config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        //createSchemaTables(config);
        //modifySchema(config);
        
        long start = System.currentTimeMillis();
        write(config);
        
        long end = System.currentTimeMillis();
        LOG.log(Level.INFO, "Time: {0}", (end - start)/1000);
    }
}
