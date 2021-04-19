import java.util.*;

import java.net.URI;
import java.net.URISyntaxException;

import java.io.*;
//import java.io.IOException;

import org.apache.log4j.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

public class Hw1Grp5 {
    private static String file;
    private static int r;
    private static String op;
    private static float val;
    private static int[] rs;
    public static int str2int(String str){
        int ans = 0;
        for(int i = 0; i < str.length(); i++){
            ans *= 10;
            ans += str.charAt(i) - '0';
        }
        return ans;
    }
    public static float str2float(String str){
        if(str.contains(".") == false){
            return (float)str2int(str);
        }
        String[] n = str.split("\\.");
        float ans = 0;
        ans += str2int(n[0]);
        float f = 0;
        for(int i = n[1].length() - 1; i >= 0;i--){
            f /= 10.0;
            f += (n[1].charAt(i) - '0') / 10.0;
        }
        return ans + f;
    }
    public static boolean compare(String compare_str, String operator, float val){
        float compare_float = str2float(compare_str);
        if(operator.equals("gt")){
            return compare_float > val;
        }
        if(operator.equals("ge")){
            return compare_float >= val;
        }
        if(operator.equals("eq")){
            return compare_float == val;
        }
        if(operator.equals("ne")){
            return compare_float != val;
        }
        if(operator.equals("le")){
            return compare_float <= val;
        }
        if(operator.equals("lt")){
            return compare_float < val;
        }
        return false;
    } 
    public static void parse(String[] args){
        if (args[0].length() <= 2) {
			System.out.println("Usage: HDFSTest <hdfs-file-path>");
			System.exit(1);
		}

        String prefix = "hdfs://localhost:9000";
		file = prefix + args[0].split("=")[1];
        
        String select = args[1];
        String[] retval1 = select.split(":");
        String[] remain1 = retval1[1].split("\\,");
        r = str2int(remain1[0].split("R")[1]);
        op = remain1[1];
        val = str2float(remain1[2]);
        /*
        System.out.println(file);
        System.out.println(r);
        System.out.println(op);
        System.out.println(val);
        */
        String distinct = args[2];
        String[] retval2 = distinct.split(":");
        String[] rs_tmp = retval2[1].split("\\,");
        int len = rs_tmp.length;
        rs = new int[len];
        for(int i = 0; i < len; i++){
            rs[i] = str2int(rs_tmp[i].split("R")[1]);
            //System.out.println(rs[i]);
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException{
        //java 5_2020E8013282011_hw1 R=/hw1-input/lineitem.tbl select:R1,gt,5.1 distinct:R2,R3,R5
        //java 5_2020E8013282011_hw1 R=/hw1/distinct_0.tbl select:R0,gt,3 distinct:R1
        //java 5_2020E8013282011_hw1 R=/hw1/distinct_1.tbl select:R1,ge,30 distinct:R2,R0
        //java 5_2020E8013282011_hw1 R=/hw1/distinct_2.tbl select:R2,lt,100 distinct:R3,R3

        parse(args);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        Logger.getRootLogger().setLevel(Level.WARN);
        // create table descriptor
        String tableName= "Result";
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor("res");
        htd.addFamily(cf);
        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);
        if (hAdmin.tableExists(tableName)){
            System.out.println("Table already exists");
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully");
        hAdmin.close();

        HTable table = new HTable(configuration,tableName);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        Map m = new HashMap(); 
        for(int i = 0; (s=in.readLine())!=null; ){
            String ss[] = s.split("\\|");
            if(compare(ss[r], op, val)){
                String tmp = "";
                for(int x: rs){
                    tmp += ss[x];
                }
                if(m.containsKey(tmp)) continue;
                else{
                    m.put(tmp, "1");
                    for(int x: rs){
                        Put put = new Put(String.valueOf(i).getBytes());
                        put.add("res".getBytes(), ("R" + String.valueOf(x)).getBytes(), ss[x].getBytes());
                        table.put(put);
                    }
                    i++;
                }
                //System.out.println(s);
            }
            
        }

        in.close();
        fs.close();

        table.close();
        System.out.println("put successfully");
        
    }
}
