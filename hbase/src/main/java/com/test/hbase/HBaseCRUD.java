import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
 
public class HBaseCRUD {
 
    public static void main(String[] args) {
         HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
         hconfig.set("hbase.zookeeper.quorum", "172.31.11.101");
         hconfig.set("hbase.zookeeper.property.clientPort", "2181");
 
         //DeleteHBaseTable(hconfig);
        CreateHBaseTable();
        InsertRecords();
    }

    public static void CreateHBaseTable() {
         HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
         hconfig.set("hbase.zookeeper.quorum", "172.31.11.101");
         hconfig.set("hbase.zookeeper.property.clientPort", "2181");
         HTableDescriptor htable = new HTableDescriptor("users"); 
         htable.addFamily( new HColumnDescriptor("name"));
         htable.addFamily( new HColumnDescriptor("contact_info"));
         htable.addFamily( new HColumnDescriptor("personal_info"));
         System.out.println( "Connecting..." );
         try {
              HBaseAdmin hbase_admin = new HBaseAdmin( hconfig );
              System.out.println( "Creating Table..." );
              hbase_admin.createTable( htable );
              System.out.println("Done!");
         } catch (Exception e) {
              e.printStackTrace();
         }
    }

    public static void DeleteHBaseTable(HBaseConfiguration conf) {

         // Instantiating configuration class
         //Configuration conf = HBaseConfiguration.create();
         try{
             // Instantiating HBaseAdmin class
             HBaseAdmin admin = new HBaseAdmin(conf);

             // disabling table named emp
             admin.disableTable("users");

             // Deleting emp
             admin.deleteTable("users");
             System.out.println("Table deleted");
         } catch (Exception e){
              e.printStackTrace();
         }
    }
 
    public static void InsertRecords() {
         Configuration config = HBaseConfiguration.create();
         config.set("hbase.zookeeper.quorum", "172.31.11.101");
         config.set("hbase.zookeeper.property.clientPort", "2181");
 
         String tableName = "users";
 
         Connection connection = null;
         Table table = null;
 
         try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
 
            //    creating sample data that can be used to save into hbase table
            String[][] people = {
                    { "1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26" },
                    { "2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24" },
                    { "3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27" },
                    { "4", "Rae", "Schroeder", "rae@xyz.com", "F", "31" },
                    { "5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25" },
                    { "6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24" } };
 
            for (int i = 0; i < people.length; i++) {
                Put person = new Put(Bytes.toBytes(people[i][0]));
                person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
                person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
                person.addColumn(Bytes.toBytes("contact_info"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
                person.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("gender"), Bytes.toBytes(people[i][4]));
                person.addColumn(Bytes.toBytes("personal_info"), Bytes.toBytes("age"), Bytes.toBytes(people[i][5]));
                table.put(person);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
 
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}
