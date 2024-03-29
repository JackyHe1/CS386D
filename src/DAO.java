import java.util.*;
import java.sql.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;

public class DAO{
    private final String url = "jdbc:postgresql://127.0.0.1:5432/hw2";
    private final String username = "postgres";
    private final String password = "0807";
    private String tableName = "";  //init using call create table function
    private List<int[]> data = new ArrayList<int[]>();
    private final int insertSize = 10000;
    private final int querySize = 100;
    private int dataSize = 0;
    Random rnd = new Random();

    private long query1Time = 0;
    private long query2Time = 0;
    private long query3Time = 0;

    private  Connection conn = null;


    public static void main(String[] args) {
    }

    public void init() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println( "Could not find org.postgresql.Driver, please include in library path!");
            e.printStackTrace();
            return;
        }

        System.out.println("Found PostgreSQL JDBC Driver...");


        try {
            conn = DriverManager.getConnection(url, username, password);

        } catch (SQLException e) {
            System.out.println("Connection Failed!");
            e.printStackTrace();
            return;
        }

        if (conn != null) {
            System.out.println("Connected to database successfully...");
        } else {
            System.out.println("Failed to make connection!");
        }

    }

    public void createTable(String tablename) {
        try {
            tableName = tablename;

            Statement stmt = null;
            stmt = conn.createStatement();
            String  sql = "CREATE TABLE IF NOT EXISTS " + tableName +
                    "(theKey integer not null PRIMARY KEY, " +
                    "columnA integer, " +
                    "columnB integer, " +
                    "filler char(247));";
            stmt.executeUpdate(sql);
            System.out.println("Create table benchmark successfully...");

        } catch (SQLException sqle) {
            System.out.println("Could not create table");
            sqle.printStackTrace();
        }
    }

    public void clearTable(String tableName) {
        try {
            Statement stmt = null;
            stmt = conn.createStatement();
            String  sql = "TRUNCATE TABLE " + tableName;
            stmt.executeUpdate(sql);
            System.out.println("clear table " + tableName + " data successfully...");
        } catch (SQLException sqle) {
            System.out.println("Could not clear table");
            sqle.printStackTrace();
        }
    }

    public void dropTable(String tableName) {
        try {
            Statement stmt = null;
            stmt = conn.createStatement();
            String  sql = "DROP TABLE " + tableName;
            stmt.executeUpdate(sql);
            System.out.println("drop table " + tableName + " successfully...");
        } catch (SQLException sqle) {
            System.out.println("Could not delete table");
            sqle.printStackTrace();
        }
    }

    void variation2() {
        //generate index randomly, and then insert bunch of tuples
    }

    void generateData(int size) {
        data = new ArrayList<int[]>();
        for (int key = 1; key <= size; key++) {
            int[] tuple = new int[3];
            tuple[0] = key;
            tuple[1] = rnd.nextInt(50000) + 1;
            tuple[2] = rnd.nextInt(50000) + 1;
            data.add(tuple);
        }
        dataSize = size;
    }

    void insertAllData(int size, boolean sortedPK) {
        try {
            Statement stmt = null;
            stmt = conn.createStatement();


            PreparedStatement ps = null;
            String sql = "insert into " + tableName + " values (?, ?, ?, ?)";
            ps = conn.prepareStatement(sql); // 批量插入时ps对象必须放到for循环外面

            if(!sortedPK) {
                //shuffle primary key
                Collections.shuffle(data);
            }

            for (int key = 1; key <= data.size(); key++) {

                ps.setInt(1, data.get(key - 1)[0]);
                ps.setInt(2, data.get(key - 1)[1]);
                ps.setInt(3, data.get(key - 1)[2]);
                //ps.setString(4, RandomStringUtils.randomAlphanumeric(247));
                ps.setString(4, "");  //this column in database table just for reverse space
                ps.addBatch();
                if (key % insertSize == 0) {
                    ps.executeBatch();
                    //conn.commit();
                    ps.clearBatch();
                }
                /*
                String sql = "INSERT INTO benchmark(theKey, columnA, columnB, filler) VALUES("
                        + key + "," + (rnd.nextInt(50000) + 1) + "," + (rnd.nextInt(50000) + 1)
                        + "," + RandomStringUtils.randomAlphanumeric(247) + ");";
                stmt.executeUpdate(sql);
                System("insert to ")
                */
                if (key % (size / 10) == 0) {
                    System.out.println("percents: " + key / (size / 10) * 10 + "%...");
                }
            }

            //insert rest data(less than insertSize)
            ps.executeBatch();
            ps.clearBatch();

            System.out.println("Data loading finished...");

        } catch (SQLException sqle) {
            System.out.println("Could not create table");
            sqle.printStackTrace();
        }
    }

    void loadDataAndQuery(String tableName, String attrA, String attrB, boolean sortedPK) {
        List list = getQueryList(querySize);

        System.out.println();
        if(sortedPK) System.out.println("Sorted primary key:");
        else System.out.println("Unsorted primary key:");

        createTable(tableName);
        long startTime = System.currentTimeMillis();
        //insert data without secondary index in table
        insertAllData(dataSize, sortedPK);
        long endTime = System.currentTimeMillis();

        System.out.println("Data loading time(without index) = " + (endTime - startTime));
        excuteBatchQueries(list);


        //insert data with secondary index on columnA in table
        System.out.println();
        dropTable(tableName);

        createTable(tableName);
        startTime = System.currentTimeMillis();
        createIndex(attrA);
        insertAllData(dataSize, sortedPK);
        endTime = System.currentTimeMillis();

        System.out.println("Data loading time(with index on ColumnA) = " + (endTime - startTime));

        System.out.println("Query with index on column A:");
        excuteBatchQueries(list);
        deleteIndex(attrA);


        //insert data with secondary index on columnB in table
        System.out.println();
        dropTable(tableName);
        createTable(tableName);
        startTime = System.currentTimeMillis();
        createIndex(attrB);
        insertAllData(dataSize, sortedPK);
        endTime = System.currentTimeMillis();

        System.out.println("Data loading time(with index on ColumnB) = " + (endTime - startTime));
        System.out.println("Query with index on column B:");
        excuteBatchQueries(list);
        deleteIndex(attrB);


        //insert data with secondary index on columnA and columnB in table
        System.out.println();
        dropTable(tableName);
        createTable(tableName);
        startTime = System.currentTimeMillis();
        createIndex(attrA);
        createIndex(attrB);
        insertAllData(dataSize, sortedPK);
        endTime = System.currentTimeMillis();

        System.out.println("Data loading time(with index on ColumnA and ColumnB) = " + (endTime - startTime));
        System.out.println("Query with index on column A and column B:");
        excuteBatchQueries(list);

        dropTable(tableName);
    }

    void excuteBatchQueries(List list) {
        excuteQuery(list, 0);
        excuteQuery(list, 1);
        excuteQuery(list, 2);
        printAvgQueryTime();
    }

    void createIndex(String attrname) {
        try {
            Statement stmt = null;
            stmt = conn.createStatement();
            String  sql = "create index " + attrname + "Index on " + tableName + " (" + attrname + ")";
            stmt.executeUpdate(sql);
            System.out.println("create index on " + attrname + " successfully...");
        } catch (SQLException sqle) {
            System.out.println("Could not index on " + attrname);
            sqle.printStackTrace();
        }
    }

    List<Integer> getQueryList(int querySize) {
        List<Integer> res = new ArrayList<>();
        for(int i = 0; i < querySize; i++) {
            res.add(rnd.nextInt(50000) + 1);
        }
        return res;
    }

    void excuteQuery(List list, int flag) {
        try {
            Statement stmt = null;
            stmt = conn.createStatement();

            long startTime = System.currentTimeMillis();
            for(int i = 0; i < list.size(); i++) {
                String sql = "select * from " + tableName;
                if (flag == 0) {
                    sql += " WHERE " + tableName + ".columnA = " + list.get(i);
                } else if (flag == 1) {
                    sql += " WHERE " + tableName + ".columnB = " + list.get(i);
                } else if(flag == 2){
                    sql += " WHERE " + tableName + ".columnA = " + list.get(i)
                            + "AND " + tableName + ".columnB = " + list.get(i);
                }
                stmt.execute(sql);
            }


            long endTime = System.currentTimeMillis();

            if(flag == 0) {
                query1Time = endTime - startTime;
            }
            else if(flag == 1) {
                query2Time = endTime - startTime;
            }
            else if(flag == 2) {
                query3Time = endTime - startTime;
            }

        } catch (SQLException sqle) {
            System.out.println("Could not excute query!");
            sqle.printStackTrace();
        }
    }

    void deleteIndex(String attrname) {
        try {
            Statement stmt = null;
            stmt = conn.createStatement();
            String  sql = "drop index " + attrname + "Index";
            stmt.executeUpdate(sql);
            System.out.println("drop index " + attrname + " successfully...");
        } catch (SQLException sqle) {
            System.out.println("Could not drop index " + attrname);
            sqle.printStackTrace();
        }
    }

    void printAvgQueryTime() {
        System.out.println("Query 1 select A, avg time: " + (double)query1Time / querySize);
        System.out.println("Query 2 select B, avg time: " + (double)query2Time / querySize);
        System.out.println("Query 3 select A and B, avg time: " + (double)query3Time / querySize);
    }

}
