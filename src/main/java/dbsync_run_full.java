import java.net.ConnectException;
import java.sql.*;
import java.util.ArrayList;

public class dbsync_run_full  extends Thread {
    private Thread t;
    private String thread_name;


    /**
     * initiates thread
     */
    dbsync_run_full( String thread_name ){
        this.thread_name = thread_name;
    }

    /**
     * 从source_tb更新target_tb的列项
     * update entries in target_tb from source_tb
     * @param on on which record to update
     */
    private void sync_table_entry (String on) throws java.sql.SQLException{
        Connection source_con = null;
        Connection target_con = null;
        Statement source_statement = null;
        Statement target_statement = null;
        ResultSet source_rs = null;
        ResultSet target_rs = null;
        // timer
        long timeMilli = System.currentTimeMillis();

        try{
            // update target table on entry
            String source_pw = "12345678";
            String source_url = "jdbc:mysql://113.16.195.18:3307/cgx_zcgl?characterEncoding=UTF-8&useUnicode=true&useSSL=false";
            String source_username = "root";
            source_con = DriverManager.getConnection(source_url, source_username, source_pw);
            if (source_con == null) {
                throw new ConnectException();
            }

            String target_url = "jdbc:mysql://localhost:3306/cgx_zcgl2";
            String target_username = "root";
            String target_pw = "Ryoh2s33";
            target_con = DriverManager.getConnection(target_url, target_username, target_pw);
            if (target_con == null) {
                throw new ConnectException();
            }

            // 更新表格
            // update contents of target table
            source_statement = source_con.createStatement();
            target_statement = target_con.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE
            ); // target will be updated with the result set

            // 由源表获取列名及其数据类型
            // get columns from source table
            ArrayList<String> cols = new ArrayList<>();
            ArrayList<String> col_types = new ArrayList<>();

            source_rs = source_statement.executeQuery("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS\n"
                    + "WHERE TABLE_SCHEMA='cgx_zcgl' AND TABLE_NAME='" + thread_name + "'\n"
                    + "ORDER BY ORDINAL_POSITION ");
            while(source_rs.next()){
                cols.add(source_rs.getString("COLUMN_NAME"));
                col_types.add(source_rs.getString("DATA_TYPE"));
            }

            source_rs = source_statement.executeQuery("SELECT * FROM " + thread_name + " ORDER BY " + cols.get(0));
            target_rs = target_statement.executeQuery("SELECT * FROM " + thread_name + " ORDER BY " + cols.get(0));

            // 获取源表上的主键值
            // update or insert records from source into the target table
            ArrayList<String> source_pks = new ArrayList<>();

            // 建向SQL服务器发送的更新query
            // Build SQL query to update or insert new records
            while( source_rs.next()) {
                // record key in source
                source_pks.add(source_rs.getString(cols.get(0)));

                // build query
                StringBuilder query = new StringBuilder("INSERT INTO " + thread_name + " (");
                StringBuilder keys = new StringBuilder(cols.get(0) + ",");
                StringBuilder values = new StringBuilder("VALUES (");
                if (col_types.get(0).contains("char") || col_types.get(0).contains("text") ) {
                    values.append("'").append(source_rs.getString(cols.get(0))).append("',");
                } else {
                    values.append(source_rs.getString(cols.get(0))).append(",");
                }
                StringBuilder sets = new StringBuilder("On DUPLICATE KEY UPDATE ");
                for (int i = 1; i < cols.size() -1; i++){
                    keys.append(cols.get(i)).append(",");
                    values.append("?,");
                    sets.append(cols.get(i)).append("=?,");
                }
                keys.append(cols.get(cols.size()-1)).append(")\n");
                values.append("?)\n");
                sets.append(cols.get(cols.size()-1)).append("=?;\n");
                query.append(keys).append(values).append(sets);

                PreparedStatement update_statement = target_con.prepareStatement(query.toString());
                // 需按数据类型设值。
                // set value according to types
                for (int j = 1; j < cols.size(); j++ ) {
                    if (col_types.get(j).contains("char") || col_types.get(j).contains("text")) {
                        update_statement.setNString(j, source_rs.getNString(cols.get(j)));
                        update_statement.setNString(j + cols.size() - 1, source_rs.getNString(cols.get(j)));
                    } else if ( col_types.get(j).contains("date")) {
                        update_statement.setDate(j, source_rs.getDate(cols.get(j)));
                        update_statement.setDate(j + cols.size() - 1, source_rs.getDate(cols.get(j)));
                    } else if (col_types.get(j).equalsIgnoreCase("Time")){
                        update_statement.setTime(j, source_rs.getTime(cols.get(j)));
                        update_statement.setTime(j + cols.size() - 1, source_rs.getTime(cols.get(j)));
                    } else if (col_types.get(j).equals("timestamp")) {
                        update_statement.setTimestamp(j, source_rs.getTimestamp(cols.get(j)));
                        update_statement.setTimestamp(j + cols.size() - 1, source_rs.getTimestamp(cols.get(j)));
                    } else if (col_types.get(j).contains("int") || col_types.get(j).equals("numeric") ) {
                        update_statement.setInt(j, source_rs.getInt(cols.get(j)));
                        update_statement.setInt(j + cols.size() - 1, source_rs.getInt(cols.get(j)));
                    } else if (col_types.get(j).equals("decimal")) {
                        update_statement.setBigDecimal(j, source_rs.getBigDecimal(cols.get(j)));
                        update_statement.setBigDecimal(j + cols.size() - 1, source_rs.getBigDecimal(cols.get(j)));
                    } else if (col_types.get(j).equals("float") ) {
                        update_statement.setFloat(j, source_rs.getFloat(cols.get(j)));
                        update_statement.setFloat(j + cols.size() - 1, source_rs.getFloat(cols.get(j)));
                    } else if (col_types.get(j).equals("double") ) {
                        update_statement.setDouble(j, source_rs.getDouble(cols.get(j)));
                        update_statement.setDouble(j + cols.size() - 1, source_rs.getDouble(cols.get(j)));
                    } else if (col_types.get(j).equals("long") ) {
                        update_statement.setLong(j, source_rs.getLong(cols.get(j)));
                        update_statement.setLong(j + cols.size() - 1, source_rs.getLong(cols.get(j)));
                    } else if (col_types.get(j).equals("short") ) {
                        update_statement.setShort(j, source_rs.getShort(cols.get(j)));
                        update_statement.setShort(j + cols.size() - 1, source_rs.getShort(cols.get(j)));
                    } else {
                        // 如果未定义数值类型，扔出错误
                        // throw exception if the data type is not defined in the nested statement
                        throw new Exception("Cannot process data type: " + col_types.get(j));
                    }
                }
                // 更新
                // commit update
                update_statement.execute();
                update_statement.close();
            }

            // 收集目标表中的主键值
            // collect primary key values from the target table
            ArrayList<String> target_pks = new ArrayList<>();

            while (target_rs.next()) {
                target_pks.add(target_rs.getString(cols.get(0)));
            }

            //System.out.println(source_pks.toString() + "\n" + target_pks.toString());

            // 从目标表中删除源表中不存在的记录。
            // delete from target table records not found in the source.
            for (int k = 0; k < target_pks.size(); k++ ) {
                if (!source_pks.contains(target_pks.get(k))) {
                    // drop key target_pks[k] from target table
                    String drop_rec_query = "DELETE FROM " + thread_name + " WHERE " + cols.get(0) + " = ";
                    try {
                        target_statement.executeUpdate(drop_rec_query + target_pks.get(k));
                    } catch (SQLException E){
                        target_statement.executeUpdate(drop_rec_query + "'" + target_pks.get(k) + "'");
                    }
                }
            }
            // complete
            timeMilli = System.currentTimeMillis() - timeMilli;
            System.out.println("Synchronisation complete: 在" + timeMilli + "ms后完成与源服务器" + thread_name + "表的同步。");

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            // cleanup
            if (source_con != null) source_con.close();
            if (target_con != null) target_con.close();
            if (source_statement != null) source_statement.close();
            if (target_statement != null) target_statement.close();
            if (source_rs != null) source_rs.close();
            if (target_rs != null) target_rs.close();
        }
    }


    /**
     * run a thread
     */
    @Override
    public void run() {
        try{
            // run sync scripts for each entry
            sync_table_entry("");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Start thread
     */
    public void start(){
        if (t == null) {
            t = new Thread (this, thread_name);
            t.start();
        }
    }
}
