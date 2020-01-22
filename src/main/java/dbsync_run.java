import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class dbsync_run  extends Thread {
    private Thread t;
    private String thread_name;
    private int debug_level;

    // 由于源库与前置库有不同键名，需字典转换。在dbsync_run中定义规则。
    // Translate different key names between servers with a dictionary.
    private Map<String, String> column_map = new HashMap<>();

    /**
     * initiates thread
     */
    dbsync_run( String thread_name ){
        // 为跨服务器间不同函数名的联系定义。
        // define column_map for column name inconsistencies
        column_map.put("dep_child_name", "dept_child_name");
        column_map.put("dict_id1", "dict_id");

        this.thread_name = thread_name;
    }

    /**
     * return the matching column name
     * @param column_name: the name of column on the front-end database
     */
    private String get_column_name(String column_name) {
        return column_map.getOrDefault(column_name, column_name);
    }

    /**
     * 从source_tb更新target_tb的列项
     * update entries in target_tb from source_tb
     */
    private void sync_table_entry () throws java.sql.SQLException, IOException, ParserConfigurationException, SAXException {
        Connection source_con;
        Connection target_con;
        Connection update_con;
        Statement source_statement;
        Statement target_statement;
        Statement update_statement;
        ResultSet source_rs = null;
        ResultSet target_rs = null;
        ResultSet update_rs = null;
        BufferedWriter log_writer = null;

        String cur_dir = System.getProperty("user.dir");
        // acquire current datetime
        DateTimeFormatter datetime_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // open log
        try {
            log_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cur_dir + "/log.txt", true), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // 获取设定数值
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document settings_document = db.parse(new File(cur_dir + "/config/settings.xml"));
            settings_document.normalizeDocument();
            Element settings_doc_element =  (Element) settings_document.getElementsByTagName("warning_level").item(0);
            debug_level = Integer.parseInt(settings_doc_element.getElementsByTagName("value").item(0).getTextContent());
        } catch ( Exception e) {
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法打开设置文件。");
            e.printStackTrace();
            System.exit(1);
        }

        // timer
        long timeMilli = System.currentTimeMillis();

        DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document settings_document = db.parse(new File(cur_dir + "/config/settings.xml"));
        settings_document.normalizeDocument();
        Element settings_source_element =  (Element) settings_document.getElementsByTagName("source").item(0);
        Element settings_target_element =  (Element) settings_document.getElementsByTagName("target").item(0);

        // get updated tables list
        String source_pw = settings_source_element.getElementsByTagName("password").item(0).getTextContent();
        String source_url = settings_source_element.getElementsByTagName("uri").item(0).getTextContent();
        String source_username = settings_source_element.getElementsByTagName("username").item(0).getTextContent();
        String target_pw = settings_target_element.getElementsByTagName("password").item(0).getTextContent();
        String target_url = settings_target_element.getElementsByTagName("uri").item(0).getTextContent();
        String target_username = settings_target_element.getElementsByTagName("username").item(0).getTextContent();

        // update target table on entry
        source_con = DriverManager.getConnection(source_url, source_username, source_pw);
        if (source_con == null) {
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法连接源数据库。\n");
            throw new ConnectException();
        }

        update_con = DriverManager.getConnection(source_url, source_username, source_pw);
        if (update_con == null) {
            throw new ConnectException();
        }

        target_con = DriverManager.getConnection(target_url, target_username, target_pw);
        if (target_con == null) {
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法连接目标数据库。\n");
            throw new ConnectException();
        }

        // 更新表格
        // update contents of target table
        source_statement = source_con.createStatement();
        update_statement = update_con.createStatement();
        target_statement = target_con.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE
        ); // target will be updated with the result set

        // 由源表获取列名及其数据类型
        // get columns from source table
        ArrayList<String> col_names = new ArrayList<>();
        ArrayList<String> col_types = new ArrayList<>();

        // 获取更新记录
        // acquire database update/modification records
        try{
            // 获得目标表的数据结构
            // acquire target table data structure
            target_rs = target_statement.executeQuery("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS\n"
                    + "WHERE TABLE_SCHEMA='cgx_zcgl2' AND TABLE_NAME='" + thread_name + "'\n"
                    + "ORDER BY ORDINAL_POSITION ");
            while(target_rs.next()){
                col_names.add(target_rs.getString("COLUMN_NAME"));
                col_types.add(target_rs.getString("DATA_TYPE"));
            }

            StringBuilder query_string;
            StringBuilder value_string;
            // 根据更新记录更新前端数据库表格
            // update front-end database table(s) based on update records
            update_rs = update_statement.executeQuery("SELECT id, table_id, update_type FROM iam_trigger_record\n" +
                    "WHERE table_name = '" + thread_name + "' ORDER BY create_time asc");

            int entry_count = 0;

            while(update_rs.next()){
                String update_type = update_rs.getString("update_type");
                String entry_id = update_rs.getString("table_id");
                String update_id = update_rs.getString("id");

                switch (update_type) {
                    case "update":
                        query_string = new StringBuilder("SELECT * FROM " + thread_name + " WHERE " + col_names.get(0) + " = ");
                        try {
                            source_rs = source_statement.executeQuery(query_string + "'" + entry_id + "'");
                        } catch (SQLException e) {
                            source_rs = source_statement.executeQuery(query_string + entry_id);
                        }

                        if (source_rs.next()) {
                            // 获取源数据库表格与前置库表相符的数据并更新
                            // update the front-end database table with source data
                            query_string = new StringBuilder("UPDATE IGNORE " + thread_name + "\nSET ");
                            for (int i = 1; i < col_names.size() - 1; i++) {
                                query_string.append(col_names.get(i)).append("=?,");
                            }
                            query_string.append(col_names.get(col_names.size() - 1)).append("=?");
                            query_string.append("\nWHERE ").append(col_names.get(0)).append("='").append(entry_id).append("'");

                            // build query
                            PreparedStatement target_query = target_con.prepareStatement(query_string.toString());
                            for (int j = 1; j < col_names.size(); j++) {
                                if (col_types.get(j).contains("char") || col_types.get(j).contains("text")) {
                                    target_query.setNString(j, source_rs.getNString(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).contains("date") || col_types.get(j).contains("time") ) {
                                    target_query.setString(j, source_rs.getString(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).contains("int") || col_types.get(j).equals("numeric")) {
                                    target_query.setInt(j, source_rs.getInt(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("decimal")) {
                                    target_query.setBigDecimal(j, source_rs.getBigDecimal(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("float")) {
                                    target_query.setFloat(j, source_rs.getFloat(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("double")) {
                                    target_query.setDouble(j, source_rs.getDouble(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("long")) {
                                    target_query.setLong(j, source_rs.getLong(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("short")) {
                                    target_query.setShort(j, source_rs.getShort(get_column_name(col_names.get(j))));
                                } else {
                                    // 如果未定义数值类型，扔出错误
                                    log_writer.append(datetime_formatter.format(LocalDateTime.now()).concat(": 无法为")
                                            .concat(thread_name).concat("更新：不明表值数据类型").concat(col_types.get(j)).concat("。\n"));
                                    // throw exception if the data type is not defined in the nested statement
                                    throw new Exception("Cannot process data type: " + col_types.get(j));
                                }
                            }

                            // System.out.println(target_query.toString());
                            target_query.execute();

                            // 将iam_trigger_record上的条例转至iam_his_trigger_record
                            source_rs = source_statement.executeQuery("SELECT * FROM iam_trigger_record WHERE id='"
                                    + update_id + "'");
                            source_rs.next();
                            // insert into iam_hist_trigger_record
                            query_string = new StringBuilder("INSERT IGNORE INTO iam_his_trigger_record (id, table_id, table_name, update_type, create_time, deal_time)");
                            query_string.append("\n Values (");
                            query_string.append("'").append(source_rs.getString("id")).append("',");
                            query_string.append("'").append(source_rs.getString("table_id")).append("',");
                            query_string.append("'").append(source_rs.getString("table_name")).append("',");
                            query_string.append("'").append(source_rs.getString("update_type")).append("',");
                            query_string.append("'").append(source_rs.getString("create_time")).append("',");
                            query_string.append("'").append(datetime_formatter.format(LocalDateTime.now())).append("')");

                            source_statement.executeUpdate(query_string.toString());
                            source_statement.executeUpdate("DELETE FROM iam_trigger_record\nWHERE id='" + update_id + "'");
                            entry_count += 1;

                        } else {
                            log_writer.append(datetime_formatter.format(LocalDateTime.now()).concat(": ").concat(thread_name)
                                    .concat("无法更新条例: ").concat(entry_id).concat("|未在源库表中找到该条例。\n"));
                            if (debug_level == 2) System.out.println(thread_name + "无法更新条例: " + entry_id
                                    + "|未在源库表中找到该条例。\n");
                        }
                        break;

                    case "insert":
                        // 将源数据库新增的条例插入前置库内
                        // insert into front-end database table the newly-added record in the source table.
                        query_string = new StringBuilder("SELECT * FROM " + thread_name + " WHERE " + col_names.get(0) + " = ");

                        try {
                            source_rs = source_statement.executeQuery(query_string + "'" + entry_id + "'");
                        } catch (SQLException e) {
                            source_rs = source_statement.executeQuery(query_string + entry_id);
                        }

                        if (source_rs.next()) {
                            // insert source_rs data into front-end table
                            query_string = new StringBuilder("INSERT IGNORE INTO " + thread_name + " (");
                            value_string = new StringBuilder("\n" + "VALUES (");
                            for (int i = 0; i < col_names.size() - 1; i++) {
                                query_string.append(col_names.get(i)).append(",");
                                value_string.append("?,");
                            }
                            query_string.append(col_names.get(col_names.size() - 1)).append(")");
                            value_string.append("?)");
                            query_string.append(value_string);
                            PreparedStatement target_query = target_con.prepareStatement(query_string.toString());
                            for (int j = 0; j < col_names.size(); j++) {
                                if (col_types.get(j).contains("char") || col_types.get(j).contains("text")) {
                                    target_query.setNString(j + 1, source_rs.getNString(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).contains("date") || col_types.get(j).contains("time") ) {
                                    target_query.setString(j + 1, source_rs.getString(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).contains("int") || col_types.get(j).equals("numeric")) {
                                    target_query.setInt(j + 1, source_rs.getInt(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("decimal")) {
                                    target_query.setBigDecimal(j + 1, source_rs.getBigDecimal(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("float")) {
                                    target_query.setFloat(j + 1, source_rs.getFloat(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("double")) {
                                    target_query.setDouble(j + 1, source_rs.getDouble(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("long")) {
                                    target_query.setLong(j + 1, source_rs.getLong(get_column_name(col_names.get(j))));
                                } else if (col_types.get(j).equals("short")) {
                                    target_query.setShort(j + 1, source_rs.getShort(get_column_name(col_names.get(j))));
                                } else {
                                    // 如果未定义数值类型，扔出错误
                                    log_writer.append(datetime_formatter.format(LocalDateTime.now()).concat(": 无法为")
                                            .concat(thread_name).concat("新增条例：不明表值数据类型").concat(col_types.get(j)).concat("。\n"));
                                    // throw exception if the data type is not defined in the nested statement
                                    throw new Exception("Cannot process data type: " + col_types.get(j));
                                }
                            }

                            target_query.execute();
                            // 将iam_trigger_record上的条例转至iam_his_trigger_record
                            source_rs = source_statement.executeQuery("SELECT * FROM iam_trigger_record WHERE id='"
                                    + update_id + "'");
                            source_rs.next();
                            // insert into iam_hist_trigger_record
                            query_string = new StringBuilder("INSERT IGNORE INTO iam_his_trigger_record (id, table_id, table_name, update_type, create_time, deal_time)");
                            query_string.append("\n Values (");
                            query_string.append("'").append(source_rs.getString("id")).append("',");
                            query_string.append("'").append(source_rs.getString("table_id")).append("',");
                            query_string.append("'").append(source_rs.getString("table_name")).append("',");
                            query_string.append("'").append(source_rs.getString("update_type")).append("',");
                            query_string.append("'").append(source_rs.getString("create_time")).append("',");
                            query_string.append("'").append(datetime_formatter.format(LocalDateTime.now())).append("')");

                            source_statement.executeUpdate(query_string.toString());
                            source_statement.executeUpdate("DELETE FROM iam_trigger_record\nWHERE id='" + update_id + "'");
                            entry_count += 1;

                        } else {
                            log_writer.append(datetime_formatter.format(LocalDateTime.now()).concat(": ").concat(thread_name)
                                    .concat("无法新增条例: ").concat(entry_id).concat("|未在源库表中找到该条例。\n"));
                            if (debug_level == 2) System.out.println(thread_name + "无法新增条: " + entry_id + "|未在源库表中找到该条例。\n");
                        }

                        break;

                    case "delete":
                        // 从前置库删除纪录中删除项
                        // delete from front-end table the deleted record
                        query_string = new StringBuilder("DELETE FROM ").append(thread_name);
                        query_string.append("\nWHERE ").append(col_names.get(0)).append("='").append(entry_id).append("'");
                        PreparedStatement target_query = target_con.prepareStatement(query_string.toString());
                        try{
                            target_query.execute();

                            // 将iam_trigger_record上的条例转至iam_his_trigger_record
                            source_rs = source_statement.executeQuery("SELECT * FROM iam_trigger_record WHERE id='"
                                    + update_id + "'");
                            source_rs.next();
                            // insert into iam_hist_trigger_record
                            query_string = new StringBuilder("INSERT IGNORE INTO iam_his_trigger_record (id, table_id, table_name, update_type, create_time, deal_time)");
                            query_string.append("\n Values (");
                            query_string.append("'").append(source_rs.getString("id")).append("',");
                            query_string.append("'").append(source_rs.getString("table_id")).append("',");
                            query_string.append("'").append(source_rs.getString("table_name")).append("',");
                            query_string.append("'").append(source_rs.getString("update_type")).append("',");
                            query_string.append("'").append(source_rs.getString("create_time")).append("',");
                            query_string.append("'").append(datetime_formatter.format(LocalDateTime.now())).append("')");

                            source_statement.executeUpdate(query_string.toString());
                            source_statement.executeUpdate("DELETE FROM iam_trigger_record\nWHERE id='" + update_id + "'");
                            entry_count += 1;
                        } catch (SQLException e) {
                            if (debug_level > 0) {
                                System.out.println(datetime_formatter.format(LocalDateTime.now()) + ": 无法删除条例。: ");
                            }
                            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法删除条例。: ")
                                    .append(e.getMessage()).append("\n");
                        }
                        break;
                }
            }
            // count row differentials between tables
            target_rs = target_statement.executeQuery("SELECT COUNT(*) AS row_counts from " + thread_name);
            source_rs = source_statement.executeQuery("SELECT COUNT(*) AS row_counts from " + thread_name);
            target_rs.next();
            source_rs.next();
            // complete
            timeMilli = System.currentTimeMillis() - timeMilli;

            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 完成").append(thread_name).append("的同步: 在").append(String.valueOf(timeMilli)).append("ms后完成与源服务器的同步。更新或添加记录").append(String.valueOf(entry_count)).append("条。源: ").append(source_rs.getString("row_counts")).append("条记录。前置库: ").append(target_rs.getString("row_counts")).append("条记录。\n");
            if (debug_level == 2) System.out.println(datetime_formatter.format(LocalDateTime.now()) + ": 完成" + thread_name + "的同步: 在"
                    + timeMilli + "ms后完成与源服务器的同步。更新或添加记录" + entry_count + "条。源: " + source_rs.getString("row_counts")
                    + "条记录。前置库: " + target_rs.getString("row_counts") + "条记录.");

        } catch (Exception e) {
            if (debug_level > 0) {
                System.out.print(thread_name + ": ");
                e.printStackTrace();
            }
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": ").append(thread_name).append(": ").append(e.getMessage()).append("。\n");
        } finally {
            // cleanup
            log_writer.close();
            source_con.close();
            target_con.close();
            update_con.close();
            if (source_statement != null) source_statement.close();
            target_statement.close();
            update_statement.close();
            if (source_rs != null) source_rs.close();
            target_rs.close();
            if (update_rs != null) update_rs.close();
        }
    }


    /**
     * run a thread
     */
    @Override
    public void run() {
        try{
            // run sync scripts for each entry
            sync_table_entry();
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
