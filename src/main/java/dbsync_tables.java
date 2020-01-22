import java.io.*;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class dbsync_tables implements Runnable {
    //private Thread t;
    private List<String> tables_to_update = new ArrayList<>();
    int debug_level = 0;

    /**
     * 由后端服务器获取需要更新的表名
     * populate tables_to_update list from back-end server.
     */
    dbsync_tables() throws SQLException, IOException {
        String cur_dir = System.getProperty("user.dir");
        BufferedWriter log_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cur_dir + "/log.txt", true), StandardCharsets.UTF_8));
        DateTimeFormatter datetime_formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

        // 获取设定数值
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document settings_document = db.parse(new File(cur_dir + "/config/settings.xml"));
            settings_document.normalizeDocument();
            Element settings_doc_element =  (Element) settings_document.getElementsByTagName("warning_level").item(0);
            debug_level = Integer.parseInt(settings_doc_element.getElementsByTagName("value").item(0).getTextContent());
        } catch ( Exception e) {
            e.printStackTrace();
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法打开设置文件: ").append(e.getMessage()).append("\n");
            System.exit(-1);
        }

        Connection update_con = null;

        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document settings_document = db.parse(new File(cur_dir + "/config/settings.xml"));
            settings_document.normalizeDocument();
            Element settings_doc_element =  (Element) settings_document.getElementsByTagName("source").item(0);
            // get updated tables list
            String source_pw = settings_doc_element.getElementsByTagName("password").item(0).getTextContent();
            String source_url = settings_doc_element.getElementsByTagName("uri").item(0).getTextContent();
            String source_username = settings_doc_element.getElementsByTagName("username").item(0).getTextContent();
            update_con = DriverManager.getConnection(source_url, source_username, source_pw);

            if (update_con == null) {
                throw new ConnectException();
            }
            Statement updated_tables_statement = update_con.createStatement();
            ResultSet updated_tables_rs = updated_tables_statement.executeQuery("SELECT DISTINCT table_name FROM iam_trigger_record");
            // populate update table list
            while (updated_tables_rs.next()) {
                tables_to_update.add(updated_tables_rs.getString("table_name"));
            }

            Element settings_time_element = (Element) settings_document.getElementsByTagName("start_time").item(0);
            int at_hours = Integer.parseInt(settings_time_element.getElementsByTagName("hours").item(0).getTextContent());
            int at_minutes = Integer.parseInt(settings_time_element.getElementsByTagName("minutes").item(0).getTextContent());
            int at_seconds = Integer.parseInt(settings_time_element.getElementsByTagName("seconds").item(0).getTextContent());

            if (tables_to_update.size() > 1) {
                log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 预计于").append(String.valueOf(at_hours)).append(":").append(String.valueOf(at_minutes)).append(":").append(String.valueOf(at_seconds)).append(":").append("更新前置库表项: ").append(tables_to_update.toString().substring(1, tables_to_update.toString().length() - 1)).append("...\n");
                if (debug_level == 2) {
                    System.out.println(datetime_formatter.format(LocalDateTime.now()) + ": 预计于" + at_hours + ":" + at_minutes + ":" + at_seconds + ":"
                            + "更新前置库表项: " + tables_to_update.toString().substring(1, tables_to_update.toString().length() - 1) + "...");
                }
            } else if (tables_to_update.size() == 1 ) {
                log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 预计于").append(String.valueOf(at_hours)).append(":").append(String.valueOf(at_minutes)).append(":").append(String.valueOf(at_seconds)).append(":").append("更新前置库表: ").append(tables_to_update.get(0)).append("。\n");
                if (debug_level == 2) {
                    System.out.println(datetime_formatter.format(LocalDateTime.now()) + ": 预计于" + at_hours + ":" + at_minutes + ":" + at_seconds + ":"
                            + "更新前置库表: " + tables_to_update.get(0));
                }
            } else {
                log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无源表更新记录。\n");
                if (debug_level == 2) {
                    System.out.println(datetime_formatter.format(LocalDateTime.now()) + ": 无源表更新记录。");
                }
            }
        } catch (SQLException | ConnectException | ParserConfigurationException | SAXException e) {
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": ").append(e.getMessage()).append("\n");
            if (debug_level > 0) {
                e.printStackTrace();
            }
        } finally {
            log_writer.close();
            if (update_con != null){
                update_con.close();
            }
        }
    }

    /**
     * 需更新列表的数量
     * returns the number of tables to be updated
     */
    private int get_num_tables(){
        return tables_to_update.size();
    }

    /**
     * 为每个需更新的表格分线
     * Run each table update in separate threads
     */
    public void run(){
        int num_tables = get_num_tables();
        for (int thread_count = 0; thread_count < num_tables; thread_count++) {
            new dbsync_run(tables_to_update.get(thread_count)).start();
        }
    }

//    public void start(List<String> tables) {
//        if (t == null) {
//            t = new Thread (this, "table_sync_thread");
//            t.start ();
//        }
//    }
}
