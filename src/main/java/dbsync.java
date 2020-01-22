import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.io.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class dbsync {

    // run
    public static void main(String[] args) throws IOException, SQLException {

        DateTimeFormatter datetime_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String cur_dir = System.getProperty("user.dir");
        BufferedWriter log_writer;
        BufferedWriter settings_writer = null;
        boolean log_created = false;
        boolean config_created;
        int interval_sec = 86400;
        boolean debug_mode = false;

        // 如未找到日志文件，创建文件。
        File log_file = new File(cur_dir + "/log.txt");
        if (! log_file.exists() ) {
            log_created = log_file.createNewFile();
        }
        // 录入日志创建记录
        log_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cur_dir + "/log.txt", true), StandardCharsets.UTF_8));
        if (log_created) {
            log_writer.write(datetime_formatter.format(LocalDateTime.now()).concat(": ").concat("未找到配置文件：创建日志文件。\n"));
        }

        File settings_file = new File(cur_dir + "/config/settings.xml");
        // 如未找到设定文件，创建文件。
        if ( !settings_file.exists() ){
            try {
                if (new File(cur_dir + "/config").mkdir()) {
                    config_created = new File(cur_dir + "/config/settings.xml").createNewFile();
                    if (config_created) {
                        log_writer.append(datetime_formatter.format(LocalDateTime.now()).concat(": ").concat("未找到设定文件：创建设定文件。\n"));
                    }

                    // 初始化设置文件内容
                    try {
                        settings_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cur_dir + "/config/settings.xml", true), StandardCharsets.UTF_8));
                        settings_writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                                "<settings>\n" +
                                "\t<warning_level>\n" +
                                "\t\t<value>2</value>\n" +
                                "\t\t<comments>显示DEBUG消息：0-无，1-错误，2-全部</comments>\n" +
                                "\t</warning_level>\n" +
                                "\t<source>\n" +
                                "\t\t<uri>jdbc:mysql://113.16.195.18:3307/cgx_zcgl?characterEncoding=UTF-8&amp;useUnicode=true&amp;useSSL=false</uri>\n" +
                                "\t\t<username>root</username>\n" +
                                "\t\t<password>12345678</password>\n" +
                                "\t</source>" +
                                "\t<target>\n" +
                                "\t\t<uri>jdbc:mysql://113.16.195.18:3307/cgx_zcgl2?characterEncoding=UTF-8&amp;useUnicode=true&amp;useSSL=false</uri>\n" +
                                "\t\t<username>root</username>\n" +
                                "\t\t<password>12345678</password>\n" +
                                "\t</target>\n" +
                                "\t<start_time>\n" +
                                "\t\t<hours>16</hours>\n" +
                                "\t\t<minutes>0</minutes>\n" +
                                "\t\t<seconds>0</seconds>\n" +
                                "\t\t<comment>更新初始时间：时-分-秒</comment>\n" +
                                "\t</start_time>\n" +
                                "\t<update_interval>\n" +
                                "\t\t<seconds>86400</seconds>\n" +
                                "\t\t<comment>更新间断时长（秒）。</comment>\n" +
                                "\t</update_interval>\n" +
                                "\t<debug_quick_start>\n" +
                                "\t\t<on>false</on>\n" +
                                "\t\t<comment>测试：立刻更新</comment>\n" +
                                "\t</debug_quick_start>" +
                                "\n</settings>");
                    } catch (IOException e) {
                        log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法创建设置文件:").append(e.getMessage()).append("\n");
                        e.printStackTrace();
                    } finally {
                        try {
                            if (settings_writer != null) settings_writer.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        // 每日定时运行时间设置
        // withHour():withMinute():withSecond():withNano() -- HH:MM:SS:NS
        // The scheduled time to be run each day
        int at_hours = 16, at_minutes = 0, at_seconds = 0;
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document settings_document = db.parse(new File(cur_dir + "/config/settings.xml"));
            settings_document.normalizeDocument();
            Element settings_doc_element = (Element) settings_document.getElementsByTagName("update_interval").item(0);
            interval_sec = Integer.parseInt(settings_doc_element.getElementsByTagName("seconds").item(0).getTextContent());
            settings_doc_element = (Element) settings_document.getElementsByTagName("start_time").item(0);
            at_hours = Integer.parseInt(settings_doc_element.getElementsByTagName("hours").item(0).getTextContent());
            at_minutes = Integer.parseInt(settings_doc_element.getElementsByTagName("minutes").item(0).getTextContent());
            at_seconds = Integer.parseInt(settings_doc_element.getElementsByTagName("seconds").item(0).getTextContent());
        } catch ( Exception e) {
            e.printStackTrace();
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法读取设置文件: ").append(e.getMessage()).append("\n");
        }

        log_writer.close();

        LocalDateTime scheduled = LocalDateTime.now().withHour(at_hours).withMinute(at_minutes).withSecond(at_seconds);

        // 计算第一次运行与当前的时间差
        // Calculate the difference between the current and the first scheduled run time.
        LocalDateTime now = LocalDateTime.now();

        if ( now.compareTo(scheduled) > 0 ) {
            scheduled = scheduled.plusDays(1); // schedule the next day
        }
        // The wait on first start up
        long init_wait_time_sec = 1;

        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document settings_document = db.parse(new File(cur_dir + "/config/settings.xml"));
            settings_document.normalizeDocument();
            Element settings_doc_element =  (Element) settings_document.getElementsByTagName("debug_quick_start").item(0);
            if ( settings_doc_element.getElementsByTagName("on").item(0).getTextContent().equals("True") |
                    settings_doc_element.getElementsByTagName("on").item(0).getTextContent().equals("true") ) {
                debug_mode = true;
            }
        } catch ( Exception e) {
            log_writer.append(datetime_formatter.format(LocalDateTime.now())).append(": 无法读取设置文件: ").append(e.getMessage()).append("\n");
            e.printStackTrace();
        }

        if (!debug_mode) init_wait_time_sec = Duration.between(now, scheduled).getSeconds();

        // 创建定时任务线
        // run scheduled task in new thread
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new dbsync_tables(),
                init_wait_time_sec,
                // executed once per day
                interval_sec,
                TimeUnit.SECONDS);
    }

}

