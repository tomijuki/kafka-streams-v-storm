package fer.hr.jukic.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogUtil {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
    private static final String LOG_FILE_PATH = "/data/output.log";
    private static final boolean OPEN = true;

    /**
     * Write log message to a log file
     * @param o
     * @param msg
     */
    public static void write2Log(Object o, String msg) {
        if (!OPEN) {
            return;
        }

        try {
            Date now = new Date();
            String currrentTime = sdf.format(now);
            String thread = "Thread-" + Thread.currentThread().getId();
            String oname = o.getClass().getSimpleName() + ":" + o.hashCode();
            String prefix = "[" + currrentTime + "," + thread + "," + oname + "] ";

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
                writer.write(prefix + msg + System.getProperty("line.separator"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
