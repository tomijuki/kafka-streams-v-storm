package fer.hr.jukic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.Timestamp;
import java.util.Objects;

public class Record {

    private static final int TIMESTAMP = 0;
    private static final int UID = 1;
    private static final int TO_NUMBER = 2;
    private static final int FROM_NUMBER = 3;
    private static final int DURATION = 4;
    @JsonProperty
    public Timestamp time;

    @JsonProperty
    public String uid;

    @JsonProperty
    public String toNumber;

    @JsonProperty
    public String fromNumber;

    @JsonProperty
    public Integer duration;

    public Record() {}

    public Record(String[] parametri) {
        this.uid = parametri[Record.UID];
        this.time = new Timestamp((long) (Double.parseDouble(parametri[Record.TIMESTAMP])));
        this.toNumber = parametri[Record.TO_NUMBER];
        this.fromNumber = parametri[Record.FROM_NUMBER];
        try {
            this.duration = Integer.parseInt(parametri[Record.DURATION]);
        } catch (NumberFormatException e) {
            this.duration = -1;
        }
    }

    @Override
    public String toString() {
        return "{" +
                "\"time\":\"" + time + '\"' +
                ", \"uid\":\"" + uid + '\"' +
                ", \"toNumber\":\"" + toNumber + '\"' +
                ", \"fromNumber\":\"" + fromNumber + '\"' +
                ", \"duration\":" + duration +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record record)) return false;

        if (!Objects.equals(toNumber, record.toNumber)) return false;
        if (!Objects.equals(fromNumber, record.fromNumber)) return false;
        if (Double.compare(record.duration, duration) != 0) return false;
        if (!Objects.equals(time, record.time)) return false;
        return Objects.equals(uid, record.uid);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = time != null ? time.hashCode() : 0;
        result = 31 * result + (uid != null ? uid.hashCode() : 0);
        result = 31 * result + (toNumber != null ? toNumber.hashCode() : 0);
        result = 31 * result + (fromNumber != null ? fromNumber.hashCode() : 0);;
        temp = Integer.hashCode(duration);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
