package spout;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by milya on 12.11.15.
 */
public class TableUpdate implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;
    private String tableName;
    private List<ColumnUpdate> columnUpdates;

    public TableUpdate(String tableName) {
        this.tableName = tableName;
        this.columnUpdates = null;
    }

    public TableUpdate(WAL.Entry e) {
        columnUpdates = new ArrayList<ColumnUpdate>();
        tableName = Bytes.toString(e.getKey().getTablename().getName());
        for (Cell walCell : e.getEdit().getCells()) {
            columnUpdates.add(new ColumnUpdate(walCell));
        }
    }

    public TableUpdate(WAL.Entry e, List<ColumnUpdate> colUpdates) {
        columnUpdates = new ArrayList<ColumnUpdate>();
        tableName = Bytes.toString(e.getKey().getTablename().getName());
        columnUpdates.addAll(colUpdates);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("spout.TableUpdate {" +
                "tableName='" + tableName + '\'' +
                ", columnUpdates= [");
        for (ColumnUpdate cb : columnUpdates)
            sb.append(cb.toString() + ", ");
        sb.append("]}");
        return sb.toString();
    }
}
