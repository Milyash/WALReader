package walentry;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by milya on 11.12.15.
 */
public class WALEntry implements Serializable {
    private static final long serialVersionUID = 6521735098267757690L;
    private String tableName;
    private List<Cell> cells;

    public WALEntry(WAL.Entry entry) {
        this.tableName = Bytes.toString(entry.getKey().getTablename().getName());
        this.cells = new ArrayList<Cell>();
        for (org.apache.hadoop.hbase.Cell cell : entry.getEdit().getCells()) {
            this.cells.add(new Cell(cell));
        }
    }

    public WALEntry(String tableName, List<Cell> cells) {
        this.tableName = tableName;
        this.cells = cells;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Cell> getCells() {
        return cells;
    }

    public void setCells(List<Cell> cells) {
        this.cells = cells;
    }

    @Override
    public String toString() {
        return "WALEntry{" +
                "tableName='" + tableName + '\'' +
                ", cells=" + cells +
                '}';
    }
}
