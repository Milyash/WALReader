package spout;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * Created by milya on 12.11.15.
 */
public class ColumnUpdate implements Serializable {
    private static final long serialVersionUID = 6529685098267757691L;
    private String columnFamily;
    private String column;
    private byte[] value;

    public ColumnUpdate(String columnFamily, String column, byte[] value) {
        this.columnFamily = columnFamily;
        this.column = column;
        this.value = value;
    }

    public ColumnUpdate(Cell walCell) {
        columnFamily = Bytes.toString(walCell.getFamily());
        column = Bytes.toString(walCell.getQualifier());
        value = CellUtil.cloneValue(walCell);
    }

    @Override
    public String toString() {
        return "spout.ColumnUpdate{" +
                "columnFamily='" + columnFamily + '\'' +
                ", column='" + column + '\'' +
                ", value=" + Bytes.toString(value) +
                '}';
    }
}
