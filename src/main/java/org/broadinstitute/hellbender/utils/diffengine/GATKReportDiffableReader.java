package org.broadinstitute.hellbender.utils.diffengine;

import org.broadinstitute.hellbender.utils.report.GATKReport;
import org.broadinstitute.hellbender.utils.report.GATKReportColumn;
import org.broadinstitute.hellbender.utils.report.GATKReportTable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;


/**
 * Class implementing diffnode reader for GATKReports
 */

// TODO Version check to be added at the report level

public final class GATKReportDiffableReader implements DiffableReader {
    @Override
    public String getName() {
        return "GATKReport";
    }

    @Override
    public DiffElement readFromFile(File file, int maxElementsToRead) {
        DiffNode root = DiffNode.rooted(file.getName());
        try {
            // one line reads the whole thing into memory
            GATKReport report = new GATKReport(file);

            for (GATKReportTable table : report.getTables()) {
                root.add(tableToNode(table, root));
            }

            return root.getBinding();
        } catch (Exception e) {
            return null;
        }
    }

    private DiffNode tableToNode(GATKReportTable table, DiffNode root) {
        DiffNode tableRoot = DiffNode.empty(table.getTableName(), root);

        tableRoot.add("Description", table.getTableDescription());
        tableRoot.add("NumberOfRows", table.getNumRows());

        for ( GATKReportColumn column : table.getColumnInfo() ) {
            DiffNode columnRoot = DiffNode.empty(column.getColumnName(), tableRoot);

            columnRoot.add("Width", column.getColumnFormat().getWidth());
            // NOTE: as the values are trimmed during parsing left/right alignment is not currently preserved
            columnRoot.add("Displayable", true);

            for ( int i = 0; i < table.getNumRows(); i++ ) {
                String name = column.getColumnName() + (i+1);
                columnRoot.add(name, table.get(i, column.getColumnName()).toString());
            }

            tableRoot.add(columnRoot);
        }

        return tableRoot;
    }

    @Override
    public boolean canRead(File file) {
        try {
            final String HEADER = GATKReport.GATKREPORT_HEADER_PREFIX;
            final char[] buff = new char[HEADER.length()];
            final FileReader FR = new FileReader(file);
            FR.read(buff, 0, HEADER.length());
            FR.close();
            String firstLine = new String(buff);
            return firstLine.startsWith(HEADER);
        } catch (IOException e) {
            return false;
        }
    }
}
