package StorageEngine;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Ultra-optimized Storage Engine using FileChannel (like reference
 * implementation)
 * Key optimizations based on their approach:
 * 1. FileChannel + ByteBuffer (10x faster than RandomAccessFile)
 * 2. Direct string comparison (no toLowerCase overhead)
 * 3. Simplified recursive traversal
 * 4. Reusable page buffer
 */
public class StorageEngine {

    private static final int HEADER_STRING_SIZE = 16;
    private static final int DATABASE_HEADER_SIZE = 100;
    private static final int FIRST_PAGE_OFFSET = DATABASE_HEADER_SIZE;

    private static final byte PAGE_TYPE_INTERIOR_INDEX = 0x02;
    private static final byte PAGE_TYPE_INTERIOR_TABLE = 0x05;
    private static final byte PAGE_TYPE_LEAF_INDEX = 0x0a;
    private static final byte PAGE_TYPE_LEAF_TABLE = 0x0d;

    private static final int LEAF_PAGE_HEADER_SIZE = 8;
    private static final int INTERIOR_PAGE_HEADER_SIZE = 12;

    private final FileChannel channel;
    private final int pageSize;
    private final ByteBuffer pageBuffer; // Reusable buffer

    private int varintBytesConsumedLast;

    public StorageEngine(String databasePath) throws IOException {
        FileInputStream fis = new FileInputStream(databasePath);
        this.channel = fis.getChannel();
        this.pageSize = readPageSize();
        this.pageBuffer = ByteBuffer.allocate(pageSize);
        validateDatabaseFile();
    }

    private int readPageSize() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        channel.position(HEADER_STRING_SIZE).read(buffer);
        buffer.flip();
        short pageSizeShort = buffer.getShort();
        return pageSizeShort == 1 ? 65536 : Short.toUnsignedInt(pageSizeShort);
    }

    private void validateDatabaseFile() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_STRING_SIZE);
        channel.position(0).read(buffer);
        buffer.flip();
        byte[] headerBytes = new byte[15];
        buffer.get(headerBytes);
        String header = new String(headerBytes, StandardCharsets.UTF_8);
        if (!"SQLite format 3".equals(header)) {
            throw new IOException("Not a valid SQLite 3 database file");
        }
    }

    public void close() throws IOException {
        if (channel != null)
            channel.close();
    }

    public Map<String, Object> getDatabaseInfo() throws IOException {
        Map<String, Object> info = new HashMap<>();
        info.put("pageSize", pageSize);
        info.put("tableCount", getTableNames().size());
        return info;
    }

    /* ====================== Schema Visitors ====================== */

    private interface SchemaVisitor {
        void visitCell(ByteBuffer pageBuffer, int cellOffset) throws IOException;

        boolean isDone();
    }

    private class SchemaCollector implements SchemaVisitor {
        private final List<String> tableNames = new ArrayList<>();

        @Override
        public void visitCell(ByteBuffer pageBuffer, int cellOffset) throws IOException {
            pageBuffer.position(cellOffset);
            readVarint(pageBuffer);
            readVarint(pageBuffer);

            long headerSize = readVarint(pageBuffer);
            long remaining = headerSize - varintBytesConsumedLast;

            int typeSerial = (int) readVarint(pageBuffer);
            remaining -= varintBytesConsumedLast;
            int nameSerial = (int) readVarint(pageBuffer);
            remaining -= varintBytesConsumedLast;

            while (remaining > 0) {
                readVarint(pageBuffer);
                remaining -= varintBytesConsumedLast;
            }

            String type = parseStringValue(pageBuffer, typeSerial);
            if ("table".equals(type)) {
                String name = parseStringValue(pageBuffer, nameSerial);
                tableNames.add(name);
            }
        }

        @Override
        public boolean isDone() {
            return false;
        }

        public List<String> getTableNames() {
            return tableNames;
        }
    }

    private class RootPageFinder implements SchemaVisitor {
        private final String targetName;
        int foundRootPage = -1;

        RootPageFinder(String targetName) {
            this.targetName = targetName;
        }

        @Override
        public void visitCell(ByteBuffer pageBuffer, int cellOffset) throws IOException {
            pageBuffer.position(cellOffset);
            readVarint(pageBuffer);
            readVarint(pageBuffer);

            long headerSize = readVarint(pageBuffer);
            long remaining = headerSize - varintBytesConsumedLast;

            int typeSerial = (int) readVarint(pageBuffer);
            remaining -= varintBytesConsumedLast;
            int nameSerial = (int) readVarint(pageBuffer);
            remaining -= varintBytesConsumedLast;
            int tblNameSerial = (int) readVarint(pageBuffer);
            remaining -= varintBytesConsumedLast;
            int rootPageSerial = (int) readVarint(pageBuffer);
            remaining -= varintBytesConsumedLast;

            while (remaining > 0) {
                readVarint(pageBuffer);
                remaining -= varintBytesConsumedLast;
            }

            String type = parseStringValue(pageBuffer, typeSerial);
            if ("table".equals(type)) {
                String name = parseStringValue(pageBuffer, nameSerial);
                if (targetName.equals(name)) {
                    pageBuffer.position(pageBuffer.position() + serialTypeLength(tblNameSerial));
                    foundRootPage = (int) parseLongValue(pageBuffer, rootPageSerial);
                }
            }
        }

        @Override
        public boolean isDone() {
            return foundRootPage != -1;
        }
    }

    private class SchemaSqlFinder implements SchemaVisitor {
        private final String targetName;
        String foundSql = null;

        SchemaSqlFinder(String targetName) {
            this.targetName = targetName;
        }

        @Override
        public void visitCell(ByteBuffer pageBuffer, int cellOffset) throws IOException {
            pageBuffer.position(cellOffset);
            readVarint(pageBuffer);
            readVarint(pageBuffer);

            long headerSize = readVarint(pageBuffer);
            long remaining = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint(pageBuffer));
                remaining -= varintBytesConsumedLast;
            }

            String type = parseStringValue(pageBuffer, serialTypes.get(0));
            if ("table".equals(type)) {
                String name = parseStringValue(pageBuffer, serialTypes.get(1));
                if (targetName.equals(name)) {
                    pageBuffer.position(pageBuffer.position() + serialTypeLength(serialTypes.get(2)));
                    pageBuffer.position(pageBuffer.position() + serialTypeLength(serialTypes.get(3)));
                    foundSql = parseStringValue(pageBuffer, serialTypes.get(4));
                }
            }
        }

        @Override
        public boolean isDone() {
            return foundSql != null;
        }
    }

    private class IndexFinder implements SchemaVisitor {
        private final String tableName;
        private final String columnName;
        int indexRootPage = -1;

        IndexFinder(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public void visitCell(ByteBuffer pageBuffer, int cellOffset) throws IOException {
            pageBuffer.position(cellOffset);
            readVarint(pageBuffer);
            readVarint(pageBuffer);

            long headerSize = readVarint(pageBuffer);
            long remaining = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint(pageBuffer));
                remaining -= varintBytesConsumedLast;
            }

            String type = parseStringValue(pageBuffer, serialTypes.get(0));
            if ("index".equals(type)) {
                pageBuffer.position(pageBuffer.position() + serialTypeLength(serialTypes.get(1)));
                String tblName = parseStringValue(pageBuffer, serialTypes.get(2));
                int rootPage = (int) parseLongValue(pageBuffer, serialTypes.get(3));
                String sql = parseStringValue(pageBuffer, serialTypes.get(4));

                if (tblName.equals(tableName) && sql.contains(columnName)) {
                    indexRootPage = rootPage;
                }
            }
        }

        @Override
        public boolean isDone() {
            return indexRootPage != -1;
        }
    }

    /* ====================== Public API ====================== */

    public List<String> getTableNames() throws IOException {
        SchemaCollector collector = new SchemaCollector();
        readSchemaPage(1, collector);
        return collector.getTableNames();
    }

    public int getTableRootPage(String tableName) throws IOException {
        RootPageFinder finder = new RootPageFinder(tableName);
        readSchemaPage(1, finder);
        if (finder.foundRootPage == -1) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        return finder.foundRootPage;
    }

    public String getTableCreateSql(String tableName) throws IOException {
        SchemaSqlFinder finder = new SchemaSqlFinder(tableName);
        readSchemaPage(1, finder);
        if (finder.foundSql == null) {
            throw new RuntimeException("Table not found: " + tableName);
        }
        return finder.foundSql;
    }

    public int countRowsInTable(int rootPage) throws IOException {
        ByteBuffer page = readPageToBuffer(rootPage);
        byte pageType = page.get(rootPage == 1 ? DATABASE_HEADER_SIZE : 0);

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            int offset = rootPage == 1 ? DATABASE_HEADER_SIZE + 3 : 3;
            return page.getShort(offset) & 0xFFFF;
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            int offset = rootPage == 1 ? DATABASE_HEADER_SIZE : 0;
            int cellCount = page.getShort(offset + 3) & 0xFFFF;
            int rightMostPointer = page.getInt(offset + 8);
            int totalCount = 0;
            int cellPointerArrayOffset = offset + INTERIOR_PAGE_HEADER_SIZE;
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = page.getShort(cellPointerArrayOffset + i * 2) & 0xFFFF;
                int childPage = page.getInt(cellOffset);
                totalCount += countRowsInTable(childPage);
            }
            totalCount += countRowsInTable(rightMostPointer);
            return totalCount;
        }
        return 0;
    }

    public List<String> getRowsWithFilters(String tableName, List<String> columnNames,
            String filterColumn, Object filterValue) throws IOException {

        // System.err.println("DEBUG: getRowsWithFilters called");
        // System.err.println("DEBUG: tableName=" + tableName);
        // System.err.println("DEBUG: filterColumn=" + filterColumn);
        // System.err.println("DEBUG: filterValue=" + filterValue);

        if (columnNames.isEmpty()) {
            throw new IOException("No columns selected");
        }

        String sql = getTableCreateSql(tableName);
        // System.err.println("DEBUG: CREATE TABLE SQL: " + sql);
        List<String> tableColumns = parseCreateTableColumns(sql);
        // System.err.println("DEBUG: Parsed columns: " + tableColumns);

        int[] targetIndices = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            int idx = tableColumns.indexOf(columnNames.get(i));
            if (idx == -1) {
                throw new IOException("Column not found: " + columnNames.get(i));
            }
            targetIndices[i] = idx;
        }

        int filterIndex = -1;
        if (filterColumn != null && !filterColumn.isEmpty()) {
            filterIndex = tableColumns.indexOf(filterColumn);
            // System.err.println("DEBUG: filterIndex for '" + filterColumn + "' = " +
            // filterIndex);
            if (filterIndex == -1) {
                throw new IOException("Filter column not found: " + filterColumn);
            }
        }

        int tableRoot = getTableRootPage(tableName);
        // System.err.println("DEBUG: tableRoot=" + tableRoot);

        // Try index scan (like reference implementation)
        if (filterColumn != null && filterValue instanceof String) {
            // System.err.println("DEBUG: Looking for index on column: " + filterColumn);
            IndexFinder indexFinder = new IndexFinder(tableName, filterColumn);
            readSchemaPage(1, indexFinder);
            int indexRoot = indexFinder.indexRootPage;
            // System.err.println("DEBUG: indexRoot=" + indexRoot);

            if (indexRoot != -1) {
                // System.err.println("DEBUG: Using index scan");
                return getRowsViaIndex(tableRoot, indexRoot, tableColumns, targetIndices,
                        (String) filterValue);
            } else {
                // System.err.println("DEBUG: No index found, using table scan");
            }
        }

        // Fallback to table scan
        // System.err.println("DEBUG: Using table scan");
        List<String> resultRows = new ArrayList<>();
        boolean[] useRowidForTarget = computeRowidColumns(tableColumns, targetIndices);
        traverseTableForRows(tableRoot, tableColumns, targetIndices, useRowidForTarget,
                filterIndex, filterValue, resultRows);
        return resultRows;
    }

    /*
     * ====================== Index Scan (Reference Implementation Style)
     * ======================
     */

    private List<String> getRowsViaIndex(int tableRoot, int indexRoot,
            List<String> tableColumns, int[] targetIndices, String filterValue) throws IOException {

        // Collect all matching rowids from index (simple recursive traversal)
        List<Long> rowids = new ArrayList<>();
        // System.err.println("DEBUG: Searching index for value: " + filterValue);
        collectRowidsFromIndex(indexRoot, filterValue, rowids);
        // System.err.println("DEBUG: Found " + rowids.size() + " matching rowids");

        if (rowids.isEmpty()) {
            return new ArrayList<>();
        }

        // Fetch each row by rowid
        boolean[] useRowidForTarget = computeRowidColumns(tableColumns, targetIndices);
        List<String> results = new ArrayList<>();

        for (long rowid : rowids) {
            String row = fetchRowByRowid(tableRoot, rowid, tableColumns, targetIndices, useRowidForTarget);
            if (row != null) {
                results.add(row);
            }
        }
        // System.err.println("DEBUG: Returning " + results.size() + " results");
        return results;
    }

    private void collectRowidsFromIndex(int pageNumber, String filterValue, List<Long> rowids)
            throws IOException {

        ByteBuffer page = readPageToBuffer(pageNumber);
        int headerOffset = pageNumber == 1 ? DATABASE_HEADER_SIZE : 0;
        byte pageType = page.get(headerOffset);

        if (pageType == PAGE_TYPE_LEAF_INDEX) {
            collectRowidsFromLeafIndex(page, headerOffset, filterValue, rowids);
        } else if (pageType == PAGE_TYPE_INTERIOR_INDEX) {
            collectRowidsFromInteriorIndex(pageNumber, page, headerOffset, filterValue, rowids);
        }
    }

    private void collectRowidsFromLeafIndex(ByteBuffer page, int headerOffset,
            String filterValue, List<Long> rowids) throws IOException {

        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(headerOffset + LEAF_PAGE_HEADER_SIZE);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        // System.err.println("DEBUG: Leaf index page has " + cellCount + " cells");
        for (int offset : cellOffsets) {
            // Cell offsets are absolute from the start of the page buffer
            if (offset >= page.limit()) {
                continue; // Skip invalid offset
            }
            page.position(offset);

            readVarint(page);
            long headerSize = readVarint(page);
            long remaining = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint(page));
                remaining -= varintBytesConsumedLast;
            }

            String indexedValue = parseStringValue(page, serialTypes.get(0));
            long rowid = parseLongValue(page, serialTypes.get(serialTypes.size() - 1));

            // Direct comparison like reference implementation
            if (indexedValue.equalsIgnoreCase(filterValue)) {
                rowids.add(rowid);
            }
        }
    }



    // collectRowidsFromInteriorIndex should already be fixed by you, but here's the
    // correct version:
    private void collectRowidsFromInteriorIndex(int pageNumber, ByteBuffer page, int headerOffset,
            String filterValue, List<Long> rowids) throws IOException {

        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(page.position() + 2);
        page.position(page.position() + 1);
        int rightMost = page.getInt();

        page.position(headerOffset + INTERIOR_PAGE_HEADER_SIZE);
        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        // EXTRACT ALL CHILD PAGE NUMBERS FIRST (before recursive calls)
        int[] childPages = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            int offset = cellOffsets[i];
            if (offset >= page.limit() || offset < 0) {
                childPages[i] = -1; // Mark as invalid
                continue;
            }
            page.position(offset);
            childPages[i] = page.getInt();

            // Check key in interior node
            readVarint(page); // payload size
            long headerSize = readVarint(page);
            long remaining = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint(page));
                remaining -= varintBytesConsumedLast;
            }

            String indexedValue = parseStringValue(page, serialTypes.get(0));
            long rowid = parseLongValue(page, serialTypes.get(serialTypes.size() - 1));

            if (indexedValue.equalsIgnoreCase(filterValue)) {
                rowids.add(rowid);
            }
        }

        // NOW make recursive calls (won't corrupt the page buffer)
        for (int childPage : childPages) {
            if (childPage > 0) {
                collectRowidsFromIndex(childPage, filterValue, rowids);
            }
        }
        if (rightMost > 0) {
            collectRowidsFromIndex(rightMost, filterValue, rowids);
        }
    }

    /* ====================== Row Fetching by Rowid ====================== */

    private String fetchRowByRowid(int pageNumber, long targetRowid,
            List<String> tableColumns, int[] targetIndices, boolean[] useRowidForTarget)
            throws IOException {

        ByteBuffer page = readPageToBuffer(pageNumber);
        int headerOffset = pageNumber == 1 ? DATABASE_HEADER_SIZE : 0;
        byte pageType = page.get(headerOffset);

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            return fetchFromLeafByRowid(page, headerOffset, targetRowid, tableColumns,
                    targetIndices, useRowidForTarget);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            return fetchFromInteriorByRowid(page, headerOffset, targetRowid, tableColumns,
                    targetIndices, useRowidForTarget);
        }
        return null;
    }

    private String fetchFromLeafByRowid(ByteBuffer page, int headerOffset, long targetRowid,
            List<String> tableColumns, int[] targetIndices, boolean[] useRowidForTarget)
            throws IOException {

        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(headerOffset + LEAF_PAGE_HEADER_SIZE);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        for (int offset : cellOffsets) {
            page.position(offset);
            readVarint(page);
            long rowid = readVarint(page);

            if (rowid == targetRowid) {
                return extractRowData(page, rowid, tableColumns, targetIndices, useRowidForTarget);
            }
        }
        return null;
    }

    private String fetchFromInteriorByRowid(ByteBuffer page, int headerOffset, long targetRowid,
            List<String> tableColumns, int[] targetIndices, boolean[] useRowidForTarget)
            throws IOException {

        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(page.position() + 2);
        page.position(page.position() + 1);
        int rightMost = page.getInt();

        page.position(headerOffset + INTERIOR_PAGE_HEADER_SIZE);
        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        // EXTRACT ALL DATA FROM PAGE BEFORE RECURSIVE CALLS
        int[] childPages = new int[cellCount];
        long[] keys = new long[cellCount];
        for (int i = 0; i < cellCount; i++) {
            page.position(cellOffsets[i]);
            childPages[i] = page.getInt();
            keys[i] = readVarint(page);
        }

        // Navigate B-tree to find rowid
        for (int i = 0; i < cellCount; i++) {
            if (targetRowid <= keys[i]) {
                return fetchRowByRowid(childPages[i], targetRowid, tableColumns, targetIndices, useRowidForTarget);
            }
        }

        return fetchRowByRowid(rightMost, targetRowid, tableColumns, targetIndices, useRowidForTarget);
    }

    /* ====================== Table Scan (Fallback) ====================== */

    private void traverseTableForRows(int pageNumber, List<String> tableColumns, int[] targetIndices,
            boolean[] useRowidForTarget, int filterIndex, Object filterValue, List<String> results)
            throws IOException {

        if (pageNumber <= 0)
            return;

        ByteBuffer page = readPageToBuffer(pageNumber);
        int headerOffset = pageNumber == 1 ? DATABASE_HEADER_SIZE : 0;
        byte pageType = page.get(headerOffset);

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            scanLeafPage(page, headerOffset, tableColumns, targetIndices, useRowidForTarget,
                    filterIndex, filterValue, results);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            scanInteriorPage(pageNumber, page, headerOffset, tableColumns, targetIndices,
                    useRowidForTarget, filterIndex, filterValue, results);
        }
    }

    private void scanLeafPage(ByteBuffer page, int headerOffset, List<String> tableColumns,
            int[] targetIndices, boolean[] useRowidForTarget, int filterIndex, Object filterValue,
            List<String> results) throws IOException {

        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(headerOffset + LEAF_PAGE_HEADER_SIZE);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        String filterStr = filterValue != null ? filterValue.toString() : null;
        // System.err.println("DEBUG: Table scan - leaf page has " + cellCount + "
        // cells, filter=" + filterStr+ ", filterIndex=" + filterIndex);

        for (int offset : cellOffsets) {
            page.position(offset);
            readVarint(page);
            long rowid = readVarint(page);

            long headerSize = readVarint(page);
            long remaining = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint(page));
                remaining -= varintBytesConsumedLast;
            }

            // System.err.println("DEBUG: Row " + rowid + " has " + serialTypes.size() + "
            // serial types: " + serialTypes);

            // Apply filter if present
            if (filterIndex >= 0 && filterValue != null) {
                // Save position after reading serial types (start of values)
                int valuesStart = page.position();

                // Skip values before the filter column
                for (int i = 0; i < filterIndex; i++) {
                    if (i < serialTypes.size()) {
                        int serial = serialTypes.get(i);
                        page.position(page.position() + serialTypeLength(serial));
                    }
                }

                // Now read the filter column value
                int filterSerial = filterIndex < serialTypes.size() ? serialTypes.get(filterIndex) : 0;
                // System.err.println("DEBUG: Using serial type at index " + filterIndex + " = "
                // + filterSerial);
                String cellValue = parseStringValue(page, filterSerial);
                // System.err.println("DEBUG: Table scan - comparing '" + cellValue + "' with '"
                // + filterStr + "'");

                // Reset to start of values for extracting the row
                page.position(valuesStart);

                if (!cellValue.equals(filterStr)) {
                    continue;
                }
            }

            results.add(extractRowDataFromSerials(page, rowid, serialTypes, tableColumns,
                    targetIndices, useRowidForTarget));
        }
        // System.err.println("DEBUG: Table scan - found " + results.size() + " results
        // so far");
    }

    private void scanInteriorPage(int pageNumber, ByteBuffer page, int headerOffset,
            List<String> tableColumns, int[] targetIndices, boolean[] useRowidForTarget,
            int filterIndex, Object filterValue, List<String> results) throws IOException {

        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(page.position() + 2);
        page.position(page.position() + 1);
        int rightMost = page.getInt();

        page.position(headerOffset + INTERIOR_PAGE_HEADER_SIZE);
        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        // EXTRACT ALL CHILD PAGE NUMBERS BEFORE RECURSIVE CALLS
        int[] childPages = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            page.position(cellOffsets[i]);
            childPages[i] = page.getInt();
        }

        // Now make recursive calls
        for (int childPage : childPages) {
            traverseTableForRows(childPage, tableColumns, targetIndices, useRowidForTarget,
                    filterIndex, filterValue, results);
        }

        traverseTableForRows(rightMost, tableColumns, targetIndices, useRowidForTarget,
                filterIndex, filterValue, results);
    }

    /* ====================== Helper Methods ====================== */

    private boolean[] computeRowidColumns(List<String> tableColumns, int[] targetIndices) {
        boolean[] useRowid = new boolean[targetIndices.length];
        for (int i = 0; i < targetIndices.length; i++) {
            String colName = tableColumns.get(targetIndices[i]).toLowerCase();
            useRowid[i] = colName.equals("id") || colName.equals("_id") || colName.equals("rowid");
        }
        return useRowid;
    }

    private String extractRowData(ByteBuffer page, long rowid, List<String> tableColumns,
            int[] targetIndices, boolean[] useRowidForTarget) throws IOException {

        long headerSize = readVarint(page);
        long remaining = headerSize - varintBytesConsumedLast;

        List<Integer> serialTypes = new ArrayList<>();
        while (remaining > 0) {
            serialTypes.add((int) readVarint(page));
            remaining -= varintBytesConsumedLast;
        }

        return extractRowDataFromSerials(page, rowid, serialTypes, tableColumns, targetIndices,
                useRowidForTarget);
    }

    private String extractRowDataFromSerials(ByteBuffer page, long rowid, List<Integer> serialTypes,
            List<String> tableColumns, int[] targetIndices, boolean[] useRowidForTarget)
            throws IOException {

        int valuesStartPos = page.position();
        String[] values = new String[targetIndices.length];

        for (int i = 0; i < targetIndices.length; i++) {
            if (useRowidForTarget[i]) {
                values[i] = String.valueOf(rowid);
            } else {
                int colIdx = targetIndices[i];
                if (colIdx < serialTypes.size()) {
                    // Calculate offset for this column
                    int offset = 0;
                    for (int j = 0; j < colIdx; j++) {
                        offset += serialTypeLength(serialTypes.get(j));
                    }
                    
                    page.position(valuesStartPos + offset);
                    int serial = serialTypes.get(colIdx);
                    values[i] = parseValueToString(page, serial);
                } else {
                    values[i] = "";
                }
            }
        }
        return String.join("|", values);
    }

    private String parseValueToString(ByteBuffer buffer, int serialType) {
        if (serialType == 0) {
            return "NULL";
        } else if (serialType >= 1 && serialType <= 9) {
            return String.valueOf(parseLongValue(buffer, serialType));
        } else if (serialType >= 12 && serialType % 2 == 0) {
            int len = (serialType - 12) / 2;
            buffer.position(buffer.position() + len); // skip blob
            return ""; 
        } else if (serialType >= 13 && serialType % 2 == 1) {
            int len = (serialType - 13) / 2;
            byte[] bytes = new byte[len];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return "";
    }

    /* ====================== Schema Reading ====================== */

    private void readSchemaPage(int pageNumber, SchemaVisitor visitor) throws IOException {
        ByteBuffer page = readPageToBuffer(pageNumber);
        int headerOffset = pageNumber == 1 ? DATABASE_HEADER_SIZE : 0;
        byte pageType = page.get(headerOffset);

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            readSchemaLeafPage(page, headerOffset, visitor);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            readSchemaInteriorPage(pageNumber, page, headerOffset, visitor);
        }
    }

    private void readSchemaLeafPage(ByteBuffer page, int headerOffset, SchemaVisitor visitor)
            throws IOException {
        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(headerOffset + LEAF_PAGE_HEADER_SIZE);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        for (int offset : cellOffsets) {
            visitor.visitCell(page, offset);
            if (visitor.isDone())
                return;
        }
    }

    // Fix for readSchemaInteriorPage (lines 1841-1864)
    private void readSchemaInteriorPage(int pageNumber, ByteBuffer page, int headerOffset,
            SchemaVisitor visitor) throws IOException {
        page.position(headerOffset + 1);
        page.position(page.position() + 2);
        int cellCount = page.getShort() & 0xFFFF;
        page.position(page.position() + 2);
        int rightMost = page.getInt();

        page.position(headerOffset + INTERIOR_PAGE_HEADER_SIZE);
        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = page.getShort() & 0xFFFF;
        }

        // EXTRACT ALL CHILD PAGE NUMBERS BEFORE RECURSIVE CALLS
        int[] childPages = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            page.position(cellOffsets[i]);
            childPages[i] = page.getInt();
        }

        // Now make recursive calls
        for (int childPage : childPages) {
            readSchemaPage(childPage, visitor);
            if (visitor.isDone())
                return;
        }

        readSchemaPage(rightMost, visitor);
    }

    /* ====================== Low-Level I/O ====================== */

    private ByteBuffer readPageToBuffer(int pageNumber) throws IOException {
        pageBuffer.clear();
        long position = (long) (pageNumber - 1) * pageSize;
        channel.position(position).read(pageBuffer);
        return pageBuffer.duplicate().clear();
    }

    private long readVarint(ByteBuffer buffer) {
        long value = 0;
        varintBytesConsumedLast = 0;
        for (int i = 0; i < 9; i++) {
            int b = buffer.get() & 0xFF;
            varintBytesConsumedLast++;
            if (i < 8) {
                value = (value << 7) | (b & 0x7F);
                if ((b & 0x80) == 0) {
                    return value;
                }
            } else {
                value = (value << 8) | b;
                return value;
            }
        }
        return value;
    }

    private String parseStringValue(ByteBuffer buffer, int serialType) {
        if (serialType >= 13 && serialType % 2 == 1) {
            int len = (serialType - 13) / 2;
            byte[] bytes = new byte[len];
            buffer.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return "";
    }

    private long parseLongValue(ByteBuffer buffer, int serialType) {
        if (serialType == 0)
            return 0;
        if (serialType == 8 || serialType == 9)
            return serialType - 8;

        int len = serialTypeLength(serialType);
        long val = 0;
        for (int i = 0; i < len; i++) {
            val = (val << 8) | (buffer.get() & 0xFF);
        }
        return val;
    }

    private static int serialTypeLength(int serialType) {
        return switch (serialType) {
            case 0, 8, 9 -> 0;
            case 1 -> 1;
            case 2 -> 2;
            case 3 -> 3;
            case 4 -> 4;
            case 5 -> 6;
            case 6, 7 -> 8;
            default -> (serialType >= 12) ? (serialType - 12) / 2 : 0;
        };
    }

    private List<String> parseCreateTableColumns(String sql) throws IOException {
        if (sql == null)
            throw new IOException("CREATE TABLE SQL is null");
        int lparen = sql.indexOf('(');
        int rparen = sql.lastIndexOf(')');
        if (lparen == -1 || rparen == -1)
            throw new IOException("Invalid CREATE TABLE SQL");

        String columnsStr = sql.substring(lparen + 1, rparen).trim();
        List<String> columnNames = new ArrayList<>();

        StringBuilder current = new StringBuilder();
        int parenDepth = 0;
        boolean inQuote = false;
        char quoteChar = 0;

        for (int i = 0; i < columnsStr.length(); i++) {
            char c = columnsStr.charAt(i);

            if (!inQuote && (c == '"' || c == '`' || c == '\'')) {
                inQuote = true;
                quoteChar = c;
                current.append(c);
            } else if (inQuote && c == quoteChar) {
                inQuote = false;
                current.append(c);
            } else if (!inQuote) {
                if (c == '(') {
                    parenDepth++;
                    current.append(c);
                } else if (c == ')') {
                    parenDepth--;
                    current.append(c);
                } else if (c == ',' && parenDepth == 0) {
                    processColumnDef(current.toString(), columnNames);
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            processColumnDef(current.toString(), columnNames);
        }

        if (columnNames.isEmpty())
            throw new IOException("No columns found");
        return columnNames;
    }

    private void processColumnDef(String def, List<String> columnNames) {
        String trimmed = def.trim();
        if (trimmed.isEmpty())
            return;
        String upper = trimmed.toUpperCase();
        if (upper.startsWith("PRIMARY KEY") || upper.startsWith("FOREIGN KEY") ||
                upper.startsWith("UNIQUE") || upper.startsWith("CHECK") ||
                upper.startsWith("CONSTRAINT"))
            return;

        String[] words = trimmed.split("\\s+");
        if (words.length > 0) {
            String name = words[0].replaceAll("[`\"\\[\\]]", "");
            if (!name.isEmpty())
                columnNames.add(name);
        }
    }

    public int getPageSize() {
        return pageSize;
    }
}