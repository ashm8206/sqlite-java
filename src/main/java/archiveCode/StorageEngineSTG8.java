package archiveCode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Storage Engine for reading SQLite database files.
 * Corrects issues with:
 * 1. Index scan leaf page processing (missing cell content reading)
 * 2. Interior table page B-tree navigation (wrong order)
 * 3. Filter value comparison (case sensitivity)
 */
public class StorageEngineSTG8 {

    // Constants for SQLite file structure
    private static final int HEADER_STRING_SIZE = 16;
    private static final int DATABASE_HEADER_SIZE = 100;
    private static final int FIRST_PAGE_OFFSET = DATABASE_HEADER_SIZE;

    // Page type constants
    private static final byte PAGE_TYPE_INTERIOR_INDEX = 0x02;
    private static final byte PAGE_TYPE_INTERIOR_TABLE = 0x05;
    private static final byte PAGE_TYPE_LEAF_INDEX = 0x0a;
    private static final byte PAGE_TYPE_LEAF_TABLE = 0x0d;

    // Header sizes
    private static final int LEAF_PAGE_HEADER_SIZE = 8;
    private static final int INTERIOR_PAGE_HEADER_SIZE = 12;

    private final RandomAccessFile databaseFile;
    private final int pageSize;

    // For varint reading
    private int varintBytesConsumedLast;
    private long varintValueLast;

    public StorageEngineSTG8(String databasePath) throws IOException {
        this.databaseFile = new RandomAccessFile(databasePath, "r");
        this.pageSize = readPageSize();
        validateDatabaseFile();
    }

    private int readPageSize() throws IOException {
        databaseFile.seek(HEADER_STRING_SIZE);
        byte[] pageSizeBytes = new byte[2];
        databaseFile.read(pageSizeBytes);
        short pageSizeShort = ByteBuffer.wrap(pageSizeBytes).getShort();
        return pageSizeShort == 1 ? 65536 : Short.toUnsignedInt(pageSizeShort);
    }

    private void validateDatabaseFile() throws IOException {
        databaseFile.seek(0);
        byte[] headerString = new byte[HEADER_STRING_SIZE];
        databaseFile.read(headerString);
        String header = new String(headerString, 0, 15, StandardCharsets.UTF_8);
        if (!"SQLite format 3".equals(header)) {
            throw new IOException("Not a valid SQLite 3 database file");
        }
    }

    public void close() throws IOException {
        if (databaseFile != null)
            databaseFile.close();
    }

    public Map<String, Object> getDatabaseInfo() throws IOException {
        Map<String, Object> info = new HashMap<>();
        info.put("pageSize", pageSize);
        info.put("tableCount", getTableNames().size());
        return info;
    }

    /* ====================== Schema Visitor Interface ====================== */

    private interface SchemaVisitor {
        void visitCell(long cellPosition) throws IOException;

        boolean isDone();
    }

    private class SchemaCollector implements SchemaVisitor {
        private final List<String> tableNames = new ArrayList<>();

        @Override
        public void visitCell(long cellPosition) throws IOException {
            String type = readTypeFromCell(cellPosition);
            if ("table".equals(type)) {
                String name = readNameFromCell(cellPosition);
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
        public void visitCell(long cellPosition) throws IOException {
            String type = readTypeFromCell(cellPosition);
            if ("table".equals(type)) {
                String name = readNameFromCell(cellPosition);
                if (targetName.equals(name)) {
                    foundRootPage = readRootPageFromCell(cellPosition);
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
        public void visitCell(long cellPosition) throws IOException {
            String type = readTypeFromCell(cellPosition);
            if ("table".equals(type)) {
                String name = readNameFromCell(cellPosition);
                if (targetName.equals(name)) {
                    foundSql = readSqlFromCell(cellPosition);
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
            this.tableName = tableName.toLowerCase();
            this.columnName = columnName.toLowerCase();
        }

        @Override
        public void visitCell(long cellPosition) throws IOException {
            String type = readTypeFromCell(cellPosition);
            if (!"index".equals(type)) {
                return;
            }

            String tblName = readTblNameFromCell(cellPosition).toLowerCase();
            String sql = readSqlFromCell(cellPosition);
            if (sql == null)
                return;

            sql = sql.toLowerCase();

            if (tblName.equals(tableName) && sql.contains(columnName)) {
                indexRootPage = readRootPageFromCell(cellPosition);
            }
        }

        @Override
        public boolean isDone() {
            return indexRootPage != -1;
        }
    }

    /* ====================== Public API Methods ====================== */

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
        return countRowsInBTree(rootPage);
    }

    private int countRowsInBTree(int pageNumber) throws IOException {
        byte[] page = readPage(pageNumber);
        byte pageType = page[pageNumber == 1 ? DATABASE_HEADER_SIZE : 0];

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            int offset = pageNumber == 1 ? DATABASE_HEADER_SIZE + 3 : 3;
            return readUnsignedShort(page, offset);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            int offset = pageNumber == 1 ? DATABASE_HEADER_SIZE : 0;
            int cellCount = readUnsignedShort(page, offset + 3);
            int rightMostPointer = readInt(page, offset + 8);
            int totalCount = 0;
            int cellPointerArrayOffset = offset + INTERIOR_PAGE_HEADER_SIZE;
            for (int i = 0; i < cellCount; i++) {
                int cellOffset = readUnsignedShort(page, cellPointerArrayOffset + i * 2);
                int childPage = readInt(page, cellOffset);
                totalCount += countRowsInBTree(childPage);
            }
            totalCount += countRowsInBTree(rightMostPointer);
            return totalCount;
        } else {
            throw new IOException("Unexpected page type: 0x" + String.format("%02x",
                    pageType));
        }
    }

    /*
     * ====================== Main Query Method with Filters
     * ======================
     */

    public List<String> getRowsWithFilters(String tableName, List<String> columnNames, String filterColumn,
            Object filterValue) throws IOException {

        if (columnNames.isEmpty()) {
            throw new IOException("No columns selected");
        }

        try {
            System.err.println("DEBUG: Query: table=" + tableName + ", columns=" + columnNames + ", filter="
                    + filterColumn + "=" + filterValue);

            // Get table schema
            String sql = getTableCreateSql(tableName);
            List<String> tableColumns = parseCreateTableColumns(sql);
            System.err.println("DEBUG: Found " + tableColumns.size() + " columns: " +
                    tableColumns);

            // Validate and map selected columns
            int[] targetIndices = new int[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
                int idx = tableColumns.indexOf(columnNames.get(i));
                if (idx == -1) {
                    throw new IOException("Column not found: " + columnNames.get(i) + " in table" + tableName);
                }
                targetIndices[i] = idx;
            }

            // Validate filter column
            int filterIndex = -1;
            if (filterColumn != null && !filterColumn.isEmpty()) {
                filterIndex = tableColumns.indexOf(filterColumn);
                if (filterIndex == -1) {
                    throw new IOException("Filter column not found: " + filterColumn + " in table" + tableName);
                }
            }

            int tableRoot = getTableRootPage(tableName);
            List<String> resultRows = new ArrayList<>();

            // TRY INDEX SCAN (only if filter exists and is string)
            if (filterColumn != null && filterValue instanceof String) {
                // System.err.println("DEBUG: Looking for index on " + filterColumn + "...");
                try {
                    IndexFinder indexFinder = new IndexFinder(tableName, filterColumn);
                    readSchemaPage(1, indexFinder);
                    int indexRoot = indexFinder.indexRootPage;

                    if (indexRoot != -1) {
                        // System.err.println("DEBUG: Found index root page: " + indexRoot);
                        List<Long> matchingRowids = new ArrayList<>();
                        traverseIndexForRowids(indexRoot, (String) filterValue, matchingRowids);
                        // System.err.println("DEBUG: Found " + matchingRowids.size() + " matching
                        // rowids via index");

                        // Fetch rows by rowid
                        for (long rowid : matchingRowids) {
                            fetchRowByRowid(tableRoot, rowid, tableColumns, targetIndices, resultRows);
                        }

                        // If we found results, return them
                        if (!resultRows.isEmpty()) {
                            return resultRows;
                        }
                    }
                    // } else {
                    // // System.err.println("DEBUG: No index found, falling back to table scan");
                    // }
                } catch (Exception indexEx) {
                    System.err.println(
                            "DEBUG: Index scan failed: " + indexEx.getMessage() + " - falling back to table scan");
                    indexEx.printStackTrace();
                }
            }

            // FALLBACK: Full table scan
            // System.err.println("DEBUG: Starting full table scan...");
            traverseBTreeForRows(tableRoot, tableColumns, targetIndices, filterIndex,
                    filterValue, resultRows);
            // System.err.println("DEBUG: Table scan complete, found " + resultRows.size() +
            // " rows");

            return resultRows;

        } catch (Exception e) {
            String msg = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.err.println("DEBUG ERROR in getRowsWithFilters: " + msg);
            e.printStackTrace();
            throw new IOException("getRowsWithFilters failed: " + msg, e);
        }
    }

    /* ====================== Index Scan Methods ====================== */

    private void traverseIndexForRowids(int pageNumber, String filterValue,
            List<Long> matchingRowids)
            throws IOException {
        if (pageNumber <= 0)
            return;

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);
        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();

        if (pageType == PAGE_TYPE_LEAF_INDEX) {
            processIndexLeafPage(pageNumber, filterValue, matchingRowids);
        } else if (pageType == PAGE_TYPE_INTERIOR_INDEX) {
            processIndexInteriorPage(pageNumber, filterValue, matchingRowids);
        }
    }

    /**
     * FIX #1: Properly read index leaf page cells
     * Index cells contain: [payload_size, record_header, indexed_values...,
     * rowid]
     */
    private void processIndexLeafPage(int pageNumber, String filterValue,
            List<Long> matchingRowids)
            throws IOException {
        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte(); // page type
        databaseFile.skipBytes(2); // freeblock start
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(LEAF_PAGE_HEADER_SIZE - 5);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);

            // Read the cell payload
            long payloadSize = readVarint();

            // Read record header
            long headerSize = readVarint();
            long remainingHeaderBytes = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remainingHeaderBytes > 0) {
                long st = readVarint();
                serialTypes.add((int) st);
                remainingHeaderBytes -= varintBytesConsumedLast;
            }

            // Parse all values in the index record
            // For a single-column index on TEXT, format is: [indexed_value, rowid]
            // First value: indexed column value
            Object indexedValue = parseValue(serialTypes.get(0));

            // Last value: rowid (integer)
            int rowidTypeIndex = serialTypes.size() - 1;
            Object rowidObj = parseValue(serialTypes.get(rowidTypeIndex));
            long rowid = ((Number) rowidObj).longValue();

            // FIX #2: Case-insensitive comparison for text values
            String indexedStr = valueToString(indexedValue);
            if (indexedStr.equalsIgnoreCase(filterValue)) {
                matchingRowids.add(rowid);
            }
        }
    }

    /**
     * FIX #3: Properly traverse interior index pages
     */
    private void processIndexInteriorPage(int pageNumber, String filterValue,
            List<Long> matchingRowids)
            throws IOException {
        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte(); // type
        databaseFile.skipBytes(2); // freeblock
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2); // content start
        databaseFile.readByte(); // fragmented
        int rightMost = readBigEndianInt();

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        // Traverse all child pages (we need to check all since we're looking for
        // matches)
        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            int childPage = readBigEndianInt();
            traverseIndexForRowids(childPage, filterValue, matchingRowids);
        }

        // Don't forget the rightmost child
        traverseIndexForRowids(rightMost, filterValue, matchingRowids);
    }

    /* ====================== Rowid-based Fetch Methods ====================== */

    private void fetchRowByRowid(int tableRoot, long targetRowid, List<String> tableColumns,
            int[] targetIndices, List<String> resultRows) throws IOException {
        traverseTableForRowid(tableRoot, targetRowid, tableColumns, targetIndices,
                resultRows);
    }

    private void traverseTableForRowid(int pageNumber, long targetRowid,
            List<String> tableColumns,
            int[] targetIndices, List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);
        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            processLeafTablePageForRowid(pageNumber, targetRowid, tableColumns,
                    targetIndices, resultRows);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            processInteriorTablePageForRowid(pageNumber, targetRowid, tableColumns,
                    targetIndices, resultRows);
        }
    }

    private void processLeafTablePageForRowid(int pageNumber, long targetRowid,
            List<String> tableColumns,
            int[] targetIndices, List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(LEAF_PAGE_HEADER_SIZE - 5);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        boolean[] useRowidForTarget = new boolean[targetIndices.length];
        for (int i = 0; i < targetIndices.length; i++) {
            int colIdx = targetIndices[i];
            String colName = tableColumns.get(colIdx).toLowerCase();
            useRowidForTarget[i] = colName.equals("id") || colName.equals("_id") ||
                    colName.equals("rowid");
        }

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            readVarint(); // payload
            long rowid = readVarint();
            if (rowid != targetRowid)
                continue;

            long headerSize = readVarint();
            long remaining = headerSize - varintBytesConsumedLast;
            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint());
                remaining -= varintBytesConsumedLast;
            }

            List<Object> storedValues = new ArrayList<>();
            for (int st : serialTypes) {
                storedValues.add(parseValue(st));
            }

            String[] selected = new String[targetIndices.length];
            for (int i = 0; i < targetIndices.length; i++) {
                if (useRowidForTarget[i]) {
                    selected[i] = String.valueOf(rowid);
                } else {
                    int colIdx = targetIndices[i];
                    selected[i] = colIdx < storedValues.size() ? valueToString(storedValues.get(colIdx)) : "";
                }
            }

            resultRows.add(String.join("|", selected));
            return; // found it
        }
    }

    /**
     * FIX #4: Correct B-tree navigation for interior table pages during rowid
     * lookup
     */
    private void processInteriorTablePageForRowid(int pageNumber, long targetRowid, List<String> tableColumns,
            int[] targetIndices, List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte();
        databaseFile.skipBytes(2); // freeblock
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2); // content start
        databaseFile.readByte(); // fragmented
        int rightMost = readBigEndianInt();

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        // B-tree interior node navigation:
        // Each cell contains: [left_child_page, key_rowid]
        // If target < key, go to left_child
        // If target >= key, continue to next cell or rightmost

        for (int i = 0; i < cellCount; i++) {
            int offset = cellOffsets[i];
            databaseFile.seek(pageStart + offset);
            int childPage = readBigEndianInt();
            long key = readVarint();

            // If target is less than this key, it must be in the left child
            if (targetRowid < key) {
                traverseTableForRowid(childPage, targetRowid, tableColumns, targetIndices,
                        resultRows);
                return;
            }
        }

        // If we didn't find it in any left child, it's in the rightmost
        traverseTableForRowid(rightMost, targetRowid, tableColumns, targetIndices,
                resultRows);
    }

    /* ====================== Full Table Scan Methods ====================== */

    private void traverseBTreeForRows(int pageNumber, List<String> tableColumns,
            int[] targetIndices, int filterIndex, Object filterValue,
            List<String> resultRows) throws IOException {

        if (pageNumber <= 0 || pageNumber > 100000) {
            return;
        }
        long pageStartPos = pageStart(pageNumber);
        if (pageStartPos >= databaseFile.length()) {
            return;
        }

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);
        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            processLeafTablePage(pageNumber, tableColumns, targetIndices, filterIndex,
                    filterValue, resultRows);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            processInteriorTablePage(pageNumber, tableColumns, targetIndices,
                    filterIndex, filterValue, resultRows);
        }
    }

    private void processLeafTablePage(int pageNumber, List<String> tableColumns,
            int[] targetIndices, int filterIndex, Object filterValue,
            List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageNumber == 1 ? pageStart + DATABASE_HEADER_SIZE : pageStart;

        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(LEAF_PAGE_HEADER_SIZE - 5);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        boolean[] useRowidForTarget = new boolean[targetIndices.length];
        for (int i = 0; i < targetIndices.length; i++) {
            int colIdx = targetIndices[i];
            if (colIdx < tableColumns.size()) {
                String colName = tableColumns.get(colIdx).toLowerCase();
                useRowidForTarget[i] = colName.equals("id") || colName.equals("_id") ||
                        colName.equals("rowid");
            }
        }

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            readVarint(); // payload size
            long rowid = readVarint();

            long headerSize = readVarint();
            long remaining = headerSize - varintBytesConsumedLast;
            List<Integer> serialTypes = new ArrayList<>();
            while (remaining > 0) {
                serialTypes.add((int) readVarint());
                remaining -= varintBytesConsumedLast;
            }

            List<Object> storedValues = new ArrayList<>();
            for (int st : serialTypes) {
                storedValues.add(parseValue(st));
            }

            // Build selected values
            String[] selected = new String[targetIndices.length];
            for (int i = 0; i < targetIndices.length; i++) {
                if (useRowidForTarget[i]) {
                    selected[i] = String.valueOf(rowid);
                } else {
                    int colIdx = targetIndices[i];
                    selected[i] = colIdx < storedValues.size() ? valueToString(storedValues.get(colIdx)) : "";
                }
            }

            // FIX #5: Case-insensitive filter comparison
            if (filterIndex >= 0 && filterValue != null) {
                Object cellValue = null;

                String filterCol = tableColumns.get(filterIndex);
                boolean isIdColumn = filterCol.toLowerCase().equals("id") ||
                        filterCol.toLowerCase().equals("_id") ||
                        filterCol.toLowerCase().equals("rowid");

                if (isIdColumn) {
                    cellValue = rowid;
                } else {
                    if (filterIndex < storedValues.size()) {
                        cellValue = storedValues.get(filterIndex);
                    }
                }

                String cellStr = valueToString(cellValue);
                String filterStr = valueToString(filterValue);

                // Case-insensitive comparison
                if (!cellStr.equalsIgnoreCase(filterStr)) {
                    continue;
                }
            }

            resultRows.add(String.join("|", selected));
        }
    }

    /**
     * FIX #6: Correct B-tree navigation for interior table pages during scan
     */
    private void processInteriorTablePage(int pageNumber, List<String> tableColumns,
            int[] targetIndices, int filterIndex, Object filterValue,
            List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte();
        databaseFile.skipBytes(2); // freeblock
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2); // content start
        databaseFile.readByte(); // fragmented
        int rightMostPointer = readBigEndianInt();

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        // Process cells in order (left to right)
        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            int childPage = readBigEndianInt();

            if (childPage <= 0 || childPage > 100000) {
                continue;
            }
            long childStart = pageStart(childPage);
            if (childStart >= databaseFile.length()) {
                continue;
            }

            try {
                readVarint(); // skip the rowid key
            } catch (Exception ignored) {
            }

            traverseBTreeForRows(childPage, tableColumns, targetIndices,
                    filterIndex, filterValue, resultRows);
        }

        // Don't forget the rightmost child
        if (rightMostPointer > 0 && rightMostPointer <= 100000) {
            traverseBTreeForRows(rightMostPointer, tableColumns, targetIndices,
                    filterIndex, filterValue, resultRows);
        }
    }

    /* ====================== Column Parsing ====================== */

    private List<String> parseCreateTableColumns(String sql) throws IOException {
        if (sql == null)
            throw new IOException("CREATE TABLE SQL is null");
        int lparen = sql.indexOf('(');
        int rparen = sql.lastIndexOf(')');
        if (lparen == -1 || rparen == -1 || lparen >= rparen)
            throw new IOException("Invalid CREATE TABLE SQL");

        String columnsStr = sql.substring(lparen + 1, rparen).trim();
        List<String> parts = splitColumns(columnsStr);
        List<String> columnNames = new ArrayList<>();

        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty())
                continue;
            String upper = trimmed.toUpperCase();
            if (upper.startsWith("PRIMARY KEY") || upper.startsWith("FOREIGN KEY") ||
                    upper.startsWith("UNIQUE") || upper.startsWith("CHECK") ||
                    upper.startsWith("CONSTRAINT"))
                continue;

            String[] words = trimmed.split("\\s+");
            if (words.length > 0) {
                String name = words[0].replaceAll("[`\"\\[\\]]", "");
                if (!name.isEmpty())
                    columnNames.add(name);
            }
        }

        if (columnNames.isEmpty())
            throw new IOException("No columns found");
        return columnNames;
    }

    private List<String> splitColumns(String columnsStr) {
        List<String> result = new ArrayList<>();
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
                if (i + 1 < columnsStr.length() && columnsStr.charAt(i + 1) == quoteChar) {
                    current.append(c).append(quoteChar);
                    i++;
                } else {
                    inQuote = false;
                    current.append(c);
                }
            } else if (!inQuote) {
                if (c == '(') {
                    parenDepth++;
                    current.append(c);
                } else if (c == ')') {
                    parenDepth--;
                    current.append(c);
                } else if (c == ',' && parenDepth == 0) {
                    result.add(current.toString());
                    current = new StringBuilder();
                } else
                    current.append(c);
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0)
            result.add(current.toString());
        return result;
    }

    private String valueToString(Object value) {
        if (value == null)
            return "";
        if (value instanceof byte[])
            return bytesToHex((byte[]) value);
        return String.valueOf(value);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /* ====================== Schema Cell Parsing ====================== */

    private String readTypeFromCell(long cellPosition) throws IOException {
        return (String) readFieldFromCell(cellPosition, 0);
    }

    private String readNameFromCell(long cellPosition) throws IOException {
        return (String) readFieldFromCell(cellPosition, 1);
    }

    private String readTblNameFromCell(long cellPosition) throws IOException {
        return (String) readFieldFromCell(cellPosition, 2);
    }

    private int readRootPageFromCell(long cellPosition) throws IOException {
        Number n = (Number) readFieldFromCell(cellPosition, 3);
        return n.intValue();
    }

    private String readSqlFromCell(long cellPosition) throws IOException {
        return (String) readFieldFromCell(cellPosition, 4);
    }

    private Object readFieldFromCell(long cellPosition, int fieldIndex) throws IOException {
        long savedPos = databaseFile.getFilePointer();
        databaseFile.seek(cellPosition);

        readVarint(); // payload size
        readVarint(); // rowid

        long headerSize = readVarint();
        long remaining = headerSize - varintBytesConsumedLast;

        List<Integer> serialTypes = new ArrayList<>();
        while (remaining > 0) {
            long st = readVarint();
            serialTypes.add((int) st);
            remaining -= varintBytesConsumedLast;
        }

        Object value = null;
        for (int i = 0; i <= fieldIndex; i++) {
            value = parseValue(serialTypes.get(i));
        }

        databaseFile.seek(savedPos);
        return value;
    }

    /* ====================== Schema Page Reading ====================== */

    private void readSchemaLeafPage(int pageNumber, SchemaVisitor visitor) throws IOException {
        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);

        byte pageType = databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(LEAF_PAGE_HEADER_SIZE - 5);

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        for (int offset : cellOffsets) {
            long cellPosition = pageStart + offset;
            visitor.visitCell(cellPosition);
            if (visitor.isDone()) {
                return;
            }
        }
    }

    private void readSchemaInteriorPage(int pageNumber, SchemaVisitor visitor)
            throws IOException {
        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);

        byte pageType = databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2);
        int rightMostPointer = readBigEndianInt();

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            int childPage = readBigEndianInt();
            readSchemaPage(childPage, visitor);
            if (visitor.isDone()) {
                return;
            }
        }

        readSchemaPage(rightMostPointer, visitor);
    }

    private void readSchemaPage(int pageNumber, SchemaVisitor visitor) throws IOException {
        long headerOffset = pageHeaderOffset(pageNumber);
        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();
        if (pageType == PAGE_TYPE_LEAF_TABLE)
            readSchemaLeafPage(pageNumber, visitor);
        else if (pageType == PAGE_TYPE_INTERIOR_TABLE)
            readSchemaInteriorPage(pageNumber, visitor);
        else
            throw new IOException("Unexpected schema page type");
    }

    /* ====================== Low-level Helpers ====================== */

    private long readVarint() throws IOException {
        long value = 0;
        varintBytesConsumedLast = 0;
        for (int i = 0; i < 9; i++) {
            int b = databaseFile.readUnsignedByte();
            varintBytesConsumedLast++;
            if (i < 8) {
                value = (value << 7) | (b & 0x7F);
                if ((b & 0x80) == 0) {
                    varintValueLast = value;
                    return value;
                }
            } else {
                value = (value << 8) | b;
                varintValueLast = value;
                return value;
            }
        }
        varintValueLast = value;
        return value;
    }

    private int readBigEndianInt() throws IOException {
        byte[] b = new byte[4];
        databaseFile.readFully(b);
        return ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) |
                (b[3] & 0xFF);
    }

    private int readBigEndianShort() throws IOException {
        byte[] b = new byte[2];
        databaseFile.readFully(b);
        return ((b[0] & 0xFF) << 8) | (b[1] & 0xFF);
    }

    private long pageStart(int pageNumber) {
        return (long) (pageNumber - 1) * pageSize;
    }

    private long pageHeaderOffset(int pageNumber) {
        return pageNumber == 1 ? pageStart(pageNumber) + FIRST_PAGE_OFFSET : pageStart(pageNumber);
    }

    private byte[] readPage(int pageNumber) throws IOException {
        byte[] buffer = new byte[pageSize];
        databaseFile.seek(pageStart(pageNumber));
        databaseFile.readFully(buffer);
        return buffer;
    }

    private static int readUnsignedShort(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private static int readInt(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24) | ((data[offset + 1] & 0xFF) << 16) |
                ((data[offset + 2] & 0xFF) << 8) | (data[offset + 3] & 0xFF);
    }

    private Object parseValue(int serialType) throws IOException {
        if (serialType >= 13 && serialType % 2 == 1) {
            int len = (serialType - 13) / 2;
            byte[] buf = new byte[len];
            databaseFile.readFully(buf);
            return new String(buf, StandardCharsets.UTF_8);
        } else if (serialType >= 12 && serialType % 2 == 0) {
            int len = (serialType - 12) / 2;
            byte[] buf = new byte[len];
            databaseFile.readFully(buf);
            return buf;
        } else if (serialType == 0)
            return null;
        else if (serialType == 8 || serialType == 9)
            return serialType - 8;

        int len = serialTypeLength(serialType);
        byte[] buf = new byte[8];
        databaseFile.readFully(buf, 0, len);
        long val = 0;
        for (int i = 0; i < len; i++)
            val = (val << 8) | (buf[i] & 0xFF);
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

    public int getPageSize() {
        return pageSize;
    }
}