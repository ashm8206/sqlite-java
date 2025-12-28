package archiveCode;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Storage Engine for reading SQLite database files.
 * Fast index scans with batch processing - May work Partially Not sure!
 */
public class StorageEngineTest9File {

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

    // Cache for frequently accessed data
    private final Map<Integer, byte[]> pageCache = new HashMap<>();
    private static final int MAX_CACHE_SIZE = 100;

    public StorageEngineTest9File(String databasePath) throws IOException {
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

    /* ====================== OPTIMIZED Query Method ====================== */

    public List<String> getRowsWithFilters(String tableName, List<String> columnNames,
            String filterColumn, Object filterValue) throws IOException {

        if (columnNames.isEmpty()) {
            throw new IOException("No columns selected");
        }

        // Get table schema
        String sql = getTableCreateSql(tableName);
        List<String> tableColumns = parseCreateTableColumns(sql);

        // Validate and map selected columns
        int[] targetIndices = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            int idx = tableColumns.indexOf(columnNames.get(i));
            if (idx == -1) {
                throw new IOException("Column not found: " + columnNames.get(i));
            }
            targetIndices[i] = idx;
        }

        // Validate filter column
        int filterIndex = -1;
        if (filterColumn != null && !filterColumn.isEmpty()) {
            filterIndex = tableColumns.indexOf(filterColumn);
            if (filterIndex == -1) {
                throw new IOException("Filter column not found: " + filterColumn);
            }
        }

        int tableRoot = getTableRootPage(tableName);

        // OPTIMIZATION: Try index scan FIRST for string filters
        if (filterColumn != null && filterValue instanceof String) {
            IndexFinder indexFinder = new IndexFinder(tableName, filterColumn);
            readSchemaPage(1, indexFinder);
            int indexRoot = indexFinder.indexRootPage;

            if (indexRoot != -1) {
                // Use optimized batch index scan
                return getRowsViaIndexBatch(tableRoot, indexRoot, tableColumns,
                        targetIndices,
                        (String) filterValue);
            }
        }

        // Fallback to table scan
        List<String> resultRows = new ArrayList<>();
        traverseBTreeForRows(tableRoot, tableColumns, targetIndices, filterIndex,
                filterValue, resultRows);
        return resultRows;
    }

    /*
     * ====================== OPTIMIZED Batch Index Scan ======================
     */

    /**
     * OPTIMIZATION: Batch process rowids for better cache locality
     * Instead of fetching rows one-by-one, we:
     * 1. Collect ALL matching rowids from index
     * 2. Sort them (improves sequential page access)
     * 3. Batch fetch from table pages
     */
    private List<String> getRowsViaIndexBatch(int tableRoot, int indexRoot,
            List<String> tableColumns, int[] targetIndices,
            String filterValue) throws IOException {

        // Step 1: Collect all matching rowids from index
        List<Long> matchingRowids = new ArrayList<>();
        traverseIndexForRowids(indexRoot, filterValue, matchingRowids);

        if (matchingRowids.isEmpty()) {
            return new ArrayList<>();
        }

        // Step 2: Sort rowids for better page locality
        Collections.sort(matchingRowids);

        // Step 3: Batch fetch using page-aware traversal
        List<String> resultRows = new ArrayList<>();
        fetchRowsBatch(tableRoot, matchingRowids, tableColumns, targetIndices,
                resultRows);

        return resultRows;
    }

    /**
     * OPTIMIZATION: Fetch multiple rows from the same page in one pass
     * Reduces page reads by processing all rowids from the same page together
     */
    private void fetchRowsBatch(int pageNumber, List<Long> sortedRowids,
            List<String> tableColumns, int[] targetIndices,
            List<String> resultRows) throws IOException {

        if (sortedRowids.isEmpty())
            return;

        long headerOffset = pageHeaderOffset(pageNumber);
        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();

        if (pageType == PAGE_TYPE_LEAF_TABLE) {
            fetchFromLeafPageBatch(pageNumber, sortedRowids, tableColumns, targetIndices,
                    resultRows);
        } else if (pageType == PAGE_TYPE_INTERIOR_TABLE) {
            fetchFromInteriorPageBatch(pageNumber, sortedRowids, tableColumns,
                    targetIndices, resultRows);
        }
    }

    /**
     * OPTIMIZATION: Process multiple rowids from a leaf page in one scan
     */
    private void fetchFromLeafPageBatch(int pageNumber, List<Long> sortedRowids,
            List<String> tableColumns, int[] targetIndices,
            List<String> resultRows) throws IOException {

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

        // Pre-compute which columns use rowid
        boolean[] useRowidForTarget = new boolean[targetIndices.length];
        for (int i = 0; i < targetIndices.length; i++) {
            int colIdx = targetIndices[i];
            String colName = tableColumns.get(colIdx).toLowerCase();
            useRowidForTarget[i] = colName.equals("id") || colName.equals("_id") ||
                    colName.equals("rowid");
        }

        // Convert to set for O(1) lookup
        Set<Long> rowidSet = new HashSet<>(sortedRowids);

        // Single pass through all cells
        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            readVarint(); // payload
            long rowid = readVarint();

            if (!rowidSet.contains(rowid)) {
                continue; // Skip if not in our target list
            }

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

            // Early exit if we've found all
            if (resultRows.size() == sortedRowids.size()) {
                return;
            }
        }
    }

    /**
     * OPTIMIZATION: Route rowids to correct child pages in batches
     */
    private void fetchFromInteriorPageBatch(int pageNumber, List<Long> sortedRowids,
            List<String> tableColumns, int[] targetIndices,
            List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2);
        databaseFile.readByte();
        int rightMost = readBigEndianInt();

        // Read all cell pointers and keys
        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        // Build routing table: which rowids go to which child page
        List<List<Long>> childRowids = new ArrayList<>(cellCount + 1);
        for (int i = 0; i <= cellCount; i++) {
            childRowids.add(new ArrayList<>());
        }

        // Read keys and route rowids
        long[] keys = new long[cellCount];
        int[] childPages = new int[cellCount];

        for (int i = 0; i < cellCount; i++) {
            databaseFile.seek(pageStart + cellOffsets[i]);
            childPages[i] = readBigEndianInt();
            keys[i] = readVarint();
        }

        // Route each rowid to the appropriate child
        for (long rowid : sortedRowids) {
            int childIndex = cellCount; // default to rightmost
            for (int i = 0; i < cellCount; i++) {
                if (rowid < keys[i]) {
                    childIndex = i;
                    break;
                }
            }
            childRowids.get(childIndex).add(rowid);
        }

        // Recursively fetch from child pages
        for (int i = 0; i < cellCount; i++) {
            if (!childRowids.get(i).isEmpty()) {
                fetchRowsBatch(childPages[i], childRowids.get(i), tableColumns,
                        targetIndices, resultRows);
            }
        }

        // Process rightmost child
        if (!childRowids.get(cellCount).isEmpty()) {
            fetchRowsBatch(rightMost, childRowids.get(cellCount), tableColumns,
                    targetIndices, resultRows);
        }
    }

    /* ====================== Index Scan Methods ====================== */

    private void traverseIndexForRowids(int pageNumber, String filterValue,
            List<Long> matchingRowids)
            throws IOException {
        if (pageNumber <= 0)
            return;

        long headerOffset = pageHeaderOffset(pageNumber);
        databaseFile.seek(headerOffset);
        byte pageType = databaseFile.readByte();

        if (pageType == PAGE_TYPE_LEAF_INDEX) {
            processIndexLeafPage(pageNumber, filterValue, matchingRowids);
        } else if (pageType == PAGE_TYPE_INTERIOR_INDEX) {
            processIndexInteriorPage(pageNumber, filterValue, matchingRowids);
        }
    }

    private void processIndexLeafPage(int pageNumber, String filterValue,
            List<Long> matchingRowids)
            throws IOException {
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

        // Pre-lowercase filter value for comparison
        String filterLower = filterValue.toLowerCase();

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);

            long payloadSize = readVarint();
            long headerSize = readVarint();
            long remainingHeaderBytes = headerSize - varintBytesConsumedLast;

            List<Integer> serialTypes = new ArrayList<>();
            while (remainingHeaderBytes > 0) {
                long st = readVarint();
                serialTypes.add((int) st);
                remainingHeaderBytes -= varintBytesConsumedLast;
            }

            // First value: indexed column
            Object indexedValue = parseValue(serialTypes.get(0));

            // Last value: rowid
            int rowidTypeIndex = serialTypes.size() - 1;
            Object rowidObj = parseValue(serialTypes.get(rowidTypeIndex));
            long rowid = ((Number) rowidObj).longValue();

            // Case-insensitive comparison
            String indexedStr = valueToString(indexedValue).toLowerCase();
            if (indexedStr.equals(filterLower)) {
                matchingRowids.add(rowid);
            }
        }
    }

    private void processIndexInteriorPage(int pageNumber, String filterValue,
            List<Long> matchingRowids)
            throws IOException {
        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2);
        databaseFile.readByte();
        int rightMost = readBigEndianInt();

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            int childPage = readBigEndianInt();
            traverseIndexForRowids(childPage, filterValue, matchingRowids);
        }

        traverseIndexForRowids(rightMost, filterValue, matchingRowids);
    }

    /*
     * ====================== Full Table Scan Methods (Fallback)
     * ======================
     */

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
            if (colIdx < tableColumns.size()) {
                String colName = tableColumns.get(colIdx).toLowerCase();
                useRowidForTarget[i] = colName.equals("id") || colName.equals("_id") ||
                        colName.equals("rowid");
            }
        }

        String filterLower = filterValue != null ? valueToString(filterValue).toLowerCase() : null;

        for (int offset : cellOffsets) {
            databaseFile.seek(pageStart + offset);
            readVarint();
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

            String[] selected = new String[targetIndices.length];
            for (int i = 0; i < targetIndices.length; i++) {
                if (useRowidForTarget[i]) {
                    selected[i] = String.valueOf(rowid);
                } else {
                    int colIdx = targetIndices[i];
                    selected[i] = colIdx < storedValues.size() ? valueToString(storedValues.get(colIdx)) : "";
                }
            }

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

                String cellStr = valueToString(cellValue).toLowerCase();
                if (!cellStr.equals(filterLower)) {
                    continue;
                }
            }

            resultRows.add(String.join("|", selected));
        }
    }

    private void processInteriorTablePage(int pageNumber, List<String> tableColumns,
            int[] targetIndices, int filterIndex, Object filterValue,
            List<String> resultRows) throws IOException {

        long pageStart = pageStart(pageNumber);
        long headerOffset = pageHeaderOffset(pageNumber);

        databaseFile.seek(headerOffset);
        databaseFile.readByte();
        databaseFile.skipBytes(2);
        int cellCount = readBigEndianShort();
        databaseFile.skipBytes(2);
        databaseFile.readByte();
        int rightMostPointer = readBigEndianInt();

        int[] cellOffsets = new int[cellCount];
        for (int i = 0; i < cellCount; i++) {
            cellOffsets[i] = readBigEndianShort();
        }

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
                readVarint();
            } catch (Exception ignored) {
            }

            traverseBTreeForRows(childPage, tableColumns, targetIndices,
                    filterIndex, filterValue, resultRows);
        }

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

        readVarint();
        readVarint();

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

        databaseFile.readByte();
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

        databaseFile.readByte();
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