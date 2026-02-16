package stepdefinitions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import utilities.JsonCompare;
import utilities.JsonCompare.ValidationReport;
import utilities.databasecolumnUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;

public class Testautomation {
    private final ObjectMapper mapper = new ObjectMapper();
    private final databasecolumnUtil dbUtil = new databasecolumnUtil();
    private final JsonCompare jsonCompare = new JsonCompare();

    private static final String SEP = "================================================================";

    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private String payloadPath;
    private String expectedPath;
    private String schemaDir;
    private JsonNode payloadArray;

    private final List<ValidationReport> reports = new ArrayList<>();
    private final Map<String, JsonCompare.ColumnRule> columnRuleCache = new HashMap<>();
    private final Map<String, LookupConfig> lookupConfigCache = new HashMap<>();
    private final Map<String, TableColumnPolicy> tablePolicyCache = new HashMap<>();

    @Given("mysql host {string} port {int} database {string} user {string} password {string} and payload file {string} and expected file {string} and schema dir {string}")
    public void setup(String host, int port, String db, String user, String password, String payload, String expected, String schemaDir) throws Exception {
        this.host = host;
        this.port = port;
        this.database = db;
        this.user = user;
        this.password = password;
        this.payloadPath = payload;
        this.expectedPath = expected;
        this.schemaDir = schemaDir;
        this.payloadArray = mapper.readTree(Path.of(payload).toFile());
    }

    @Then("database values should match expected data")
    public void validateDatabase() throws Exception {
        printRunHeader();

        if (!payloadArray.isArray()) {
            throw new IllegalArgumentException("Payload file must be a JSON array");
        }

        List<PayloadRecord> payloadRecords = extractPayloadRecords(payloadArray);
        if (payloadRecords.isEmpty()) {
            throw new IllegalArgumentException("No payload records with id/order fields found in " + payloadPath);
        }

        Set<String> payloadEventIds = new HashSet<>();
        Set<String> payloadOrderIds = new HashSet<>();
        for (PayloadRecord record : payloadRecords) {
            if (!record.eventId.isEmpty()) payloadEventIds.add(record.eventId);
            if (!record.orderId.isEmpty()) payloadOrderIds.add(record.orderId);
        }

        List<ExpectedTable> tables = loadExpectedTables(payloadEventIds, payloadOrderIds);
        if (tables.isEmpty()) {
            throw new IllegalStateException("No matching expected rows found in " + expectedPath + " for payload event/order ids.");
        }

        int matchedExpectedRows = 0;
        for (PayloadRecord payloadRecord : payloadRecords) {
            String eventId = payloadRecord.eventId;
            String orderId = payloadRecord.orderId;
            printScenarioHeader(eventId, orderId);

            for (ExpectedTable table : tables) {
                List<JsonNode> expectedRows = table.getMatchedRows(payloadRecord);
                if (expectedRows.isEmpty()) {
                    log("No expected rows for table=" + table.tableName + " (skipped)");
                    continue;
                }

                printTableHeader(table.tableName, expectedRows.size());
                applySchemaTablePolicy(table.tableName, table.schema);

                for (JsonNode baseExpectedRow : expectedRows) {
                    JsonNode expectedRow = applyTableIgnorePolicy(table.tableName, baseExpectedRow);
                    if (expectedRow == null || !expectedRow.isObject() || expectedRow.size() == 0) {
                        continue;
                    }
                    matchedExpectedRows++;

                    LinkedHashMap<String, String> criteria = buildLookupCriteria(table.lookupConfig, payloadRecord, expectedRow);
                    if (criteria.isEmpty()) {
                        log("LOOKUP: no lookup values resolved for table=" + table.tableName + " row=" + expectedRow + " (skipped)");
                        continue;
                    }

                    List<Map<String, Object>> actualRows;
                    try {
                        log("DB: fetching table=" + table.tableName + " by " + criteria);
                        actualRows = dbUtil.fetchByCriteria(
                                host,
                                port,
                                database,
                                user,
                                password,
                                table.tableName,
                                criteria
                        );
                    } catch (SQLException ex) {
                        log("DB ERROR: " + ex.getMessage());
                        throw new RuntimeException("DB fetch failed for table " + table.tableName, ex);
                    }

                    enrichSchemaWithColumnRules(table.tableName, table.schema, expectedRow);
                    ArrayNode expectedArray = mapper.createArrayNode().add(expectedRow);
                    ValidationReport report = jsonCompare.validateTable("phpmyadmin", eventId, table.tableName, actualRows, expectedArray, table.schema);
                    reports.add(report);
                    printScenarioTableSummary(report);
                }
            }
        }

        if (matchedExpectedRows == 0) {
            throw new AssertionError("No expected rows matched payload IDs from " + expectedPath + ". Check payload/expected alignment.");
        }

        for (ValidationReport r : reports) {
            if ("FAIL".equals(r.status)) {
                printRunSummary();
                throw new AssertionError("Validation failed for table " + r.tableName);
            }
        }

        printRunSummary();
    }

    private void log(String msg) {
        System.out.println("[Testautomation] " + msg);
    }

    private void enrichSchemaWithColumnRules(String tableName, JsonCompare.Schema schema, JsonNode expectedRow) throws Exception {
        Iterator<String> fields = expectedRow.fieldNames();
        while (fields.hasNext()) {
            String column = fields.next();
            JsonNode value = expectedRow.get(column);

            boolean looksJson = value.isObject() || value.isArray() ||
                    (value.isTextual() && (value.asText().trim().startsWith("{") || value.asText().trim().startsWith("[")));
            if (!looksJson) {
                continue;
            }

            String fileName = tableName + "_" + column + ".schema.json";
            Path path = Path.of(schemaDir, fileName);
            if (java.nio.file.Files.exists(path)) {
                JsonCompare.ColumnRule rule = columnRuleCache.get(path.toString());
                if (rule == null) {
                    rule = jsonCompare.loadColumnRule(path);
                    columnRuleCache.put(path.toString(), rule);
                }
                schema.rules.put(column, rule);
            }
        }
    }

    private List<ExpectedTable> loadExpectedTables(Set<String> payloadEventIds, Set<String> payloadOrderIds) throws Exception {
        List<ExpectedTable> tables = new ArrayList<>();
        Path configuredPath = Path.of(expectedPath);
        if (!java.nio.file.Files.exists(configuredPath)) {
            throw new IllegalArgumentException("Expected path not found: " + expectedPath);
        }

        Path expectedDir = java.nio.file.Files.isDirectory(configuredPath) ? configuredPath : configuredPath.getParent();
        if (expectedDir == null || !java.nio.file.Files.exists(expectedDir)) {
            throw new IllegalArgumentException("Expected directory not found for: " + expectedPath);
        }

        try (java.util.stream.Stream<Path> stream = java.nio.file.Files.list(expectedDir)) {
            List<Path> expectedFiles = stream
                    .filter(p -> p.getFileName().toString().endsWith("_expected_data.json"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();

            if (expectedFiles.isEmpty()) {
                throw new IllegalArgumentException("No *_expected_data.json files found in: " + expectedDir);
            }

            for (Path expectedFile : expectedFiles) {
                String file = expectedFile.getFileName().toString();
                String tableName = file.substring(0, file.indexOf("_expected_data.json"));

                JsonNode rows = jsonCompare.loadExpected(expectedFile);
                if (!rows.isArray()) {
                    throw new IllegalArgumentException("Expected file must contain a JSON array: " + expectedFile);
                }

                LookupConfig lookup = resolveLookup(tableName, null);
                List<JsonNode> matchedRows = new ArrayList<>();
                for (JsonNode row : rows) {
                    JsonNode normalizedRow = normalizeExpectedRow(row);
                    if (rowMatchesPayloadSets(normalizedRow, payloadEventIds, payloadOrderIds, lookup)) {
                        matchedRows.add(normalizedRow);
                    }
                }

                JsonCompare.Schema schema = new JsonCompare.Schema();
                schema.tableName = tableName;
                tables.add(new ExpectedTable(tableName, matchedRows, schema, lookup));
                log("EXPECTED: loaded " + file + " rows=" + rows.size() + " matched=" + matchedRows.size() + " lookupColumns=" + lookup.columns);
            }
        }
        return tables;
    }

    private boolean rowMatchesPayloadSets(JsonNode row, Set<String> payloadEventIds, Set<String> payloadOrderIds, LookupConfig lookup) {
        if (row == null || !row.isObject()) return false;

        boolean usedPayloadMappedLookup = false;
        boolean matchedPayloadMappedLookup = false;

        for (String column : lookup.columns) {
            String payloadMappedType = payloadMappedTypeForColumn(column);
            if (payloadMappedType.isEmpty()) {
                continue;
            }
            usedPayloadMappedLookup = true;
            String rowVal = rowValue(row, column);
            if (rowVal.isEmpty()) {
                continue;
            }
            if ("eventId".equals(payloadMappedType) && payloadEventIds.contains(rowVal)) {
                matchedPayloadMappedLookup = true;
            }
            if ("orderId".equals(payloadMappedType) && payloadOrderIds.contains(rowVal)) {
                matchedPayloadMappedLookup = true;
            }
        }

        if (usedPayloadMappedLookup) {
            return matchedPayloadMappedLookup;
        }

        String rowId = firstText(row, "id", "event_id", "event-id");
        String rowOrder = firstText(row, "orderid", "order_id", "order-id");
        return payloadEventIds.contains(rowId) || payloadOrderIds.contains(rowOrder);
    }

    private LookupConfig resolveLookup(String tableName, JsonNode expectedRow) throws Exception {
        if (lookupConfigCache.containsKey(tableName)) {
            return lookupConfigCache.get(tableName);
        }

        LookupConfig cfg = new LookupConfig();

        Path perTableLookupFile = Path.of(schemaDir, tableName + "_lookup.json");
        if (java.nio.file.Files.exists(perTableLookupFile)) {
            JsonNode node = mapper.readTree(perTableLookupFile.toFile());
            applyLookupNode(cfg, node);
        }

        Path globalLookupFile = Path.of(schemaDir, "lookup.json");
        if (java.nio.file.Files.exists(globalLookupFile)) {
            JsonNode root = mapper.readTree(globalLookupFile.toFile());
            if (root.isObject() && root.has(tableName)) {
                applyLookupNode(cfg, root.get(tableName));
            }
        }

        if (cfg.columns.isEmpty() && expectedRow != null) {
            if (expectedRow.has("id") || expectedRow.has("event_id") || expectedRow.has("event-id")) cfg.columns.add("id");
            if (expectedRow.has("orderid") || expectedRow.has("order_id") || expectedRow.has("order-id")) cfg.columns.add("orderid");
        }

        if (cfg.columns.isEmpty()) {
            cfg.columns.add("id");
            cfg.columns.add("orderid");
        }

        cfg.columns = dedup(cfg.columns);
        for (String c : cfg.columns) {
            String mapped = payloadMappedTypeForColumn(c);
            if ("eventId".equals(mapped) && cfg.idColumn == null) cfg.idColumn = c;
            if ("orderId".equals(mapped) && cfg.orderIdColumn == null) cfg.orderIdColumn = c;
        }

        lookupConfigCache.put(tableName, cfg);
        return cfg;
    }

    private void applyLookupNode(LookupConfig cfg, JsonNode node) {
        if (node == null || node.isNull()) return;

        if (node.isTextual()) {
            cfg.columns.add(node.asText().trim());
            return;
        }

        if (node.isArray()) {
            for (JsonNode n : node) {
                if (n.isTextual()) cfg.columns.add(n.asText().trim());
            }
            return;
        }

        if (!node.isObject()) {
            return;
        }

        if (node.has("idColumn")) {
            String v = node.get("idColumn").asText().trim();
            if (!v.isEmpty()) {
                cfg.idColumn = v;
                cfg.columns.add(v);
            }
        }
        if (node.has("orderIdColumn")) {
            String v = node.get("orderIdColumn").asText().trim();
            if (!v.isEmpty()) {
                cfg.orderIdColumn = v;
                cfg.columns.add(v);
            }
        }
        if (node.has("columns") && node.get("columns").isArray()) {
            for (JsonNode c : node.get("columns")) {
                if (c.isTextual()) cfg.columns.add(c.asText().trim());
            }
        }
    }

    private LinkedHashMap<String, String> buildLookupCriteria(LookupConfig lookup, PayloadRecord payload, JsonNode expectedRow) {
        LinkedHashMap<String, String> criteria = new LinkedHashMap<>();
        for (String column : lookup.columns) {
            String value = payloadValueForColumn(column, payload);
            if (value.isEmpty()) {
                value = rowValue(expectedRow, column);
            }
            if (!value.isEmpty()) {
                criteria.put(column, value);
            }
        }

        if (criteria.isEmpty()) {
            if (lookup.idColumn != null) {
                String v = payloadValueForColumn(lookup.idColumn, payload);
                if (v.isEmpty()) v = rowValue(expectedRow, lookup.idColumn);
                if (!v.isEmpty()) criteria.put(lookup.idColumn, v);
            }
            if (lookup.orderIdColumn != null) {
                String v = payloadValueForColumn(lookup.orderIdColumn, payload);
                if (v.isEmpty()) v = rowValue(expectedRow, lookup.orderIdColumn);
                if (!v.isEmpty()) criteria.put(lookup.orderIdColumn, v);
            }
        }

        return criteria;
    }

    private String payloadValueForColumn(String column, PayloadRecord payload) {
        String type = payloadMappedTypeForColumn(column);
        if ("eventId".equals(type)) return payload.eventId;
        if ("orderId".equals(type)) return payload.orderId;
        return "";
    }

    private String payloadMappedTypeForColumn(String column) {
        if (column == null) return "";
        String c = column.trim().toLowerCase(Locale.ROOT).replace("-", "_");
        if (c.equals("id") || c.equals("event_id") || c.equals("eventid")) return "eventId";
        if (c.equals("orderid") || c.equals("order_id")) return "orderId";
        return "";
    }

    private String rowValue(JsonNode row, String requestedColumn) {
        if (row == null || !row.isObject() || requestedColumn == null) return "";
        String c = requestedColumn.trim();
        if (c.isEmpty()) return "";

        String cUnderscore = c.replace('-', '_');
        String cHyphen = c.replace('_', '-');

        return firstText(row, c, cUnderscore, cHyphen);
    }

    private void applySchemaTablePolicy(String tableName, JsonCompare.Schema schema) {
        TableColumnPolicy policy = getTablePolicy(tableName);
        for (String f : policy.required) {
            if (!schema.requiredFields.contains(f)) schema.requiredFields.add(f);
        }
        for (String f : policy.optional) {
            if (!schema.optionalFields.contains(f)) schema.optionalFields.add(f);
        }
    }

    private JsonNode applyTableIgnorePolicy(String tableName, JsonNode expectedRow) {
        if (expectedRow == null || !expectedRow.isObject()) return expectedRow;

        TableColumnPolicy policy = getTablePolicy(tableName);
        if (policy.ignore.isEmpty()) {
            return expectedRow;
        }

        ObjectNode out = ((ObjectNode) expectedRow).deepCopy();
        for (String c : policy.ignore) {
            out.remove(c);
            out.remove(c.replace('-', '_'));
            out.remove(c.replace('_', '-'));
        }
        return out;
    }

    private TableColumnPolicy getTablePolicy(String tableName) {
        if (tablePolicyCache.containsKey(tableName)) {
            return tablePolicyCache.get(tableName);
        }

        TableColumnPolicy policy = new TableColumnPolicy();
        Path rulesPath = Path.of(schemaDir, "table_columns.json");
        if (java.nio.file.Files.exists(rulesPath)) {
            try {
                JsonNode root = mapper.readTree(rulesPath.toFile());
                if (root.isObject() && root.has(tableName)) {
                    JsonNode node = root.get(tableName);
                    if (node.isArray() && node.size() > 0) node = node.get(0);
                    if (node.isObject()) {
                        policy.required.addAll(readStringArray(node, "required"));
                        policy.optional.addAll(readStringArray(node, "optional"));
                        policy.ignore.addAll(readStringArray(node, "ignore"));
                        policy.ignore.addAll(readStringArray(node, "ignored"));
                    }
                }
            } catch (Exception ex) {
                log("TABLE POLICY: failed to load " + rulesPath + " -> " + ex.getMessage());
            }
        }

        policy.required = dedup(policy.required);
        policy.optional = dedup(policy.optional);
        policy.ignore = dedup(policy.ignore);

        tablePolicyCache.put(tableName, policy);
        return policy;
    }

    private List<String> readStringArray(JsonNode node, String field) {
        List<String> out = new ArrayList<>();
        if (node == null || !node.has(field) || !node.get(field).isArray()) return out;
        for (JsonNode v : node.get(field)) {
            if (v.isTextual()) {
                String s = v.asText().trim();
                if (!s.isEmpty()) out.add(s.replace('-', '_'));
            }
        }
        return out;
    }

    private <T> List<T> dedup(List<T> values) {
        LinkedHashSet<T> set = new LinkedHashSet<>(values);
        return new ArrayList<>(set);
    }

    private void printScenarioTableSummary(ValidationReport report) {
        int pass = 0;
        int fail = 0;
        int skipped = 0;
        for (utilities.JsonCompare.ColumnResult r : report.results) {
            if ("PASS".equals(r.status)) pass++;
            if ("FAIL".equals(r.status)) fail++;
            if ("SKIPPED".equals(r.status)) skipped++;
        }

        System.out.println("Status      : " + report.status);
        System.out.println("Columns     : total=" + report.results.size() + " pass=" + pass + " fail=" + fail + " skipped=" + skipped);
        System.out.println(SEP);
    }

    private void printRunHeader() {
        System.out.println(SEP);
        System.out.println("DB EVENT VALIDATION");
        System.out.println("Payload     : " + payloadPath);
        System.out.println("Expected    : " + expectedPath);
        System.out.println("Schema dir  : " + schemaDir);
        System.out.println("DB          : " + host + ":" + port + "/" + database);
        System.out.println(SEP);
    }

    private void printScenarioHeader(String eventId, String orderId) {
        System.out.println("Scenario");
        System.out.println("  eventId   : " + eventId);
        System.out.println("  orderId   : " + orderId);
    }

    private void printTableHeader(String tableName, int expectedRows) {
        System.out.println("Table       : " + tableName);
        System.out.println("ExpectedRows: " + expectedRows);
    }

    private void printRunSummary() {
        int reportPass = 0;
        int reportFail = 0;
        int colPass = 0;
        int colFail = 0;
        int colSkipped = 0;

        for (ValidationReport r : reports) {
            if ("FAIL".equals(r.status)) reportFail++; else reportPass++;
            for (utilities.JsonCompare.ColumnResult c : r.results) {
                if ("PASS".equals(c.status)) colPass++;
                if ("FAIL".equals(c.status)) colFail++;
                if ("SKIPPED".equals(c.status)) colSkipped++;
            }
        }

        System.out.println("RUN SUMMARY");
        System.out.println("  tableReports : " + reports.size() + " (pass=" + reportPass + ", fail=" + reportFail + ")");
        System.out.println("  columns      : pass=" + colPass + ", fail=" + colFail + ", skipped=" + colSkipped);
        Path passFile = writePassReportFile();
        System.out.println("  passReport   : " + passFile);
        printValidationCasesTable(false);
        System.out.println(SEP);
    }

    private void printValidationCasesTable(boolean includePass) {
        List<String[]> rows = new ArrayList<>();
        for (ValidationReport report : reports) {
            if (report.globalErrors != null && !report.globalErrors.isEmpty()) {
                for (String err : report.globalErrors) {
                    rows.add(new String[]{
                            report.tableName,
                            "<global>",
                            report.eventId,
                            "<n/a>",
                            "<n/a>",
                            "FAIL",
                            normalizeCell(err)
                    });
                }
            }
            for (utilities.JsonCompare.ColumnResult r : report.results) {
                if (!includePass && !"FAIL".equals(r.status) && !"SKIPPED".equals(r.status)) {
                    continue;
                }
                rows.add(new String[]{
                        report.tableName,
                        r.column,
                        report.eventId,
                        normalizeCell(r.expected),
                        normalizeCell(r.actual),
                        r.status,
                        normalizeCell(r.reason)
                });
            }
        }

        if (rows.isEmpty()) {
            System.out.println(includePass ? "ALL VALIDATION CASES" : "FAILED/SKIPPED CASES");
            System.out.println("  none");
            return;
        }

        String[] headers = {"table", "columnname", "order/id", "expected", "actual", "status", "error message"};
        int[] widths = new int[headers.length];
        for (int i = 0; i < headers.length; i++) widths[i] = headers[i].length();
        for (String[] row : rows) {
            for (int i = 0; i < row.length; i++) {
                widths[i] = Math.max(widths[i], row[i].length());
            }
        }

        for (int i = 0; i < widths.length; i++) widths[i] = Math.min(widths[i], 60);

        System.out.println(includePass ? "ALL VALIDATION CASES" : "FAILED/SKIPPED CASES");
        System.out.println(formatTableRow(headers, widths));
        System.out.println(formatTableSeparator(widths));
        for (String[] row : rows) {
            String[] clipped = new String[row.length];
            for (int i = 0; i < row.length; i++) clipped[i] = clip(row[i], widths[i]);
            System.out.println(formatTableRow(clipped, widths));
        }
    }

    private Path writePassReportFile() {
        List<String[]> rows = new ArrayList<>();
        for (ValidationReport report : reports) {
            for (utilities.JsonCompare.ColumnResult r : report.results) {
                if (!"PASS".equals(r.status)) {
                    continue;
                }
                rows.add(new String[]{
                        report.tableName,
                        r.column,
                        report.eventId,
                        normalizeCell(r.expected),
                        normalizeCell(r.actual),
                        r.status
                });
            }
        }

        try {
            Path outDir = Path.of("target", "validation-reports");
            Files.createDirectories(outDir);
            Path outFile = outDir.resolve("pass-report-" + System.currentTimeMillis() + ".csv");
            List<String> lines = new ArrayList<>();
            lines.add("table,columnname,order/id,expected,actual,status");
            for (String[] row : rows) {
                lines.add(csv(row));
            }
            Files.write(outFile, lines, StandardCharsets.UTF_8);
            return outFile;
        } catch (Exception ex) {
            log("PASS REPORT: failed to write file -> " + ex.getMessage());
            return Path.of("target", "validation-reports", "pass-report-write-failed.csv");
        }
    }

    private String csv(String[] row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            if (i > 0) sb.append(",");
            String val = row[i] == null ? "" : row[i];
            String escaped = val.replace("\"", "\"\"");
            sb.append("\"").append(escaped).append("\"");
        }
        return sb.toString();
    }

    private String formatTableRow(String[] values, int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(" | ");
            sb.append(padRight(values[i], widths[i]));
        }
        return sb.toString();
    }

    private String formatTableSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            if (i > 0) sb.append("-+-");
            sb.append("-".repeat(widths[i]));
        }
        return sb.toString();
    }

    private String padRight(String value, int width) {
        String v = value == null ? "" : value;
        if (v.length() >= width) return v;
        return v + " ".repeat(width - v.length());
    }

    private String clip(String value, int max) {
        String v = value == null ? "" : value;
        if (v.length() <= max) return v;
        if (max <= 3) return v.substring(0, max);
        return v.substring(0, max - 3) + "...";
    }

    private String normalizeCell(String value) {
        if (value == null) return "<null>";
        String v = value.replace("\r", " ").replace("\n", " ").trim();
        if (v.isEmpty()) return "<empty>";
        return v;
    }

    private List<PayloadRecord> extractPayloadRecords(JsonNode node) {
        List<PayloadRecord> records = new ArrayList<>();
        collectPayloadRecords(node, records);
        return records;
    }

    private void collectPayloadRecords(JsonNode node, List<PayloadRecord> records) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                collectPayloadRecords(child, records);
            }
            return;
        }
        if (!node.isObject()) {
            return;
        }

        String eventId = firstText(node, "event-id", "id", "event_id");
        String orderId = firstText(node, "order-id", "orderid", "order_id");
        if (orderId.isEmpty()) {
            orderId = nestedText(node, "data", "orderId");
        }

        if (!eventId.isEmpty() || !orderId.isEmpty()) {
            records.add(new PayloadRecord(eventId, orderId));
        }
    }

    private String firstText(JsonNode node, String... names) {
        for (String name : names) {
            if (node.has(name) && !node.get(name).isNull()) {
                String v = node.get(name).asText().trim();
                if (!v.isEmpty()) return v;
            }
        }
        return "";
    }

    private String nestedText(JsonNode node, String parent, String child) {
        if (node.has(parent) && node.get(parent).isObject()) {
            JsonNode p = node.get(parent);
            if (p.has(child) && !p.get(child).isNull()) {
                return p.get(child).asText().trim();
            }
        }
        return "";
    }

    private JsonNode normalizeExpectedRow(JsonNode row) {
        if (!row.isObject()) {
            return row;
        }
        ObjectNode normalized = mapper.createObjectNode();
        Iterator<String> fields = row.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            String normalizedField = field.replace('-', '_');
            normalized.set(normalizedField, row.get(field));
        }
        return normalized;
    }

    private static class PayloadRecord {
        final String eventId;
        final String orderId;

        PayloadRecord(String eventId, String orderId) {
            this.eventId = eventId == null ? "" : eventId;
            this.orderId = orderId == null ? "" : orderId;
        }
    }

    private static class LookupConfig {
        List<String> columns = new ArrayList<>();
        String idColumn;
        String orderIdColumn;
    }

    private static class ExpectedTable {
        final String tableName;
        final List<JsonNode> matchedRows;
        final JsonCompare.Schema schema;
        final LookupConfig lookupConfig;

        ExpectedTable(String tableName, List<JsonNode> matchedRows, JsonCompare.Schema schema, LookupConfig lookupConfig) {
            this.tableName = tableName;
            this.matchedRows = matchedRows;
            this.schema = schema;
            this.lookupConfig = lookupConfig;
        }

        List<JsonNode> getMatchedRows(PayloadRecord payloadRecord) {
            LinkedHashMap<String, JsonNode> dedup = new LinkedHashMap<>();
            for (JsonNode row : matchedRows) {
                String rowEventId = row.has("id") ? row.get("id").asText() : row.path("event_id").asText("");
                String rowOrderId = row.has("orderid") ? row.get("orderid").asText() : row.path("order_id").asText("");

                boolean matchEvent = payloadRecord.eventId.isEmpty() || payloadRecord.eventId.equals(rowEventId);
                boolean matchOrder = payloadRecord.orderId.isEmpty() || payloadRecord.orderId.equals(rowOrderId);
                if (matchEvent || matchOrder) {
                    dedup.put(row.toString(), row);
                }
            }
            return new ArrayList<>(dedup.values());
        }
    }

    private static class TableColumnPolicy {
        List<String> required = new ArrayList<>();
        List<String> optional = new ArrayList<>();
        List<String> ignore = new ArrayList<>();
    }
}
