package com.capitec.kafka.orderservice;

/**
 * Minimal JSON field extractor — avoids pulling in Jackson/Gson.
 * Handles simple flat JSON objects only.
 */
public class JsonParser {

    public static String getString(String json, String field) {
        String search = "\"" + field + "\"";
        int idx = json.indexOf(search);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + search.length());
        if (colon < 0) return null;

        // Check for quoted string value
        int afterColon = colon + 1;
        while (afterColon < json.length() && json.charAt(afterColon) == ' ') afterColon++;

        if (afterColon >= json.length()) return null;

        if (json.charAt(afterColon) == '"') {
            int start = afterColon + 1;
            int end = start;
            while (end < json.length()) {
                if (json.charAt(end) == '"' && json.charAt(end - 1) != '\\') break;
                end++;
            }
            return json.substring(start, end).replace("\\\"", "\"");
        }

        // Unquoted value (number, boolean, null)
        int end = afterColon;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') end++;
        return json.substring(afterColon, end).trim();
    }

    public static long getLong(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return 0L;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return 0L;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (start >= json.length()) return 0L;
        // skip optional opening quote
        if (json.charAt(start) == '"') start++;
        int end = start;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}' && json.charAt(end) != '"') end++;
        String val = json.substring(start, end).trim();
        try { return Long.parseLong(val); } catch (NumberFormatException e) { return 0L; }
    }

    public static double getDouble(String json, String field) {
        String val = getString(json, field);
        if (val == null) return 0.0;
        try {
            // Handle locale-specific decimal separators (comma vs period)
            return Double.parseDouble(val.replace(',', '.'));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public static Order parseOrder(String json) {
        if (json == null || json.isBlank()) return null;

        // Handle compound messages like {"orderID":"1"},{"Amount":80}
        // Merge all JSON objects in the string before parsing
        String merged = mergeJsonObjects(json);

        String orderID    = getString(merged, "orderID");
        String customerID = getString(merged, "customerID");
        String product    = getString(merged, "product");
        double amount     = getDouble(merged, "amount");
        if (amount == 0.0) amount = getDouble(merged, "Amount");
        String status     = getString(merged, "status");

        if (orderID == null) return null;

        return new Order(
            orderID,
            customerID != null ? customerID : "UNKNOWN",
            product    != null ? product    : "UNKNOWN",
            amount,
            status     != null ? status     : "PENDING"
        );
    }

    // Merges multiple adjacent JSON objects: {"a":"1"},{"b":"2"} → {"a":"1","b":"2"}
    private static String mergeJsonObjects(String json) {
        String trimmed = json.trim();
        if (!trimmed.contains("},{")) return trimmed;
        return trimmed.replace("},{", ",");
    }
}
