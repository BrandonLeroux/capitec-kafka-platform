package com.capitec.kafka.inventory;

public class InventoryItem {
    public String sku;          // inventory key e.g. TYR-175-65-R14
    public String productID;    // e.g. TYRE_175_65_R14
    public String name;
    public String category;
    public int    quantity;
    public int    reorderLevel; // alert threshold
    public double unitPrice;
    public String updatedAt;

    public InventoryItem() {}

    public InventoryItem(String sku, String productID, String name, String category,
                         int quantity, int reorderLevel, double unitPrice) {
        this.sku          = sku;
        this.productID    = productID;
        this.name         = name;
        this.category     = category;
        this.quantity     = quantity;
        this.reorderLevel = reorderLevel;
        this.unitPrice    = unitPrice;
    }
}
