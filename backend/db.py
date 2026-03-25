"""
SAP O2C Database — built on actual dataset files.

ACTUAL tables (confirmed from disk):
  billing_document_cancellations   → the only billing table (all billing docs, some cancelled)
  business_partners
  customer_company_assignments
  customer_sales_area_assignments
  journal_entry_items_accounts_receivable
  outbound_delivery_headers        → NO salesOrder column, NO delivery items table
  payments_accounts_receivable
  plants
  product_descriptions
  sales_order_headers
  sales_order_items                → has 'material' column linking to product_descriptions.product

CONFIRMED JOIN CHAIN:
  Customer ──(soldToParty)──────────► SalesOrder
  Customer ──(soldToParty)──────────► BillingDocument
  BillingDocument ──(accountingDocument)────► JournalEntry
  JournalEntry ──(clearingAccountingDocument)──► Payment
  SalesOrder ──(salesOrder = salesOrderItem)──► SalesOrderItem ──(material)──► ProductDescription

NOTE: There is NO outbound_delivery_items table, so there is NO direct join
      between SalesOrder and Delivery in this dataset.
      Deliveries are standalone nodes linked to the graph conceptually only.
"""

import os
import glob
import json
import duckdb
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "sap-o2c-data")

_con: Optional[duckdb.DuckDBPyConnection] = None


def _get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is not None:
        return _con

    _con = duckdb.connect(":memory:")

    def register(view_name: str, folder: str):
        pattern = os.path.join(DATA_DIR, folder, "*.jsonl")
        files = glob.glob(pattern)
        if not files:
            print(f"[WARN] No files for {folder}")
            return
        quoted = ", ".join(f"'{f}'" for f in files)
        _con.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM read_json_auto([{quoted}], ignore_errors=true)"
        )
        count = _con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        print(f"[DB] {view_name}: {count} rows")

    # The ACTUAL table names in the dataset (align view names with the schema used by LLM)
    register("billing_document_cancellations", "billing_document_cancellations")
    register("billing_document_headers",       "billing_document_headers")
    register("billing_document_items",         "billing_document_items")
    register("business_partners",              "business_partners")
    register("customer_company_assignments",   "customer_company_assignments")
    register("customer_sales_area_assignments", "customer_sales_area_assignments")
    register("journal_entry_items_accounts_receivable", "journal_entry_items_accounts_receivable")
    register("outbound_delivery_headers",      "outbound_delivery_headers")
    register("outbound_delivery_items",        "outbound_delivery_items")
    register("payments_accounts_receivable",   "payments_accounts_receivable")
    register("plants",                         "plants")
    register("product_descriptions",           "product_descriptions")
    register("sales_order_headers",            "sales_order_headers")
    register("sales_order_items",              "sales_order_items")

    # Fail fast if dataset files were not found and views were not created (common on Render if data folder is missing)
    required_views = [
        "business_partners",
        "billing_document_headers",
        "billing_document_items",
        "journal_entry_items_accounts_receivable",
        "payments_accounts_receivable",
        "sales_order_headers",
        "sales_order_items",
    ]
    missing = [v for v in required_views if not _con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{v}'").fetchone()[0]]
    if missing:
        raise RuntimeError(
            "Missing required dataset tables: " + ", ".join(missing) +
            " — ensure sap-o2c-data is present on the server or set DATA_DIR to the dataset path."
        )

    return _con


# ─── Schema for LLM ────────────────────────────────────────────────────────────
SCHEMA_DESCRIPTION = """
=== SAP ORDER-TO-CASH DATABASE SCHEMA ===
Database: DuckDB. All monetary amounts are strings — use TRY_CAST(x AS DOUBLE) for math.
All dates are ISO strings — use CAST(x AS DATE) for comparisons.

━━━ TABLE REFERENCE (exact names — never alias or rename these) ━━━

1. sales_order_headers [100 rows] — One row per sales order
   PK: salesOrder
   Columns: salesOrder, salesOrderType, salesOrganization, distributionChannel,
   organizationDivision, salesGroup, salesOffice, soldToParty, creationDate,
   createdByUser, lastChangeDateTime, totalNetAmount, overallDeliveryStatus,
   overallOrdReltdBillgStatus, overallSdDocReferenceStatus, transactionCurrency,
   pricingDate, requestedDeliveryDate, headerBillingBlockReason, deliveryBlockReason,
   incotermsClassification, incotermsLocation1, customerPaymentTerms, totalCreditCheckStatus

2. sales_order_items [167 rows] — Line items within a sales order
   PK: salesOrder + salesOrderItem
   FK: salesOrder → sales_order_headers.salesOrder
   FK: material → products.product (note: column is named "material" but references products.product)
   Columns: salesOrder, salesOrderItem, salesOrderItemCategory, material,
   requestedQuantity, requestedQuantityUnit, transactionCurrency, netAmount,
   materialGroup, productionPlant, storageLocation, salesDocumentRjcnReason, itemBillingBlockReason

3. sales_order_schedule_lines [179 rows] — Delivery schedule per order item
   PK: salesOrder + salesOrderItem + scheduleLine
   FK: salesOrder → sales_order_headers.salesOrder
   Columns: salesOrder, salesOrderItem, scheduleLine, confirmedDeliveryDate,
   orderQuantityUnit, confdOrderQtyByMatlAvailCheck

4. outbound_delivery_headers [86 rows] — One row per delivery document
   PK: deliveryDocument
   ⚠️ NO salesOrder column — link to sales orders via outbound_delivery_items
   Columns: deliveryDocument, shippingPoint, hdrGeneralIncompletionStatus,
   headerBillingBlockReason, deliveryBlockReason, creationDate, creationTime,
   lastChangeDate, actualGoodsMovementDate, actualGoodsMovementTime,
   overallGoodsMovementStatus, overallPickingStatus, overallProofOfDeliveryStatus

5. outbound_delivery_items [137 rows] — Line items within a delivery
   PK: deliveryDocument + deliveryDocumentItem
   FK: deliveryDocument → outbound_delivery_headers.deliveryDocument
   FK: referenceSdDocument → sales_order_headers.salesOrder  ← link delivery to SO
   Columns: deliveryDocument, deliveryDocumentItem, referenceSdDocument,
   referenceSdDocumentItem, plant, storageLocation, actualDeliveryQuantity,
   deliveryQuantityUnit, batch, itemBillingBlockReason, lastChangeDate

6. billing_document_headers [163 rows] — One row per billing document (invoice)
   PK: billingDocument
   FK: soldToParty → business_partners.businessPartner
   FK: accountingDocument → journal_entry_items_accounts_receivable.accountingDocument
   Columns: billingDocument, billingDocumentType, creationDate, creationTime,
   lastChangeDateTime, billingDocumentDate, billingDocumentIsCancelled,
   cancelledBillingDocument, totalNetAmount, transactionCurrency,
   companyCode, fiscalYear, accountingDocument, soldToParty
   NOTE: billingDocumentIsCancelled=true means cancelled (legitimately has no journal entry)

7. billing_document_items [245 rows] — Line items within a billing document
   PK: billingDocument + billingDocumentItem
   FK: billingDocument → billing_document_headers.billingDocument
   FK: referenceSdDocument → outbound_delivery_headers.deliveryDocument  ← link billing to delivery
   FK: material → products.product (column named "material" references products.product)
   Columns: billingDocument, billingDocumentItem, material, billingQuantity,
   billingQuantityUnit, netAmount, transactionCurrency,
   referenceSdDocument, referenceSdDocumentItem

8. billing_document_cancellations [80 rows] — Cancelled billing documents
   Same columns as billing_document_headers. These are the 80 cancelled invoices.
   PK: billingDocument

9. journal_entry_items_accounts_receivable [123 rows] — Accounting entries from billing
   PK: accountingDocument + accountingDocumentItem
   FK: accountingDocument → billing_document_headers.accountingDocument
   FK: customer → business_partners.businessPartner
   Columns: companyCode, fiscalYear, accountingDocument, glAccount, referenceDocument,
   costCenter, profitCenter, transactionCurrency, amountInTransactionCurrency,
   companyCodeCurrency, amountInCompanyCodeCurrency, postingDate, documentDate,
   accountingDocumentType, accountingDocumentItem, assignmentReference,
   lastChangeDateTime, customer, financialAccountType, clearingDate,
   clearingAccountingDocument, clearingDocFiscalYear

10. payments_accounts_receivable [120 rows] — Payments received against journal entries
    PK: accountingDocument + accountingDocumentItem
    FK: clearingAccountingDocument → journal_entry_items_accounts_receivable.clearingAccountingDocument
    FK: customer → business_partners.businessPartner
    Columns: companyCode, fiscalYear, accountingDocument, accountingDocumentItem,
    clearingDate, clearingAccountingDocument, clearingDocFiscalYear,
    amountInTransactionCurrency, transactionCurrency, amountInCompanyCodeCurrency,
    companyCodeCurrency, customer, invoiceReference, invoiceReferenceFiscalYear,
    salesDocument, salesDocumentItem, postingDate, documentDate,
    assignmentReference, glAccount, financialAccountType, profitCenter, costCenter

11. business_partners [8 rows] — Customers/partners master data
    PK: businessPartner
    Columns: businessPartner, customer, businessPartnerCategory, businessPartnerFullName,
    businessPartnerGrouping, businessPartnerName, correspondenceLanguage, createdByUser,
    creationDate, creationTime, firstName, formOfAddress, industry, lastChangeDate,
    lastName, organizationBpName1, organizationBpName2, businessPartnerIsBlocked,
    isMarkedForArchiving

12. business_partner_addresses [8 rows] — Addresses for business partners
    FK: businessPartner → business_partners.businessPartner
    Columns: businessPartner, addressId, validityStartDate, validityEndDate,
    addressUuid, addressTimeZone, cityName, country, poBox, poBoxPostalCode,
    postalCode, region, streetName, taxJurisdiction, transportZone

13. customer_company_assignments [8 rows] — Customer to company code mapping
    FK: customer → business_partners.businessPartner
    Columns: customer, companyCode, accountingClerk, paymentTerms,
    reconciliationAccount, deletionIndicator, customerAccountGroup

14. customer_sales_area_assignments [28 rows] — Customer sales area config
    FK: customer → business_partners.businessPartner
    Columns: customer, salesOrganization, distributionChannel, division,
    billingIsBlockedForCustomer, creditControlArea, currency,
    customerPaymentTerms, deliveryPriority, shippingCondition, salesDistrict

15. products [69 rows] — Product master data
    PK: product  ⚠️ PRIMARY KEY IS "product" — NOT "material"
    Columns: product, productType, crossPlantStatus, creationDate, createdByUser,
    lastChangeDate, lastChangeDateTime, isMarkedForDeletion, productOldId,
    grossWeight, weightUnit, netWeight, productGroup, baseUnit, division, industrySector

16. product_descriptions [69 rows] — Product names in different languages
    FK: product → products.product
    ⚠️ language = 'EN' for English (NOT 'E')
    Columns: product, language, productDescription
    JOIN pattern: billing_document_items.material = product_descriptions.product

17. product_plants [3036 rows] — Product availability per plant
    FK: product → products.product
    FK: plant → plants.plant
    Columns: product, plant, countryOfOrigin, regionOfOrigin,
    availabilityCheckType, fiscalYearVariant, profitCenter, mrpType

18. product_storage_locations [16723 rows] — Stock per product/plant/location
    FK: product → products.product
    FK: plant → plants.plant
    Columns: product, plant, storageLocation, physicalInventoryBlockInd,
    dateOfLastPostedCntUnRstrcdStk

19. plants [44 rows] — Plant master data
    PK: plant
    Columns: plant, plantName, valuationArea, plantCustomer, plantSupplier,
    salesOrganization, addressId, plantCategory, distributionChannel, division,
    language, isMarkedForArchiving

━━━ JOIN CHAIN (the O2C flow — memorize this) ━━━

Customer → Sales Order:
  business_partners.businessPartner = sales_order_headers.soldToParty

Sales Order → Delivery:
  sales_order_headers.salesOrder = outbound_delivery_items.referenceSdDocument
  outbound_delivery_items.deliveryDocument = outbound_delivery_headers.deliveryDocument

Delivery → Billing:
  outbound_delivery_headers.deliveryDocument = billing_document_items.referenceSdDocument
  billing_document_items.billingDocument = billing_document_headers.billingDocument

Billing → Journal Entry:
  billing_document_headers.accountingDocument = journal_entry_items_accounts_receivable.accountingDocument

Journal Entry → Payment:
  journal_entry_items_accounts_receivable.clearingAccountingDocument = payments_accounts_receivable.clearingAccountingDocument

Product lookup from billing:
  billing_document_items.material = products.product = product_descriptions.product
  (filter: product_descriptions.language = 'EN')

━━━ FORBIDDEN TABLE ALIASES (these tables DO NOT EXIST) ━━━
❌ deliveries          → ✅ outbound_delivery_headers
❌ delivery_items      → ✅ outbound_delivery_items
❌ delivery_headers    → ✅ outbound_delivery_headers
❌ billing_documents   → ✅ billing_document_headers
❌ journal_entries     → ✅ journal_entry_items_accounts_receivable
❌ payments            → ✅ payments_accounts_receivable
❌ customers           → ✅ business_partners

━━━ COMMON QUERY PATTERNS ━━━

Trace a billing document (SO → Delivery → Billing → Journal):
  FROM billing_document_headers bdh
  JOIN billing_document_items bdi ON bdi.billingDocument = bdh.billingDocument
  JOIN outbound_delivery_items odi ON odi.deliveryDocument = bdi.referenceSdDocument
  JOIN sales_order_headers soh ON soh.salesOrder = odi.referenceSdDocument
  LEFT JOIN journal_entry_items_accounts_receivable je ON je.accountingDocument = bdh.accountingDocument

Products with most billing documents:
  FROM product_descriptions pd
  JOIN billing_document_items bdi ON bdi.material = pd.product
  WHERE pd.language = 'EN'
  GROUP BY pd.product, pd.productDescription

Broken flows — delivered not billed:
  FROM outbound_delivery_headers odh
  LEFT JOIN billing_document_items bdi ON bdi.referenceSdDocument = odh.deliveryDocument
  WHERE bdi.billingDocument IS NULL

Broken flows — billed not posted:
  FROM billing_document_headers bdh
  LEFT JOIN journal_entry_items_accounts_receivable je ON je.accountingDocument = bdh.accountingDocument
  WHERE bdh.billingDocumentIsCancelled = false AND je.accountingDocument IS NULL
"""


def run_sql(sql: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Execute a SELECT query. Returns (rows, error_message)."""
    try:
        con = _get_con()
        result = con.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        # Clean null values if needed
        clean = []
        for row in rows:
            clean.append({k: (None if str(v) == "nan" else v) for k, v in row.items()})
        return clean, None
    except Exception as e:
        return [], str(e)


def build_graph() -> Dict[str, Any]:
    """
    Build graph nodes + edges from the actual dataset.

    Node types : Customer, SalesOrder, BillingDocument, JournalEntry, Payment, Delivery
    Edge types : PLACED_ORDER, RECEIVED_BILL, POSTED_TO, CLEARED_BY
    """
    con = _get_con()
    nodes: List[Dict] = []
    edges: List[Dict] = []
    seen: set = set()

    def add_node(nid: str, ntype: str, label: str, data: Dict):
        if nid not in seen:
            seen.add(nid)
            nodes.append({"id": nid, "type": ntype, "label": label, "data": data})

    def add_edge(src: str, tgt: str, rel: str):
        if src in seen and tgt in seen:
            edges.append({"source": src, "target": tgt, "relation": rel})

    # ── Customers ──────────────────────────────────────────────────────────
    for bp, name, full_name, blocked, created in con.execute("""
        SELECT businessPartner, businessPartnerName, businessPartnerFullName,
               businessPartnerIsBlocked, creationDate
        FROM business_partners
    """).fetchall():
        add_node(f"CU-{bp}", "Customer", full_name or name or bp, {
            "BusinessPartner": bp,
            "Name": full_name or name,
            "Blocked": str(blocked),
            "CreatedOn": str(created)[:10] if created else "",
        })

    # ── Sales Orders ───────────────────────────────────────────────────────
    for so, party, amt, curr, created, del_st, bill_st in con.execute("""
        SELECT salesOrder, soldToParty, totalNetAmount, transactionCurrency,
               creationDate, overallDeliveryStatus, overallOrdReltdBillgStatus
        FROM sales_order_headers
    """).fetchall():
        nid = f"SO-{so}"
        add_node(nid, "SalesOrder", f"SO {so}", {
            "SalesOrder": so,
            "Customer": party,
            "NetAmount": f"{amt} {curr}" if amt else "",
            "CreatedOn": str(created)[:10] if created else "",
            "DeliveryStatus": del_st or "",
            "BillingStatus": bill_st or "",
        })
        add_edge(f"CU-{party}", nid, "PLACED_ORDER")

    # ── Billing Documents ──────────────────────────────────────────────────
    billing_acct_to_nid: Dict[str, str] = {}
    for bd, party, acct, amt, curr, cancelled, created in con.execute("""
        SELECT billingDocument, soldToParty, accountingDocument,
               totalNetAmount, transactionCurrency,
               billingDocumentIsCancelled, creationDate
        FROM billing_document_headers
    """).fetchall():
        nid = f"BD-{bd}"
        add_node(nid, "BillingDocument", f"BD {bd}", {
            "BillingDocument": bd,
            "Customer": party,
            "AccountingDoc": acct,
            "NetAmount": f"{amt} {curr}" if amt else "",
            "Cancelled": str(cancelled),
            "CreatedOn": str(created)[:10] if created else "",
        })
        add_edge(f"CU-{party}", nid, "RECEIVED_BILL")
        if acct:
            billing_acct_to_nid[str(acct)] = nid

    # ── Journal Entries ────────────────────────────────────────────────────
    je_clearing_to_nid: Dict[str, str] = {}
    for acct, item, ref, cust, amt, curr, post_dt, clearing, gl, dtype, fy in con.execute("""
        SELECT accountingDocument, accountingDocumentItem, referenceDocument,
               customer, amountInTransactionCurrency, transactionCurrency,
               postingDate, clearingAccountingDocument, glAccount,
               accountingDocumentType, fiscalYear
        FROM journal_entry_items_accounts_receivable
    """).fetchall():
        nid = f"JE-{acct}"
        add_node(nid, "JournalEntry", f"JE {acct}", {
            "AccountingDocument": acct,
            "Item": item,
            "ReferenceDoc": ref,
            "Customer": cust,
            "Amount": f"{amt} {curr}" if amt else "",
            "PostingDate": str(post_dt)[:10] if post_dt else "",
            "GLAccount": gl,
            "DocType": dtype,
            "FiscalYear": str(fy) if fy else "",
        })
        # BillingDocument → JournalEntry
        bd_nid = billing_acct_to_nid.get(str(acct))
        if bd_nid:
            add_edge(bd_nid, nid, "POSTED_TO")
        if clearing:
            je_clearing_to_nid[str(clearing)] = nid

    # ── Payments ───────────────────────────────────────────────────────────
    for acct, item, cust, clearing, amt, curr, post_dt, clear_dt, gl in con.execute("""
        SELECT accountingDocument, accountingDocumentItem, customer,
               clearingAccountingDocument, amountInTransactionCurrency,
               transactionCurrency, postingDate, clearingDate, glAccount
        FROM payments_accounts_receivable
    """).fetchall():
        nid = f"PY-{acct}"
        add_node(nid, "Payment", f"PY {acct}", {
            "AccountingDocument": acct,
            "Customer": cust,
            "Amount": f"{amt} {curr}" if amt else "",
            "PostingDate": str(post_dt)[:10] if post_dt else "",
            "ClearingDate": str(clear_dt)[:10] if clear_dt else "",
            "GLAccount": gl,
        })
        # JournalEntry → Payment
        je_nid = je_clearing_to_nid.get(str(acct))
        if je_nid:
            add_edge(je_nid, nid, "CLEARED_BY")

    # ── Deliveries ───────────────
    for del_doc, ship_pt, created, gm_st, pick_st, incompl in con.execute("""
        SELECT deliveryDocument, shippingPoint, creationDate,
               overallGoodsMovementStatus, overallPickingStatus,
               hdrGeneralIncompletionStatus
        FROM outbound_delivery_headers
    """).fetchall():
        add_node(f"DE-{del_doc}", "Delivery", f"DE {del_doc}", {
            "DeliveryDocument": del_doc,
            "ShippingPoint": ship_pt or "",
            "CreatedOn": str(created)[:10] if created else "",
            "GoodsMovementStatus": gm_st or "",
            "PickingStatus": pick_st or "",
            "IncompletionStatus": incompl or "",
        })

    # ── Graph Edges for Deliveries and Billing ───────────────
    # Sales Order -> Delivery
    for so, de in con.execute("""
        SELECT DISTINCT referenceSdDocument, deliveryDocument
        FROM outbound_delivery_items
        WHERE referenceSdDocument IS NOT NULL
    """).fetchall():
        add_edge(f"SO-{so}", f"DE-{de}", "DELIVERED_IN")

    # Delivery -> Billing Document  (and maybe Sales Order -> Billing Document)
    for ref, bd in con.execute("""
        SELECT DISTINCT referenceSdDocument, billingDocument
        FROM billing_document_items
        WHERE referenceSdDocument IS NOT NULL
    """).fetchall():
        # ref could be a Sales Order or a Delivery
        if str(ref).startswith("8"):  # typical delivery doc prefix
            add_edge(f"DE-{ref}", f"BD-{bd}", "BILLED_IN")
        elif str(ref).startswith("7"): # typical sales order doc prefix
            add_edge(f"SO-{ref}", f"BD-{bd}", "BILLED_IN")

    print(f"[Graph] {len(nodes)} nodes, {len(edges)} edges")
    return {"nodes": nodes, "edges": edges}


def get_broken_flows() -> Dict[str, Any]:
    """Identify incomplete O2C flows."""
    con = _get_con()

    # Billing docs with no journal entry (not cancelled)
    unposted, _ = run_sql("""
        SELECT bd.billingDocument, bd.soldToParty, bd.totalNetAmount
        FROM billing_document_headers bd
        LEFT JOIN journal_entry_items_accounts_receivable je ON bd.accountingDocument = je.accountingDocument
        WHERE bd.billingDocumentIsCancelled = false
          AND je.accountingDocument IS NULL
        LIMIT 20
    """)

    # Journal entries with no payment
    unpaid, _ = run_sql("""
        SELECT je.accountingDocument, je.customer, je.amountInTransactionCurrency
        FROM journal_entry_items_accounts_receivable je
        LEFT JOIN payments_accounts_receivable p ON je.clearingAccountingDocument = p.accountingDocument
        WHERE je.clearingAccountingDocument IS NULL
           OR je.clearingAccountingDocument = ''
           OR p.accountingDocument IS NULL
        LIMIT 20
    """)

    return {"unposted_billings": unposted, "unpaid_journal_entries": unpaid}