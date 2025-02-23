Subject: Update on Retail Receipt Data Pipeline & Preliminary Insights

Hi Team,

I wanted to share a brief update on our retail receipt data pipeline project and highlight some early insights and questions from our analysis. Our goal has been to clean, normalize, and model unstructured receipt, brand, and user data so we can reliably answer key business questions.

**Business Insights:**
-----
**Top 5 Brands by Receipts Scanned (Most Recent Month):**
- Tostitos: 23
- Swanson: 11
- Cracker Barrel Cheese: 10
- Prego: 7
- Pepperidge Farm: 5

**Receipt Status Comparison:**
- **Accepted Receipts:**  
    - Total Items Purchased: 8,184  
    - Average Spend: $80.85
- **Rejected Receipts:**  
    - Total Items Purchased: 173  
    - Average Spend: $23.33

**New User Brand Analysis (Users Created in the Past 6 Months):**
- **Tostitos** leads with:
    - Total Spend: $15,799.37
    - Total Transactions: 23



**Unit Test Results (Data Quality Checks):**

- **Items without Brands:** 6,859  
    *(This indicates that many items in our dataset are missing brand associations.)*
- **Orphaned Receipts:** 148 
    *(These receipts have no associated user. For example, our SQL test returned:)*  
- **Receipts with Item Count Mismatch**: 40
    *(Some receipts show a discrepancy between the recorded number of items and the actual items data.)*



-------

Aditionally, I did have some questions I would like to discuss!

1. What defines an "active" user? Is it based on last login? Or maybe if Scanned a receipt in the last x days?
2. Should "FINISHED" receipt status always = "ACCEPTED"? 
3. Has the data schema changed recently, or are there planned updates that we should prepare for?
4. I understand that the `role` in the user data is supposed to have a constant value set to 'CONSUMER', is there a specific reason for overriding the `fetch-staff`?
5. Is there any specific transformations you guys would like me to handle upstream?


----


I also wanted to touch on some issues, resolutions, and performance concerns!

**Discovery of Data Quality Issues:**
- We initially encountered parsing errors due to the JSON Lines format.
- Nested fields (for example, the `_id` and `cpg` fields) required custom logic to extract values correctly.
- We found inconsistencies, such as missing keys (e.g., `brandCode` or `topBrand`) and numeric fields stored as strings.
- Our unit tests have surfaced issues like orphaned receipts, items without brands, and item count mismatches.

**What We Need to Resolve the Issues:**
- Clarification on any schema updates or changes.
- Detailed documentation on the expected values for key fields, so we can enforce consistency.
- Insights on any edge cases (such as unusual status values or placeholder test data) to ensure our cleaning logic covers all scenarios.

**Additional Information to Optimize Data Assets:**
- Details on data refresh frequency and anticipated growth to better design our scaling strategy.
- Any performance benchmarks or current production metrics that could guide our indexing and partitioning decisions.
- Business priorities that could help us focus on the most impactful transformations or aggregations.

**Performance & Scaling Concerns:**
- **Anticipated Concerns:**
    - Increased data volumes may affect query performance as we scale.
    - Complex joins across fact and dimension tables in our star schema could slow down reporting.
- **Our Plan:**
    - Implement efficient indexing strategies and consider partitioning large tables.
    - Monitor query performance regularly and optimize joins/aggregations.
    - Explore incremental data loading and caching common queries to reduce processing time.

----

I’m happy to discuss any of these points further and answer any questions you might have. Your feedback will help us refine our approach as we move toward a production-ready system.

Best regards,

Martin Kowalczyk