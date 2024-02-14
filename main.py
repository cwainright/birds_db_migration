"""
WORKFLOW -- repeat these steps for each destination table to scale up
1. add key-value pair to assets.assets.TBL_XWALK
    e.g., 'ncrn.DetectionEvent':'tbl_Events'
2. add source (Access) query to src.qry
    - query should match table name exactly and include all columns that need to be xwalked
    e.g., to query 'tbl_Events', the query must be named 'get_tbl_Events.sql' and the value in assets.assets.TBL_XWALK must be 'tbl_Events'
3. add xwalk function to src.tbl_xwalks.py
    e.g., _detection_event_xwalk()
4. add a function call to src.make_templates.create_xwalks() for the function you wrote in step 3
5. Add details to xwalk in src.tbl_xwalks.py function
    e.g., _detection_event_xwalk()
"""

import src.make_templates as mt
import src.check as c
testdict = mt.make_birds()
c.check_birds(testdict)