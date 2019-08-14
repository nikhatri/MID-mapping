
--==================Paysafe queries===============================

select * from _mbanalysis._paysafe_file_testing;

select * from _mbanalysis._paysafe_file_testing pay
left join mbdw.dimlocation loc
on pay."store id" = loc.mid
where loc.mid is not null;

select * from _mbanalysis._paysafe_monthly_payments_test limit 10;
select * from _mbanalysis._paysafe_file_testing;


--=================-=============================================
-- merge paysafe monthly file and create a version including store code
--===================================================================================

DROP TABLE IF EXISTS _mbanalysis._paysafe_monthly_payments_test;

CREATE TABLE _mbanalysis._paysafe_monthly_payments_test
DISTKEY(merchant)
SORTKEY(merchant)
AS
select hist.*, pay."store id" as storeid from 
_mbanalysis._paysafe_monthly_payments hist
left join
_mbanalysis._paysafe_file_testing pay
ON lower(hist."account name") = lower(pay."account name")
where pay."account name" is not null;

--==========================setting the report month======================

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_ReportDate;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_ReportDate
DISTKEY(reportdate)
SORTKEY(reportdate)
AS
SELECT DISTINCT to_date(PAY."month",'YYYYMM') AS reportdate
FROM _mbanalysis._paysafe_monthly_payments_test PAY
WHERE to_date(PAY."month",'YYYYMM') = '6/1/2019'; --  <--CHANGE THIS DATE ONLY!!!!  This will be applied to below queries

--=======================================================================================================================================   
--================--Master testing table========

--select * from _mbanalysis._PAYdebit_yearly_testing;
--select * from _mbanalysis._PAYdebit_yearly_testing where storeid ='929764E7-C8F3-456E-B1C9-643D7FE05E3F';

--===================MBO location table=============

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs
DISTKEY(mid)
SORTKEY(mid, "98mid",StudioID,locationid)
AS
select loc.studioid
      , loc.locationid
      , loc.mbaccountnumber
      , "98".mbaccountnumber AS "98mbaccountnumber"
      , CASE WHEN loc.mid = '' THEN null ELSE loc.mid END AS mid
      , CASE WHEN "98".mid = '' THEN null ELSE "98".mid END AS "98MID"
from (
SELECT studioid
      , locationid
      , mbaccountnumber
      , mid
      , locationname
FROM mbdw.dimlocation
WHERE locationid <> 98
      ) loc
LEFT JOIN (
            SELECT studioid
                  , locationid
                  , mbaccountnumber
                  , mid
            FROM mbdw.dimlocation
            WHERE locationid = 98
            ) "98"
ON loc.studioID = "98".studioid
WHERE (loc.mid IS NOT NULL AND loc.mid <> '') OR ("98".mid IS NOT NULL AND "98".mid <> '');

--==================Join master PAYdebit table with location data, 1 MID to 1 MB location===============================

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_SingleMID2SingleLocation;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_SingleMID2SingleLocation
DISTKEY(merchant)
SORTKEY(merchant, StudioID, locationid)
AS
SELECT to_date('20190601','YYYYMMDD') as reportdate
      --, MAP.merchant_id
      , MAP.storeid as storeid
      , MBO.StudioID
      , MBO.locationID
      , ISNULL(MBO.mbaccountnumber,MBO."98mbaccountnumber") AS mbaccountnumber
      , 1 AS LocsPerMex
      , '3 - Matched (Single MBO Location)'::TEXT AS MBO_MAPstatus
FROM _mbanalysis._paysafe_monthly_payments_test MAP
INNER JOIN _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs MBO
ON MAP.storeid = ISNULL(MBO.mid, MBO."98mid")
WHERE MAP.storeid IN (
                      SELECT PAY.storeid
                      FROM _mbanalysis._paysafe_monthly_payments_test PAY
                      INNER JOIN _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs MBO
                      ON PAY.storeid = ISNULL(MBO.mid, MBO."98mid")
                      GROUP BY PAY.storeid
                      HAVING count(*) = 1
                      );



--=======================================================================================================================================   
-- **No matched MIDs to MBO Locations**
DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_UnmatchedMIDs2MBOLocations;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_UnmatchedMIDs2MBOLocations
DISTKEY(merchant)
SORTKEY(merchant, StudioID, locationid)
AS
SELECT to_date('20190601','YYYYMMDD') as reportdate
      --, MAP.merchant_id
      , MAP.storeid as storeid
      , CAST(NULL AS INT) AS StudioID
      , CAST(NULL AS INT) AS locationid
      , NULL AS mbaccountnumber
      , NULL AS LocsPerMex
      , '4 - No match to MBO Location'::TEXT AS MBO_MAPstatus
FROM _mbanalysis._paysafe_monthly_payments_test MAP
LEFT JOIN _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs MBO
ON (MAP.storeid = MBO.mid OR MAP.storeid = MBO."98mid")
WHERE MBO.StudioID IS NULL;


--=======================================================================================================================================   
-- **Single MID to multiple MBO Locations**
DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_SingleMID2MultipleLocations;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_SingleMID2MultipleLocations
DISTKEY(merchant)
SORTKEY(merchant,StudioID,locationid)
AS
SELECT to_date('20190601','YYYYMMDD') as reportdate
      --, MAP.merchant_id
      , MAP.storeid as storeid
      , MBO.StudioID
      , MBO.locationID
      , ISNULL(MBO.mbaccountnumber,MBO."98mbaccountnumber") AS mbaccountnumber
      , LocsPerMex.NumLocs
      , '5 - Matched (Multiple MBO Locations)'::TEXT AS MBO_MAPstatus
FROM _mbanalysis._paysafe_monthly_payments_test MAP
INNER JOIN _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs MBO
ON MAP.storeid = ISNULL(MBO.mid, MBO."98mid")
INNER JOIN (
            SELECT MAP.storeid as storeid
                  , COUNT(*) AS NumLocs
            FROM _mbanalysis._paysafe_monthly_payments_test MAP
            INNER JOIN _mbanalysis.PAYMIDMAPPING_MBOActiveLocsMIDs MBO
            ON MAP.storeid = ISNULL(MBO.mid, MBO."98mid")
            GROUP BY MAP.storeid
            HAVING count(*) > 1
            ) LocsPerMex
ON MAP.storeid = LocsPerMex.storeid;


--=======================================================================================================================================   
--Master Table for PAY + MBO List 
--=======================================================================================================================================   
DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList
DISTKEY(merchant)
SORTKEY(merchant, StudioID, locationid)
AS
SELECT
          to_date('20190601','YYYYMMDD') as reportdate
         , PAY.storeid
         , PAY.agent
         , PAY.merchant
         , PAY.cur
         , PAY."account name" as account_name
         , PAY."txn volume" as txn_volume
         , PAY."txn count" as txn_count
         , PAY.revenues
         , PAY."net commission" as net_commission
         , PAY."source file" as source_file
         , COALESCE("1MIDto1MBOLoc".StudioID, "1MIDtoManyMBOLocs".StudioID) AS StudioID
         , COALESCE("1MIDto1MBOLoc".Locationid, "1MIDtoManyMBOLocs".Locationid) AS LocationID 
         , COALESCE("1MIDto1MBOLoc".mbo_mapstatus, NoMatchMIDs.mbo_mapstatus, "1MIDtoManyMBOLocs".mbo_mapstatus) AS mbo_mapstatus
FROM _mbanalysis._paysafe_monthly_payments_test PAY
LEFT JOIN _mbanalysis.PAYMIDMAPPING_SingleMID2SingleLocation "1MIDto1MBOLoc"
ON PAY.storeid = "1MIDto1MBOLoc".storeid
LEFT JOIN _mbanalysis.PAYMIDMAPPING_UnmatchedMIDs2MBOLocations NoMatchMIDs
ON PAY.storeid = NoMatchMIDs.storeid
LEFT JOIN _mbanalysis.PAYMIDMAPPING_SingleMID2MultipleLocations "1MIDtoManyMBOLocs"
ON PAY.storeid = "1MIDtoManyMBOLocs".storeid
;


--=======================================================================================================================================  
--Newt Base Tables
--=======================================================================================================================================  
--**StudioID + LocationID to Newt MBStudioID or MSAR_OldClientID and MB_LocationID**

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_MBO_Newt_MatchedtoCustomer;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_MBO_Newt_MatchedtoCustomer
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT DISTINCT PAY_MBO.reportdate
      , PAY_MBO.storeid
      , PAY_MBO.agent
      , PAY_MBO.merchant
      , PAY_MBO.cur
      , PAY_MBO.account_name
      , PAY_MBO.txn_volume
      , PAY_MBO.txn_count
      , PAY_MBO.revenues
      , PAY_MBO.net_commission
      , PAY_MBO.source_file
      , COALESCE(newt.mb_StudioID, newt2.mb_StudioID) AS newt_StudioID
      , COALESCE(newt.mb_LocationID, newt2.mb_LocationID) AS newt_LocationID
      , '6 - Matched (via StudioID or OldClientID)'::TEXT AS Newt_MAPStatus
FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList PAY_MBO
LEFT JOIN (
          SELECT ISNULL(newt.mb_studioid,newt2.mb_studioid) AS mb_StudioID
                , ISNULL(newt.mb_LocationID,newt2.mb_LocationID) AS mb_LocationID
                , newt.msar_date
                , newt.msar_clientid
          FROM _mbanalysis.newt newt
          LEFT JOIN _mbanalysis.newt newt2
            ON newt.msar_clientid = newt2.msar_clientid
            AND newt.msar_date = DATEADD('MONTH',1,newt2.msar_date) 
            ) newt
  ON PAY_MBO.studioid = newt.mb_studioID
  AND PAY_MBO.LocationID = newt.MB_LocationID
  AND PAY_MBO.reportdate = newt.msar_date
LEFT JOIN _mbanalysis.newt newt2
  ON PAY_MBO.studioid = newt2.msar_oldClientID
  AND PAY_MBO.LocationID = newt2.MB_LocationID
  AND PAY_MBO.reportdate = newt2.msar_date
WHERE (newt.msar_clientID IS NOT NULL OR newt2.msar_clientID IS NOT NULL)
;


--=======================================================================================================================================  
--**Distrubuted MEX values evenly to joined newt locations (i.e. if 5 locations had 1 mex, then (mex values / 5) to distribute dollars evenly to each location**
--No TCID dar mapping similar to TSYS so there is no need of redistribution here

--=======================================================================================================================================  
--**StudioID + LocationID to Newt MBStudioID or MSAR_OldClientID and MB_LocationID**\\
--No house accounts in Paysafe

--=========================================================================================================================================
--Matching data that doesn't match with any number of locations at all, match with the studio level instead

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT DISTINCT PAY_MBO.reportdate
      , PAY_MBO.storeid
      , PAY_MBO.agent
      , PAY_MBO.merchant
      , PAY_MBO.cur
      , PAY_MBO.account_name
      , PAY_MBO.txn_volume
      , PAY_MBO.txn_count
      , PAY_MBO.revenues
      , PAY_MBO.net_commission
      , PAY_MBO.source_file
      , COALESCE(newt_mb_StudioID.mb_StudioID, newt2_MSAR_OldClientID.msar_OldClientID) AS Newt_StudioID
      , ISNULL(COALESCE(newt_mb_StudioID.mb_locationid, newt2_MSAR_OldClientID.mb_locationid),0) AS Newt_LocationID
      , '8 - Matched (Applied to Parent Studio''s Locations)'::TEXT AS Newt_MAPStatus
FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList PAY_MBO
LEFT JOIN (
           SELECT newt.msar_date
                , ISNULL(newt.mb_studioid,newt2.mb_studioid) AS mb_StudioID
                , ISNULL(newt.mb_LocationID,newt2.mb_LocationID) AS mb_LocationID
                , tab1.LocCount
          FROM _mbanalysis.newt newt
          LEFT JOIN _mbanalysis.newt newt2
            ON newt.msar_clientid = newt2.msar_clientid
            AND newt.msar_date = DATEADD('MONTH',1,newt2.msar_date) 
          INNER JOIN 
              ( SELECT newt.msar_date
                      , ISNULL(newt.mb_studioid,newt2.mb_studioid) AS mb_StudioID
                      , COUNT(*) AS LocCount
                FROM _mbanalysis.newt
                LEFT JOIN _mbanalysis.newt newt2
                ON newt.msar_clientid = newt2.msar_clientid
                AND newt.msar_date = DATEADD('MONTH',1,newt2.msar_date) 
                GROUP BY newt.msar_date
                      , ISNULL(newt.mb_studioid,newt2.mb_studioid)
              ) tab1
          ON ISNULL(newt.mb_studioid,newt2.mb_studioid) = tab1.mb_studioid
          AND newt.msar_date = tab1.msar_date
          ) newt_mb_StudioID
  ON PAY_MBO.studioid = newt_mb_StudioID.mb_studioID
  AND PAY_MBO.reportdate = newt_mb_StudioID.msar_date
LEFT JOIN (
          SELECT newt.msar_date
                , newt.msar_OldClientID
                , newt.mb_locationid
                , tab1.LocCount
          FROM _mbanalysis.newt newt
          INNER JOIN 
              ( SELECT msar_date
                      , msar_OldClientID
                      , COUNT(*) AS LocCount
                FROM _mbanalysis.newt
                GROUP BY msar_date
                      , msar_OldClientID
              ) tab1
          ON newt.msar_OldClientID = tab1.msar_OldClientID
          AND newt.msar_date = tab1.msar_date
          ) newt2_MSAR_OldClientID
  ON PAY_MBO.studioid = newt2_MSAR_OldClientID.msar_oldClientID
  AND PAY_MBO.reportdate = newt2_MSAR_OldClientID.msar_date
WHERE (newt_mb_StudioID.mb_StudioID IS NOT NULL OR newt2_MSAR_OldClientID.msar_OldClientID IS NOT NULL)
AND PAY_MBO.storeid NOT IN (SELECT DISTINCT storeid FROM _mbanalysis.PAYMIDMAPPING_MBO_Newt_MatchedtoCustomer)
;



--======================================================================================
--Use mpar MIDs to determine studioid

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_matchedMPARMIDs;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_matchedMPARMIDs
DISTKEY(merchant)
SORTKEY(merchant, Newt_StudioID, Newt_LocationID)
AS
SELECT tab1.reportdate
      , tab1.storeid 
      , tab1.agent
      , tab1.merchant
      , tab1.cur
      , tab1.account_name
      , tab1.txn_volume / NewtLocs.NewtLocs AS txn_volume
      , tab1.txn_count / NewtLocs.NewtLocs AS txn_count
      , tab1.revenues / NewtLocs.NewtLocs AS revenues
      , tab1.net_commission / NewtLocs.NewtLocs AS net_commission
      , tab1.source_file
	  , mb_studioid AS Newt_StudioID
      , mb_locationid AS Newt_LocationID
      , null AS pay_mapstatus
      , null AS mbo_mapstatus
      , '15 - Matched MPAR MID'::TEXT AS Newt_MAPStatus
      , Null AS Newt_MIDRevenueLogic
FROM (
      SELECT DISTINCT PAY_MBO.reportdate
                  , PAY_MBO.storeid
			      , PAY_MBO.agent
			      , PAY_MBO.merchant
			      , PAY_MBO.cur
			      , PAY_MBO.account_name
			      , PAY_MBO.txn_volume
			      , PAY_MBO.txn_count
			      , PAY_MBO.revenues
			      , PAY_MBO.net_commission
			      , PAY_MBO.source_file
			      , newtMID.mb_studioid
                  , newtMID.mb_locationid
             FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList PAY_MBO
             LEFT JOIN (
                             SELECT DISTINCT msar_date
                                      , mpar_mid
                                      , mb_studioid
                                      , mb_locationid
                                FROM _mbanalysis.newt
                                WHERE --LEN(TRIM(mpar_mid)) = 16 AND, that's tsys
								(mpar_mid IS NOT NULL OR mpar_mid <> '')
                                AND mb_studioid IS NOT NULL
                                AND mb_locationid IS NOT NULL
                                --AND REGEXP_COUNT(TRIM(mpar_mid), '^[0-9]+$') > 0   , that's tsys      
                      ) newtMID
                  ON PAY_MBO.merchant = newtMID.mpar_mid
                  AND PAY_MBO.reportdate = newtMID.msar_date
          WHERE --association_name <> 'MINDBODY - House Accounts' AND 
		    	--PAY_MBO.merchant NOT IN(SELECT DISTINCT merchant_id FROM _mbanalysis.TSYSMIDMAPPING_PAY_MBO_Newt_MatchedtoCustomer_Adjusted) AND
                PAY_MBO.merchant NOT IN(SELECT DISTINCT merchant FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation)
		
				--what is the condition to filter out those records that are not matched in master list?
            
			AND (PAY_MBO.txn_volume <> 0 OR PAY_MBO.revenues <> 0)
            AND newtMID.mb_studioid is not null 
      ) tab1
      LEFT JOIN (
                  SELECT merchant, 
				  count(*) AS NewtLocs
                  FROM (
                  SELECT DISTINCT PAY_MBO.reportdate
                              , PAY_MBO.storeid
						      , PAY_MBO.agent
						      , PAY_MBO.merchant
						      , PAY_MBO.cur
						      , PAY_MBO.account_name
						      , PAY_MBO.txn_volume
						      , PAY_MBO.txn_count
						      , PAY_MBO.revenues
						      , PAY_MBO.net_commission
						      , PAY_MBO.source_file
						      , newtMID.mb_studioid
                              , newtMID.mb_locationid
                         FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList PAY_MBO
                  LEFT JOIN (
                             SELECT DISTINCT msar_date
                                      , mpar_mid
                                      , mb_studioid
                                      , mb_locationid
                                FROM _mbanalysis.newt
                                WHERE --LEN(TRIM(mpar_mid)) = 16 AND, that's tsys
                                (mpar_mid IS NOT NULL OR mpar_mid <> '')
                                AND mb_studioid IS NOT NULL
                                AND mb_locationid IS NOT NULL
                                --AND REGEXP_COUNT(TRIM(mpar_mid), '^[0-9]+$') > 0 , that's tsys        
                      ) newtMID
                  ON PAY_MBO.merchant = newtMID.mpar_mid
                  AND PAY_MBO.reportdate = newtMID.msar_date
                  WHERE --association_name <> 'MINDBODY - House Accounts' AND 
				  --merchant_id NOT IN(SELECT DISTINCT merchant_id FROM _mbanalysis.TSYSMIDMAPPING_PAY_MBO_Newt_MatchedtoCustomer_Adjusted) AND 
				  merchant NOT IN(SELECT DISTINCT merchant FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation)
                  AND (txn_volume <> 0 OR revenues <> 0)
                  AND newtMID.mb_studioid is not null)
                  GROUP BY merchant) NewtLocs
       ON tab1.merchant = NewtLocs.merchant
      ;
	  


--======================================================================================
--Data that is not matched to newt accounts

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_notmatchednewtaccounts;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_notmatchednewtaccounts
DISTKEY(merchant)
SORTKEY(merchant)
AS
SELECT reportdate
      , storeid 
      , agent
      , merchant
      , cur
      , account_name
      , txn_volume
      , txn_count
      , revenues
      , net_commission
      , Newt_StudioID AS Newt_StudioID
      , Newt_LocationID AS Newt_LocationID
      , null AS mex_dar_mapstatus
      , null AS mbo_mapstatus
      , '13 - Not Matched (Catch All Account)'::TEXT AS Newt_MAPStatus
      , Null AS Newt_MIDRevenueLogic
FROM (
      SELECT DISTINCT PAY_MBO.reportdate
                  , PAY_MBO.storeid
                  , PAY_MBO.agent
                  , PAY_MBO.merchant
                  , PAY_MBO.cur
                  , account_name
      , PAY_MBO.txn_volume
                  , PAY_MBO.txn_count
                  , PAY_MBO.revenues
                  , PAY_MBO.net_commission
                  , 123456789 AS newt_StudioID
                  , 123456789 AS newt_LocationID
                  , newtMID.mb_studioid
                  , newtMID.mb_locationid
             FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList PAY_MBO
           LEFT JOIN (
                     SELECT DISTINCT msar_date
                              , mpar_mid
                              , mb_studioid
                              , mb_locationid
                        FROM _mbanalysis.newt
                        WHERE -- LEN(TRIM(mpar_mid)) = 16 AND, this is for tsys
                        (mpar_mid IS NOT NULL OR mpar_mid <> '')
                        AND mb_studioid IS NOT NULL
                        AND mb_locationid IS NOT NULL
                        --AND REGEXP_COUNT(TRIM(mpar_mid), '^[0-9]+$') > 0         , this is for tsys
                      ) newtMID
                  ON PAY_MBO.merchant = newtMID.mpar_mid
                  AND PAY_MBO.reportdate = newtMID.msar_date
            WHERE --association_name <> 'MINDBODY - House Accounts' AND
            merchant NOT IN (SELECT DISTINCT merchant FROM _mbanalysis.PAYMIDMAPPING_MBO_Newt_MatchedtoCustomer) AND
            merchant NOT IN (SELECT DISTINCT merchant FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation)
            AND (txn_volume <> 0 OR revenues <> 0)
            AND newtMID.mb_studioid is null 
      )
      ;
	  
	  
--======================================================================================
--Unmatched locations, distribute evenly to parent location's studio

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts
DISTKEY(merchant)
SORTKEY(merchant,Newt_StudioID,Newt_LocationID)
AS
SELECT PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.reportdate
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.storeid
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.agent
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.merchant
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.cur
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.account_name
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.txn_volume / Mex_Locs AS txn_volume
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.txn_count / Mex_Locs AS txn_count
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.revenues / Mex_Locs AS revenues
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.net_commission / Mex_Locs AS net_commission
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.Newt_StudioID AS Newt_StudioID
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.Newt_LocationID AS Newt_LocationID
      , PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.Newt_MAPStatus
      , '10 - Distributed evenly to Parent Studio''s Locations'::TEXT AS Newt_MIDRevenueLogic
FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation
INNER JOIN (
          SELECT merchant
                 , COUNT(DISTINCT newt_studioID ||'|'|| newt_LocationID) AS mex_locs
          FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation
          GROUP BY merchant
          --HAVING COUNT(DISTINCT newt_studioID ||'|'|| newt_LocationID) >= 1
          ) Mex_LocationCount
ON PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation.merchant = Mex_LocationCount.merchant;


--======================================================================================
--Apply to single parent location

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_Applied2SingleLoc;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_Applied2SingleLoc
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT DISTINCT _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.reportdate
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.storeid
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.agent
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.merchant
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.cur
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.account_name
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.txn_volume
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.txn_count
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.revenues
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.net_commission
              , Newt_MAPStatus
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.Newt_StudioID
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.Newt_LocationID     
              , '12 - Applied to Single Parent Studio Location'::TEXT AS Newt_MIDRevenueLogic 
FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts
INNER JOIN (
          SELECT newt_studioID
                , newt_locationID
                , COUNT(DISTINCT merchant) AS Loc_Mexs
          FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts
          GROUP BY newt_studioID
                , newt_locationID
          HAVING COUNT(DISTINCT merchant) = 1
          ) Location_MexCount
ON _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.newt_studioID = Location_MexCount.newt_studioID
AND _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.newt_locationID = Location_MexCount.newt_locationID;

--========================================================================================================================

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_Sum2SingleLoc;

--Isn't a summation needed for adding the revenues distributed to many locations?

CREATE TABLE _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_Sum2SingleLoc
DISTKEY(merchant)
SORTKEY(merchant,Newt_StudioID,Newt_LocationID)
AS
SELECT reportdate
      , storeid
      , agent
      , merchant
      , cur
      , account_name
      , txn_volume
      , txn_count
      , revenues
      , net_commission
      , Newt_StudioID AS Newt_StudioID
      , Newt_LocationID AS Newt_LocationID
      , null AS Newt_MAPStatus
      , '9 - Split to Parent Studio Locations'::TEXT AS Newt_MIDRevenueLogic
FROM (
SELECT DISTINCT _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.reportdate
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.storeid
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.agent
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.merchant
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.cur
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.account_name
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.txn_volume
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.txn_count
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.revenues
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.net_commission
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.Newt_StudioID
              , _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.Newt_LocationID      
FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts
INNER JOIN (
          SELECT newt_studioID
                , newt_locationID
                , COUNT(DISTINCT merchant) AS Loc_Mexs
          FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts
          GROUP BY newt_studioID
                , newt_locationID
          HAVING COUNT(DISTINCT merchant) > 1
          ) Location_MexCount
ON _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.newt_studioID = Location_MexCount.newt_studioID
AND _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2ParentAccounts.newt_locationID = Location_MexCount.newt_locationID
    )
      ;


--======================================================================================
--Join All Mapping Lists
--removed the column MBO MAP status, check if it is really not needed.

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_JoinAllMaps;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_JoinAllMaps
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, MAP, Newt_StudioID, Newt_LocationID
FROM (
SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, COALESCE('7 - Applied to Single Matched LocationID', Newt_MAPStatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.PAYMIDMAPPING_MBO_Newt_MatchedtoCustomer
UNION ALL
--SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, COALESCE(Newt_MIDRevenueLogic, Newt_MAPStatus, mex_dar_mapstatus, mbo_mapstatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.TSYSMIDMAPPING_MEX_DAR_MBO_Newt_HouseAccounts
--UNION ALL
SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, COALESCE(Newt_MIDRevenueLogic, Newt_MAPStatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_notmatchednewtaccounts
UNION ALL
SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, COALESCE(Newt_MIDRevenueLogic, Newt_MAPStatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_matchedMPARMIDs
UNION ALL
SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, COALESCE(Newt_MIDRevenueLogic, Newt_MAPStatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_Applied2SingleLoc
UNION ALL
SELECT reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, COALESCE(Newt_MIDRevenueLogic, Newt_MAPStatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.PAYMIDMAPPING_PAY_MBO_Newt_MatchedtoToParentMINLocation_Sum2SingleLoc
--UNION ALL
--SELECT reportdate, merchant_id,net_settle_volume,stw_total_residual, total_settle_tickets, COALESCE(Newt_MIDRevenueLogic, Newt_MAPStatus, mex_dar_mapstatus, mbo_mapstatus) AS MAP, Newt_StudioID, CASE WHEN Newt_LocationID = 0 THEN null ELSE Newt_LocationID END AS Newt_LocationID FROM _mbanalysis.TSYSMIDMAPPING_MEX_DAR_MBO_Newt_MatchedtoToParentMINLocation_DistEvenly2SingleLoc
   );
   
   

--========================================================================================================================
--Find Customers with Multiple MIDs across multiple map types

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_MultipleMapTypes;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_MultipleMapTypes
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT reportdate
      , storeid
      , agent
      , merchant
      , cur
      , account_name
      , txn_volume
      , txn_count
      , revenues
      , net_commission
      , MAP
      , Newt_StudioID
      , Newt_LocationID
FROM _mbanalysis.PAYMIDMAPPING_JoinAllMaps
WHERE newt_studioid ||'|'||(CASE WHEN newt_locationid IS NULL THEN 0 ELSE newt_locationid END) IN (SELECT newt_studioid ||'|'||(CASE WHEN newt_locationid IS NULL THEN 0 ELSE newt_locationid END)
                                                                                                   FROM _mbanalysis.PAYMIDMAPPING_JoinAllMaps
                                                                                                   GROUP BY newt_studioID
                                                                                                         , newt_locationid
                                                                                                   HAVING count(*) > 1)
;



--========================================================================================================================
--sum multiple map types

DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_SumMultipleMapTypes;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_SumMultipleMapTypes
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT reportdate
	  , storeid
      , agent
      , LISTAGG(merchant,',') AS merchant
      , cur
      , account_name
      , SUM(txn_volume) AS txn_volume
      , SUM(txn_count) AS txn_count
      , SUM(revenues) AS revenues
      , SUM(net_commission) AS net_commission
      , '14 - Sum applied to Parent MIN LocationID V2'::TEXT AS MAP
      , Newt_StudioID
      , Newt_LocationID
FROM _mbanalysis.PAYMIDMAPPING_JoinAllMaps
WHERE newt_studioid ||'|'||(CASE WHEN newt_locationid IS NULL THEN 0 ELSE newt_locationid END) IN (SELECT newt_studioid ||'|'||newt_locationid FROM _mbanalysis.PAYMIDMAPPING_MultipleMapTypes)
GROUP BY reportdate
	  , storeid
      , agent
      , cur
      , account_name
      , Newt_StudioID
      , Newt_LocationID
;

--========================================================================================================================
--Final List
DROP TABLE IF EXISTS _mbanalysis.PAYMIDMAPPING_FINALV2;

CREATE TABLE _mbanalysis.PAYMIDMAPPING_FINALV2
DISTKEY(merchant)
SORTKEY(merchant,newt_StudioID,newt_LocationID)
AS
SELECT 
reportdate, storeid, agent, cast(merchant as char), cur, account_name, txn_volume, txn_count, revenues, net_commission, MAP, newt_studioid, newt_locationid
FROM _mbanalysis.PAYMIDMAPPING_JoinAllMaps WHERE newt_studioid ||'|'||(CASE WHEN newt_locationid IS NULL THEN 0 ELSE newt_locationid END) NOT IN (SELECT newt_studioid ||'|'||(CASE WHEN newt_locationid IS NULL THEN 0 ELSE newt_locationid END) FROM _mbanalysis.PAYMIDMAPPING_MultipleMapTypes)
UNION ALL
SELECT 
reportdate, storeid, agent, merchant, cur, account_name, txn_volume, txn_count, revenues, net_commission, MAP, newt_studioid, newt_locationid
FROM _mbanalysis.PAYMIDMAPPING_SumMultipleMapTypes
;


--========================================================================================================================
--Query Results

--List of MIDs not associated to Newt Customer
SELECT  DISTINCT _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList.mbo_mapstatus
        , _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList.mbo_mapstatus as newmap
        , _mbanalysis.PAYMIDMAPPING_JoinAllMaps.* 
FROM _mbanalysis.PAYMIDMAPPING_JoinAllMaps
INNER JOIN _mbanalysis._paysafe_monthly_payments_test on _mbanalysis.PAYMIDMAPPING_JoinAllMaps.merchant = _mbanalysis._paysafe_monthly_payments_test.merchant
INNER JOIN _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList on _mbanalysis.PAYMIDMAPPING_JoinAllMaps.merchant = _mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList.merchant
where map = '13 - Not Matched (Catch All Account)';

--Count of MIDs not associated to Newt Customer by Map Reason
SELECT  ISNULL(_mbanalysis.PAYMIDMAPPING_PAY_MBO_MasterList.mbo_mapstatus,_mbanalysis.TSYSMIDMAPPING_TSYSMIDTCIDMAP_Final.mex_dar_mapstatus) as "map failure reason"
        , COUNT(DISTINCT _mbanalysis.TSYSMIDMAPPING_JoinAllMaps.merchant_id)
FROM _mbanalysis.TSYSMIDMAPPING_JoinAllMaps
INNER JOIN _mbanalysis.TSYSMIDMAPPING_TSYSMIDTCIDMAP_Final on _mbanalysis.TSYSMIDMAPPING_JoinAllMaps.merchant_id = _mbanalysis.TSYSMIDMAPPING_TSYSMIDTCIDMAP_Final.merchant_id
INNER JOIN _mbanalysis.TSYSMIDMAPPING_MEX_DAR_MBO_MasterList on _mbanalysis.TSYSMIDMAPPING_JoinAllMaps.merchant_id = _mbanalysis.TSYSMIDMAPPING_MEX_DAR_MBO_MasterList.merchant_id
where map = '13 - Not Matched (Catch All Account)'
GROUP BY ISNULL(_mbanalysis.TSYSMIDMAPPING_MEX_DAR_MBO_MasterList.mbo_mapstatus,_mbanalysis.TSYSMIDMAPPING_TSYSMIDTCIDMAP_Final.mex_dar_mapstatus);

--List of all MIDs associated to Newt (StudioID = 999999 are House Accts, StudioID = 123456789 are not matched)
SELECT * FROM _mbanalysis.TSYSMIDMAPPING_JoinAllMaps;

--List of Newt Customers with MIDs values
SELECT * FROM _mbanalysis.TSYSMIDMAPPING_FINALV2;