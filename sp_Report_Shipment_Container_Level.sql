ALTER PROCEDURE sp_Report_Shipment_Container_Level
    @CurrentCountry CHAR(2),
    @CompanyPK uniqueidentifier,
    @TransportMode VARCHAR(255),
    @JobDateFrom DATETIME,
    @JobDateTo DATETIME,    
    @FirstLoadETDFrom DATETIME,
    @FirstLoadETDTo DATETIME,
    @LastDischargeETAFrom DATETIME,
    @LastDischargeETATo DATETIME,
    @JobType VARCHAR(255)

AS
BEGIN
    SET NOCOUNT ON;

    WITH Base AS (
        SELECT
            JS.JS_UniqueConsignRef AS ShipmentID,
            JS.JS_HouseBill AS HBL,
            JS.JS_HouseBillIssueDate AS BLIssueDate,
            JK.JK_RL_NKDischargePort AS Delivery,
            JK.JK_UniqueConsignRef AS ConsolID,
            JK.JK_MasterBillNum AS MBL,
            MCT.JW_RL_NKLoadPort AS POL,
            MCT.JW_RL_NKDiscPort AS POD,
            MCT.JW_ETD AS ETD,
            MCT.JW_ATD AS ATD,
            MCT.JW_ETA AS ETA,
            MCT.JW_ATA AS ATA,
            FLCT.FirstLoad_ETD,
            FLCT.LastDischarge_ETA,
            Consignor.FullName AS Shipper,
            Consignee.FullName AS Consignee,
            Carrier.OH_FullName AS Carrier,
            JC.JC_ContainerNum AS ContainerNum,
            RC.RC_Code AS ContainerTypeCode,
            JC.JC_GrossWeight As GrossWeight,
            JL.JL_ActualVolume AS M3,
            CF_PTS.XV_Data AS InforPTSNumber,
            JK.JK_AgentsReference AS AgentsReference,
            JD.JS_JobDate

        FROM dbo.csfn_JobShipmentsWithDirectionCompanyBased(@CurrentCountry, @CompanyPK) JS
            OUTER APPLY dbo.GetCustomFieldByName(
                JS.JS_PK,
                '(DO NOT EDIT)Infor PTS Number'
            ) CF_PTS
            LEFT JOIN dbo.csfn_ShipmentMainConsol(@CurrentCountry) SCL
                ON SCL.JS_PK = JS.JS_PK
            LEFT JOIN dbo.JobConsol JK
                ON JK.JK_PK = SCL.JK_PK
            LEFT JOIN dbo.csfn_MainConsolTransport(@CurrentCountry) MCT
                ON MCT.JW_JK = JK.JK_PK
            LEFT JOIN dbo.ViewFirstLastConsolTransport FLCT
                ON FLCT.ParentType = 'CON'
                AND FLCT.JK = JK.JK_PK
            LEFT JOIN dbo.JobContainer JC
                ON JC.JC_JK = JK.JK_PK
            LEFT JOIN dbo.RefContainer RC
                ON RC.RC_PK = JC.JC_RC
            LEFT JOIN dbo.JobContainerPackPivot J6
                ON J6.J6_JC = JC.JC_PK
            LEFT JOIN dbo.JobPackLines JL
                ON JL.JL_PK = J6.J6_JL
            LEFT JOIN dbo.ctfn_JobShipmentOrg('CRD') Consignor
                ON Consignor.JS_PK = JS.JS_PK
            LEFT JOIN dbo.ctfn_JobShipmentOrg('CED') Consignee
                ON Consignee.JS_PK = JS.JS_PK
            LEFT JOIN dbo.OrgAddress CarrierAddr
                ON CarrierAddr.OA_PK = JK.JK_OA_ShippingLineAddress
            LEFT JOIN dbo.OrgHeader Carrier
                ON Carrier.OH_PK = CarrierAddr.OA_OH
            LEFT JOIN dbo.JobHeader JH
                ON JH.JH_ParentTableCode = 'JS'
                AND JH.JH_ParentID = JS.JS_PK
                AND JH.JH_GC = @CompanyPK
                AND JH.JH_IsActive = 1
            LEFT JOIN dbo.csfn_JobConsolWithDirectionCompanyBased(@CurrentCountry, @CompanyPK) AS JW
                ON JW.JK_PK = JK.JK_PK
            LEFT JOIN (
                SELECT D3_JH, MAX(D3_RecognitionDate) AS D3_RecognitionDate
                FROM dbo.JobChargeRevRecognition
                GROUP BY D3_JH
            ) D3 ON D3.D3_JH = JH.JH_PK
            CROSS APPLY (
                SELECT JS_JobDate =
                    CASE
                        WHEN D3.D3_RecognitionDate IS NULL THEN
                            CASE WHEN JW.JK_Direction = 'IMP'
                                THEN FLCT.LastDischarge_ETA
                                ELSE FLCT.FirstLoad_ETD
                            END
                        WHEN MONTH(
                                CASE WHEN JW.JK_Direction = 'IMP'
                                    THEN FLCT.LastDischarge_ETA
                                    ELSE FLCT.FirstLoad_ETD
                                END
                            ) = MONTH(D3.D3_RecognitionDate)
                        THEN
                            CASE WHEN JW.JK_Direction = 'IMP'
                                THEN FLCT.LastDischarge_ETA
                                ELSE FLCT.FirstLoad_ETD
                            END
                        ELSE D3.D3_RecognitionDate
                    END
            ) JD

        WHERE
            JC.JC_ContainerNum IS NOT NULL
            AND NULLIF(LTRIM(RTRIM(RC.RC_Code)), '') IS NOT NULL
            AND (@TransportMode = '' OR JS.JS_TransportMode = @TransportMode)
            AND
            (
                (FLCT.FirstLoad_ETD IS NOT NULL
                    AND (@FirstLoadETDFrom IS NULL OR FLCT.FirstLoad_ETD >= @FirstLoadETDFrom)
                    AND (@FirstLoadETDTo   IS NULL OR FLCT.FirstLoad_ETD < @FirstLoadETDTo)
                )
                OR (@FirstLoadETDFrom IS NULL AND @FirstLoadETDTo IS NULL)
            )

            AND
            (
                (FLCT.LastDischarge_ETA IS NOT NULL
                    AND (@LastDischargeETAFrom IS NULL OR FLCT.LastDischarge_ETA >= @LastDischargeETAFrom)
                    AND (@LastDischargeETATo   IS NULL OR FLCT.LastDischarge_ETA < @LastDischargeETATo)
                )
                OR (@LastDischargeETAFrom IS NULL AND @LastDischargeETATo IS NULL)
            )

            AND
            (
                (NULLIF(@JobDateFrom,'') IS NULL OR @JobDateFrom = '' OR JD.JS_JobDate >= @JobDateFrom)
                AND
                (NULLIF(@JobDateTo,'')   IS NULL OR @JobDateTo   = ''
                    OR JD.JS_JobDate < DATEADD(DAY, 1, CONVERT(date, @JobDateTo)))
            )
            AND
            (
                @JobType IS NULL
                OR @JobType = ''
                OR @JobType = 'ALL JOB'
                OR (@JobType = 'KM Job'     AND CF_PTS.XV_Data IS NOT NULL)
            )
    )

    SELECT
        POL,
        POD,
        Delivery,
        Shipper,
        Consignee,
        ETD,
        ATD,
        BLIssueDate,
        ETA,
        ATA,
        MBL,
        HBL,
        ContainerNum,
        ContainerTypeCode,
        Carrier,
        GrossWeight,
        SUM(M3) AS M3,
        InforPTSNumber,
        AgentsReference
    FROM Base
    GROUP BY
        POL,
        POD,
        Delivery,
        Shipper,
        Consignee,
        ETD,
        ATD,
        BLIssueDate,
        ETA,
        ATA,
        MBL,
        HBL,
        ContainerNum,
        ContainerTypeCode,
        Carrier,
        GrossWeight,
        InforPTSNumber,
        AgentsReference

END
