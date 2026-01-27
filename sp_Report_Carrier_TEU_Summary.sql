CREATE PROCEDURE sp_Report_Carrier_TEU_Summary
    @CurrentCountry CHAR(2),
    @CompanyPK UNIQUEIDENTIFIER,
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
            COALESCE(NULLIF(Carrier.OH_FullName,''), 'UNKNOWN') AS Carrier,
            JC.JC_ContainerNum AS ContainerNum,
            RC.RC_StorageClass AS ContainerTypeCode,
            CF_PTS.XV_Data AS InforPTSNumber,
            JD.JS_JobDate

        FROM dbo.JobShipment JS
        OUTER APPLY dbo.GetCustomFieldByName(
            JS.JS_PK,
            '(DO NOT EDIT)Infor PTS Number'
        ) CF_PTS        
        LEFT JOIN dbo.JobConShipLink JCSL
            ON JCSL.JN_JS = JS.JS_PK
        LEFT JOIN dbo.JobConsol JK
            ON JK.JK_PK = JCSL.JN_JK
        LEFT JOIN dbo.JobContainer JC
            ON JC.JC_JK = JK.JK_PK
        LEFT JOIN dbo.RefContainer RC
            ON RC.RC_PK = JC.JC_RC
        LEFT JOIN dbo.OrgAddress CarrierAddr
            ON CarrierAddr.OA_PK = JK.JK_OA_ShippingLineAddress
        LEFT JOIN dbo.OrgHeader Carrier
            ON Carrier.OH_PK = CarrierAddr.OA_OH
        LEFT JOIN dbo.ViewFirstLastConsolTransport FLCT
            ON FLCT.ParentType = 'CON'
            AND FLCT.JK = JK.JK_PK
        LEFT JOIN dbo.JobHeader JH
            ON JH.JH_ParentID = JS.JS_PK
            AND JH.JH_ParentTableCode = 'JS'
            AND JH.JH_GC = @CompanyPK
            AND JH.JH_IsActive = 1
        LEFT JOIN dbo.csfn_JobConsolWithDirectionCompanyBased(@CurrentCountry, @CompanyPK) JW
            ON JW.JK_PK = JK.JK_PK
        LEFT JOIN (
            SELECT D3_JH, MAX(D3_RecognitionDate) AS D3_RecognitionDate
            FROM dbo.JobChargeRevRecognition
            GROUP BY D3_JH
        ) D3
            ON D3.D3_JH = JH.JH_PK
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
                OR (@JobType = 'Non KM Job' AND CF_PTS.XV_Data IS NULL)
            )
    ),

    ContainerTEU AS (
        SELECT
            Carrier,
            ContainerNum,
            TEU =
                CASE
                    WHEN ContainerTypeCode LIKE '20%' THEN 1
                    WHEN ContainerTypeCode LIKE '40%' THEN 2
                    WHEN ContainerTypeCode LIKE '45%' THEN 2
                    ELSE 0
                END
        FROM Base
    ),

    CarrierAgg AS (
        SELECT
            Carrier,
            COUNT(DISTINCT ContainerNum) AS Total_Containers,
            SUM(TEU) AS TEU
        FROM ContainerTEU
        GROUP BY Carrier
        HAVING SUM(TEU) <> 0
    )

    SELECT
        Carrier,
        Total_Containers,
        TEU,
        SUM(TEU) OVER () AS Total_TEU
    FROM CarrierAgg
    ORDER BY Carrier

END