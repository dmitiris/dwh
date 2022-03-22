-- 2. Выделение вставок и изменений (transform), вставка в их приемник (load)
INSERT INTO DE3AT.PAKS_DWH_DIM_TERMINALS_HIST (
    TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG
) SELECT
         STG.TERMINAL_ID,
         STG.TERMINAL_TYPE,
         STG.TERMINAL_CITY,
         STG.TERMINAL_ADDRESS,
         STG.STAGE_DT,
         TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
         'N'
    FROM DE3AT.PAKS_DWH_DIM_TERMINALS_HIST TRG
        INNER JOIN DE3AT.PAKS_STG_TERMINALS STG ON (
            STG.TERMINAL_ID=TRG.TERMINAL_ID
                AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                AND DELETED_FLG = 'N'
        ) WHERE STG.STAGE_DT > (
                SELECT LAST_UPDATE_DT
                FROM DE3AT.PAKS_META_DATA
                WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='TERMINALS')
            AND (1 = 0
                OR STG.TERMINAL_TYPE <> TRG.TERMINAL_TYPE
                OR STG.TERMINAL_CITY <> TRG.TERMINAL_CITY
                OR STG.TERMINAL_ADDRESS <> TRG.TERMINAL_ADDRESS
                OR (STG.TERMINAL_TYPE IS NULL AND TRG.TERMINAL_TYPE IS NOT NULL)
                OR (STG.TERMINAL_CITY IS NULL AND TRG.TERMINAL_CITY IS NOT NULL)
                OR (STG.TERMINAL_ADDRESS IS NULL AND TRG.TERMINAL_ADDRESS IS NOT NULL)
                OR (STG.TERMINAL_TYPE IS NOT NULL AND TRG.TERMINAL_TYPE IS NULL)
                OR (STG.TERMINAL_CITY IS NOT NULL AND TRG.TERMINAL_CITY IS NULL)
                OR (STG.TERMINAL_ADDRESS IS NOT NULL AND TRG.TERMINAL_ADDRESS IS NULL)
            );

MERGE INTO DE3AT.PAKS_DWH_DIM_TERMINALS_HIST TRG
USING DE3AT.PAKS_STG_TERMINALS STG
ON (STG.TERMINAL_ID = TRG.TERMINAL_ID AND TRG.DELETED_FLG = 'N' AND STG.STAGE_DT > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='TERMINALS'))
WHEN MATCHED THEN
    UPDATE SET TRG.EFFECTIVE_TO = STG.STAGE_DT - INTERVAL  '1' SECOND
    WHERE TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND (1 = 0
        OR STG.TERMINAL_TYPE <> TRG.TERMINAL_TYPE
        OR STG.TERMINAL_CITY <> TRG.TERMINAL_CITY
        OR STG.TERMINAL_ADDRESS <> TRG.TERMINAL_ADDRESS
        OR (STG.TERMINAL_TYPE IS NULL AND TRG.TERMINAL_TYPE IS NOT NULL)
        OR (STG.TERMINAL_CITY IS NULL AND TRG.TERMINAL_CITY IS NOT NULL)
        OR (STG.TERMINAL_ADDRESS IS NULL AND TRG.TERMINAL_ADDRESS IS NOT NULL)
        OR (STG.TERMINAL_TYPE IS NOT NULL AND TRG.TERMINAL_TYPE IS NULL)
        OR (STG.TERMINAL_CITY IS NOT NULL AND TRG.TERMINAL_CITY IS NULL)
        OR (STG.TERMINAL_ADDRESS IS NOT NULL AND TRG.TERMINAL_ADDRESS IS NULL)
    )
WHEN NOT MATCHED THEN
    INSERT (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
    VALUES (
        STG.TERMINAL_ID,
        STG.TERMINAL_TYPE,
        STG.TERMINAL_CITY,
        STG.TERMINAL_ADDRESS,
        STG.STAGE_DT,
        TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
        'N'
    ) WHERE STAGE_DT > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='TERMINALS');

-- 3. Обработка удалений.
INSERT INTO DE3AT.PAKS_DWH_DIM_TERMINALS_HIST (
    TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG
) SELECT TRG.TERMINAL_ID,
        TRG.TERMINAL_TYPE,
        TRG.TERMINAL_CITY,
        TRG.TERMINAL_ADDRESS,
        STG.STAGE_DT,
        TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
        'Y'
FROM DE3AT.PAKS_DWH_DIM_TERMINALS_HIST TRG
    LEFT JOIN DE3AT.PAKS_STG_TERMINALS_DEL DEL
    ON (DEL.TERMINAL_ID=TRG.TERMINAL_ID AND DELETED_FLG = 'N')
    JOIN (SELECT MAX(STAGE_DT) AS STAGE_DT FROM PAKS_STG_TERMINALS) STG ON STG.STAGE_DT IS NOT NULL
WHERE DEL.TERMINAL_ID IS NULL
    AND STG.STAGE_DT BETWEEN TRG.EFFECTIVE_FROM AND TRG.EFFECTIVE_TO
    AND TRG.DELETED_FLG = 'N';

UPDATE DE3AT.PAKS_DWH_DIM_TERMINALS_HIST TRG
SET EFFECTIVE_TO = (SELECT MAX(STAGE_DT) AS STAGE_DT FROM PAKS_STG_TERMINALS) - INTERVAL  '1' SECOND
WHERE 1 = 1
    AND TRG.TERMINAL_ID NOT IN (
        SELECT DEL.TERMINAL_ID FROM DE3AT.PAKS_STG_TERMINALS_DEL DEL
    )
    AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND TRG.DELETED_FLG = 'N';



-- 4. Обновление метаданных.

UPDATE DE3AT.PAKS_META_DATA
SET LAST_UPDATE_DT = (SELECT MAX(STAGE_DT) FROM DE3AT.PAKS_STG_TERMINALS)
WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='TERMINALS';

COMMIT;
