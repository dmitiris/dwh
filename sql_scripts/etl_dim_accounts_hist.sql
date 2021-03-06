DELETE FROM DE3AT.PAKS_STG_ACCOUNTS_DEL WHERE 1=1;
COMMIT;

INSERT INTO DE3AT.PAKS_STG_ACCOUNTS_DEL (ACCOUNT) SELECT ACCOUNT FROM BANK.ACCOUNTS;

INSERT INTO DE3AT.PAKS_STG_ACCOUNTS (ACCOUNT, VALID_TO, CLIENT, UPDATE_DT, STAGE_DT)
SELECT ACCOUNT, VALID_TO, CLIENT, COALESCE(UPDATE_DT, CREATE_DT), CURRENT_DATE FROM BANK.ACCOUNTS
WHERE COALESCE(UPDATE_DT, CREATE_DT) > COALESCE(
    (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='ACCOUNTS'
    ),
    TO_DATE('1900-01-01 00:00:00', 'HH24:MI:SS')
);

-- 2. Выделение вставок и изменений (transform), вставка в их приемник (load)
INSERT INTO DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST (ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
SELECT
    STG.ACCOUNT,
    STG.VALID_TO,
    STG.CLIENT,
    STG.UPDATE_DT,
    TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
    'N'
FROM DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST TRG
INNER JOIN DE3AT.PAKS_STG_ACCOUNTS STG
ON (
    TRG.ACCOUNT = STG.ACCOUNT
    AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND TRG.DELETED_FLG = 'N')
WHERE 1 = 1
    AND STG.UPDATE_DT > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='ACCOUNTS')
    AND (1 = 0
        OR STG.VALID_TO <> TRG.VALID_TO
        OR STG.CLIENT <> TRG.CLIENT
        OR (STG.VALID_TO IS NULL AND TRG.VALID_TO IS NOT NULL)
        OR (STG.CLIENT IS NULL AND TRG.CLIENT IS NOT NULL)
        OR (STG.VALID_TO IS NOT NULL AND TRG.VALID_TO IS NULL)
        OR (STG.CLIENT IS NOT NULL AND TRG.CLIENT IS NULL)
    );

MERGE INTO DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST TRG
USING DE3AT.PAKS_STG_ACCOUNTS STG
ON (STG.ACCOUNT = TRG.ACCOUNT
    AND TRG.DELETED_FLG = 'N'
    AND STG.UPDATE_DT > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='ACCOUNTS'
    )
)
WHEN MATCHED THEN
    UPDATE SET TRG.EFFECTIVE_TO = UPDATE_DT - INTERVAL  '1' SECOND
    WHERE TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND (1 = 0
        OR STG.VALID_TO <> TRG.VALID_TO
        OR STG.CLIENT <> TRG.CLIENT
        OR (STG.VALID_TO IS NULL AND TRG.VALID_TO IS NOT NULL)
        OR (STG.CLIENT IS NULL AND TRG.CLIENT IS NOT NULL)
        OR (STG.VALID_TO IS NOT NULL AND TRG.VALID_TO IS NULL)
        OR (STG.CLIENT IS NOT NULL AND TRG.CLIENT IS NULL)
    )
WHEN NOT MATCHED THEN
    INSERT (ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
    VALUES (
        STG.ACCOUNT,
        STG.VALID_TO,
        STG.CLIENT,
        STG.UPDATE_DT,
        TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
        'N'
    ) WHERE COALESCE(STG.UPDATE_DT, STG.CREATE_DT) > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='ACCOUNTS');

-- 3. Обработка удалений.
INSERT INTO DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST (
    ACCOUNT, VALID_TO, CLIENT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG
) SELECT TRG.ACCOUNT,
        TRG.VALID_TO,
        TRG.CLIENT,
        CURRENT_DATE,
        TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
        'Y'
FROM DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST TRG
    LEFT JOIN DE3AT.PAKS_STG_ACCOUNTS_DEL DEL
    ON (
        DEL.ACCOUNT=TRG.ACCOUNT
        AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        AND DELETED_FLG = 'N')
WHERE
      DEL.ACCOUNT IS NULL
      AND TRG.DELETED_FLG = 'N'
      AND CURRENT_DATE BETWEEN TRG.EFFECTIVE_FROM AND TRG.EFFECTIVE_TO ;


UPDATE DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST TRG
SET EFFECTIVE_TO = CURRENT_DATE - INTERVAL  '1' SECOND
WHERE 1 = 1
    AND TRG.ACCOUNT NOT IN (
        SELECT DEL.ACCOUNT FROM DE3AT.PAKS_STG_ACCOUNTS_DEL DEL
    )
    AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND TRG.DELETED_FLG = 'N';

-- 4. Обновление метаданных.
UPDATE DE3AT.PAKS_META_DATA
SET LAST_UPDATE_DT = (SELECT MAX(UPDATE_DT) FROM DE3AT.PAKS_STG_ACCOUNTS)
WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='ACCOUNTS';

COMMIT;
