DELETE FROM DE3AT.PAKS_STG_CARDS_DEL WHERE 1=1;
COMMIT;

INSERT INTO DE3AT.PAKS_STG_CARDS_DEL (CARD_NUM) SELECT CARD_NUM FROM BANK.CARDS;

INSERT INTO DE3AT.PAKS_STG_CARDS (CARD_NUM, ACCOUNT, UPDATE_DT, STAGE_DT)
SELECT CARD_NUM, ACCOUNT, COALESCE(UPDATE_DT, CREATE_DT), CURRENT_DATE FROM BANK.CARDS
WHERE COALESCE(UPDATE_DT, CREATE_DT) > COALESCE(
    (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='CARDS'
    ),
    TO_DATE('1900-01-01 00:00:00', 'HH24:MI:SS')
);

-- 2. Выделение вставок и изменений (transform), вставка в их приемник (load)
INSERT INTO DE3AT.PAKS_DWH_DIM_CARDS_HIST (CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
SELECT
    STG.CARD_NUM,
    STG.ACCOUNT,
    STG.UPDATE_DT,
    TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
    'N'
FROM DE3AT.PAKS_DWH_DIM_CARDS_HIST TRG
INNER JOIN DE3AT.PAKS_STG_CARDS STG
ON (
    TRG.CARD_NUM = STG.CARD_NUM
    AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND TRG.DELETED_FLG = 'N')
WHERE 1 = 1
    AND STG.UPDATE_DT > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='CARDS')
    AND (1 = 0
        OR STG.ACCOUNT <> TRG.ACCOUNT
        OR (STG.ACCOUNT IS NULL AND TRG.ACCOUNT IS NOT NULL)
        OR (STG.ACCOUNT IS NOT NULL AND TRG.ACCOUNT IS NULL)
    );

MERGE INTO DE3AT.PAKS_DWH_DIM_CARDS_HIST TRG
USING DE3AT.PAKS_STG_CARDS STG
ON (STG.CARD_NUM = TRG.CARD_NUM
    AND TRG.DELETED_FLG = 'N'
    AND STG.UPDATE_DT > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='CARDS'
    )
)
WHEN MATCHED THEN
    UPDATE SET TRG.EFFECTIVE_TO = UPDATE_DT - INTERVAL  '1' SECOND
    WHERE TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND (1 = 0
        OR STG.ACCOUNT <> TRG.ACCOUNT
        OR (STG.ACCOUNT IS NULL AND TRG.ACCOUNT IS NOT NULL)
        OR (STG.ACCOUNT IS NOT NULL AND TRG.ACCOUNT IS NULL)
    )
WHEN NOT MATCHED THEN
    INSERT (CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG)
    VALUES (
        STG.CARD_NUM,
        STG.ACCOUNT,
        STG.UPDATE_DT,
        TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
        'N'
    ) WHERE COALESCE(STG.UPDATE_DT, STG.CREATE_DT) > (
        SELECT LAST_UPDATE_DT
        FROM DE3AT.PAKS_META_DATA
        WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='CARDS');

-- 3. Обработка удалений.
INSERT INTO DE3AT.PAKS_DWH_DIM_CARDS_HIST (
    CARD_NUM, ACCOUNT, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG
) SELECT TRG.CARD_NUM,
        TRG.ACCOUNT,
        CURRENT_DATE,
        TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
        'Y'
FROM DE3AT.PAKS_DWH_DIM_CARDS_HIST TRG
    LEFT JOIN DE3AT.PAKS_STG_CARDS_DEL DEL
    ON (
        DEL.CARD_NUM=TRG.CARD_NUM
        AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
        AND DELETED_FLG = 'N')
WHERE
      DEL.CARD_NUM IS NULL
      AND TRG.DELETED_FLG = 'N'
      AND CURRENT_DATE BETWEEN TRG.EFFECTIVE_FROM AND TRG.EFFECTIVE_TO ;


UPDATE DE3AT.PAKS_DWH_DIM_CARDS_HIST TRG
SET EFFECTIVE_TO = CURRENT_DATE - INTERVAL  '1' SECOND
WHERE 1 = 1
    AND TRG.CARD_NUM NOT IN (
        SELECT DEL.CARD_NUM FROM DE3AT.PAKS_STG_CARDS_DEL DEL
    )
    AND TRG.EFFECTIVE_TO = TO_DATE('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
    AND TRG.DELETED_FLG = 'N';



-- 4. Обновление метаданных.

UPDATE DE3AT.PAKS_META_DATA
SET LAST_UPDATE_DT = (SELECT MAX(UPDATE_DT) FROM DE3AT.PAKS_STG_CARDS)
WHERE SCHEMA_NAME='DE3AT' AND TABLE_NAME='CARDS';

COMMIT;