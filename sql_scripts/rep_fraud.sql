MERGE INTO DE3AT.PAKS_REP_FRAUD REP USING (
    SELECT
        TRANS.TRANSACTION_DATE AS EVENT_DT,
        CL.PASSPORT_NUM AS PASSPORT,
        CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
        CL.PHONE,
        'INVALID AGREEMENT' AS EVENT_TYPE,
        (SELECT LAST_UPDATE_DT FROM de3at.PAKS_META_DATA WHERE TABLE_NAME='TRANSACTIONS') AS REPORT_DT
    FROM PAKS_DWH_FACT_TRANSACTIONS TRANS
        LEFT JOIN PAKS_DWH_DIM_CARDS_HIST CARDS
            ON TRANS.TRANSACTION_DATE BETWEEN CARDS.EFFECTIVE_FROM AND CARDS.EFFECTIVE_TO
            AND TRANS.CARD_NUM = CARDS.CARD_NUM
        LEFT JOIN PAKS_DWH_DIM_ACCOUNTS_HIST ACC
            ON CARDS.EFFECTIVE_FROM BETWEEN ACC.EFFECTIVE_FROM AND ACC.EFFECTIVE_TO
                   AND CARDS.ACCOUNT = ACC.ACCOUNT
        LEFT JOIN PAKS_DWH_DIM_CLIENTS_HIST CL
            ON ACC.EFFECTIVE_FROM BETWEEN CL.EFFECTIVE_FROM AND CL.EFFECTIVE_TO
                   AND ACC.CLIENT = CL.CLIENT_ID
    WHERE  ACC.VALID_TO + INTERVAL '1' DAY < TRANS.TRANSACTION_DATE) RES
ON (
        REP.EVENT_DT = RES.EVENT_DT
)
WHEN NOT MATCHED THEN
INSERT (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
VALUES (RES.EVENT_DT, RES.PASSPORT, RES.FIO, RES.PHONE, RES.EVENT_TYPE, RES.REPORT_DT);

MERGE INTO DE3AT.PAKS_REP_FRAUD REP USING (
    SELECT
        TRANS.TRANSACTION_DATE AS EVENT_DT,
        CL.PASSPORT_NUM AS PASSPORT,
        CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
        CL.PHONE,
        'INVALID PASSPORT' AS EVENT_TYPE,
        (SELECT LAST_UPDATE_DT FROM de3at.PAKS_META_DATA WHERE TABLE_NAME='TRANSACTIONS') AS REPORT_DT
    FROM PAKS_DWH_FACT_TRANSACTIONS TRANS
        LEFT JOIN PAKS_DWH_DIM_CARDS_HIST CARDS
            ON TRANS.TRANSACTION_DATE BETWEEN CARDS.EFFECTIVE_FROM AND CARDS.EFFECTIVE_TO
                AND TRANS.CARD_NUM = CARDS.CARD_NUM
        LEFT JOIN PAKS_DWH_DIM_ACCOUNTS_HIST ACC
            ON CARDS.EFFECTIVE_FROM BETWEEN ACC.EFFECTIVE_FROM AND ACC.EFFECTIVE_TO
                AND CARDS.ACCOUNT = ACC.ACCOUNT
        LEFT JOIN PAKS_DWH_DIM_CLIENTS_HIST CL
            ON ACC.EFFECTIVE_FROM BETWEEN CL.EFFECTIVE_FROM AND CL.EFFECTIVE_TO
                AND ACC.CLIENT = CL.CLIENT_ID
        LEFT JOIN PAKS_DWH_FACT_PSSPRT_BLCKLST BLCK
            ON BLOCK_DATE BETWEEN CL.EFFECTIVE_FROM AND CL.EFFECTIVE_TO
                AND CL.PASSPORT_NUM=BLCK.PASSPORT_NUM
    WHERE 1=0
       OR PASSPORT_VALID_TO + INTERVAL '1' DAY < TRANS.TRANSACTION_DATE
       OR BLOCK_DATE IS NOT NULL AND BLOCK_DATE + interval '1' DAY < TRANS.TRANSACTION_DATE) RES
ON (
        REP.EVENT_DT = RES.EVENT_DT
)
WHEN NOT MATCHED THEN
INSERT (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
VALUES (RES.EVENT_DT, RES.PASSPORT, RES.FIO, RES.PHONE, RES.EVENT_TYPE, RES.REPORT_DT);

MERGE INTO DE3AT.PAKS_REP_FRAUD REP USING (
    WITH t AS (
        SELECT
            TRANS.TRANSACTION_DATE AS EVENT_DT,
            CL.PASSPORT_NUM AS PASSPORT,
            CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
            CL.PHONE,
            'INVALID CITY' AS EVENT_TYPE,
            (SELECT LAST_UPDATE_DT FROM de3at.PAKS_META_DATA WHERE TABLE_NAME='TRANSACTIONS') AS REPORT_DT,
            TERMS.TERMINAL_CITY,
            LAG(TERMS.TERMINAL_CITY) OVER (PARTITION BY TRANS.CARD_NUM ORDER BY TRANS.TRANSACTION_DATE) AS PREV_CITY,
            LAG(TRANS.TRANSACTION_DATE) over (PARTITION BY TRANS.CARD_NUM ORDER BY TRANSACTION_DATE)    AS PREV_TIME,
            ROW_NUMBER() over (PARTITION BY TRANS.CARD_NUM, TERMINAL_CITY ORDER BY TRANSACTION_DATE) AS OP_NUM,
            TRANS.TRANSACTION_DATE - INTERVAL '1' HOUR AS ONE_HOUR_INTERVAL
        FROM PAKS_DWH_FACT_TRANSACTIONS TRANS
            LEFT JOIN PAKS_DWH_DIM_TERMINALS_HIST TERMS
                ON TRANS.TRANSACTION_DATE BETWEEN TERMS.EFFECTIVE_FROM AND TERMS.EFFECTIVE_TO
                    AND TRANS.TERMINAL_ID = TERMS.TERMINAL_ID
            LEFT JOIN PAKS_DWH_DIM_CARDS_HIST CARDS
                ON TRANS.TRANSACTION_DATE BETWEEN CARDS.EFFECTIVE_FROM AND CARDS.EFFECTIVE_TO
                    AND TRANS.CARD_NUM = CARDS.CARD_NUM
            LEFT JOIN PAKS_DWH_DIM_ACCOUNTS_HIST ACC
                ON CARDS.EFFECTIVE_FROM BETWEEN ACC.EFFECTIVE_FROM AND ACC.EFFECTIVE_TO
                    AND CARDS.ACCOUNT = ACC.ACCOUNT
            LEFT JOIN PAKS_DWH_DIM_CLIENTS_HIST CL
                ON ACC.EFFECTIVE_FROM BETWEEN CL.EFFECTIVE_FROM AND CL.EFFECTIVE_TO
                    AND ACC.CLIENT = CL.CLIENT_ID)
    SELECT EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT
        FROM t
    WHERE TERMINAL_CITY <> PREV_CITY
        AND PREV_TIME > ONE_HOUR_INTERVAL
        AND OP_NUM = 1) RES
ON (
        REP.EVENT_DT = RES.EVENT_DT
)
WHEN NOT MATCHED THEN
    INSERT (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
    VALUES (RES.EVENT_DT, RES.PASSPORT, RES.FIO, RES.PHONE, RES.EVENT_TYPE, RES.REPORT_DT);
