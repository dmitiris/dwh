MERGE INTO DE3AT.PAKS_REP_FRAUD REP USING (
    SELECT
        TRANS.TRANSACTION_DATE AS EVENT_DT,
        CL.PASSPORT_NUM AS PASSPORT,
        CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
        CL.PHONE,
        'INVALID PASSPORT (1)' AS EVENT_TYPE,
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
    SELECT
        TRANS.TRANSACTION_DATE AS EVENT_DT,
        CL.PASSPORT_NUM AS PASSPORT,
        CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
        CL.PHONE,
        'INVALID AGREEMENT (2)' AS EVENT_TYPE,
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
    WITH t AS (
        SELECT
            TRANS.TRANSACTION_DATE AS EVENT_DT,
            CL.PASSPORT_NUM AS PASSPORT,
            CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
            CL.PHONE,
            'INVALID CITY (3)' AS EVENT_TYPE,
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

MERGE INTO DE3AT.PAKS_REP_FRAUD REP USING (
    WITH PREP AS (
            SELECT TRANSACTION_DATE, AMOUNT, CARD_NUM
                , OPERATION_TYPE AS OT
                , OPERATION_RESULT AS OP
                , LAG(OPERATION_TYPE, 1) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS OT1
                , LAG(OPERATION_RESULT, 1) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS OP1
                , LAG(AMOUNT, 1) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS AM1
                , LAG(OPERATION_TYPE, 2) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS OT2
                , LAG(OPERATION_RESULT, 2) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS OP2
                , LAG(AMOUNT, 2) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS AM2
                , LAG(OPERATION_TYPE, 3) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS OT3
                , LAG(OPERATION_RESULT, 3) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS OP3
                , LAG(AMOUNT, 3) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS AM3
                , LAG(TRANSACTION_DATE, 3) OVER ( PARTITION BY CARD_NUM ORDER BY TRANSACTION_DATE) AS TD_BEGIN
            FROM PAKS_DWH_FACT_TRANSACTIONS
            WHERE 1=1
        ),
        FRAUD AS (
            SELECT TRANSACTION_DATE, CARD_NUM
                FROM prep
                WHERE 1=1
                    AND OT = 'WITHDRAW'
                    AND OT = OT1
                    AND OT = OT2
                    AND OT = OT3
                    AND OP = 'SUCCESS'
                    AND OP1 = 'REJECT'
                    AND OP1 = OP2
                    AND OP1 = OP3
                    AND AMOUNT < AM1
                    AND AM1 < AM2
                    AND AM2 < AM3
                    AND TRANSACTION_DATE - INTERVAL '20' MINUTE < TD_BEGIN
        )
    SELECT TRANSACTION_DATE AS EVENT_DT,
        CL.PASSPORT_NUM AS PASSPORT,
        CL.LAST_NAME || ' ' || CL.FIRST_NAME || ' '|| CL.PATRONYMIC AS FIO,
        CL.PHONE,
        'AMOUNT BRUTE FORCE (4)' AS EVENT_TYPE,
        (SELECT LAST_UPDATE_DT FROM de3at.PAKS_META_DATA WHERE TABLE_NAME='TRANSACTIONS') AS REPORT_DT
    FROM FRAUD
        LEFT JOIN PAKS_DWH_DIM_CARDS_HIST CARDS
            ON FRAUD.TRANSACTION_DATE BETWEEN CARDS.EFFECTIVE_FROM AND CARDS.EFFECTIVE_TO
                AND FRAUD.CARD_NUM = CARDS.CARD_NUM
        LEFT JOIN PAKS_DWH_DIM_ACCOUNTS_HIST ACC
            ON CARDS.EFFECTIVE_FROM BETWEEN ACC.EFFECTIVE_FROM AND ACC.EFFECTIVE_TO
                AND CARDS.ACCOUNT = ACC.ACCOUNT
        LEFT JOIN PAKS_DWH_DIM_CLIENTS_HIST CL
            ON ACC.EFFECTIVE_FROM BETWEEN CL.EFFECTIVE_FROM AND CL.EFFECTIVE_TO
                AND ACC.CLIENT = CL.CLIENT_ID
    ) RES
ON (
        REP.EVENT_DT = RES.EVENT_DT
)
WHEN NOT MATCHED THEN
    INSERT (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
    VALUES (RES.EVENT_DT, RES.PASSPORT, RES.FIO, RES.PHONE, RES.EVENT_TYPE, RES.REPORT_DT);
