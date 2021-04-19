WITH buy AS
  (select "accountId",
          "dateTime",
          atr AS "first_buy",
          lead(total) over(PARTITION BY "accountId"
                           ORDER BY "dateTime") AS second_buy, "period", "object" 
   FROM
     (SELECT *,
             coalesce(sum(atr) OVER (PARTITION BY "accountId"
                                     ORDER BY "dateTime" ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW), 0) AS total
      FROM
        (SELECT *,
                row_number() over(PARTITION BY "accountId", guid
                                  ORDER BY "dateTime") atr, (details::JSON->>'period')::real AS "period"
         FROM billing b2
         WHERE "object" in ('package','channel')) t
      WHERE atr = 1) t),
     test AS
  (SELECT "accountId",
          "dateTime",
          first_buy,
          second_buy,
          "period",
          "object"
   FROM
     (SELECT "accountId",
             min("dateTime") AS "dateTime",
             min(first_buy) AS first_buy,
             min(second_buy) AS second_buy,
             min("period") AS "period",
             min("object") AS "object"
      FROM buy
      GROUP BY "accountId") t)
      select  a."id" as "accountId", 
	          a."registerAt",
      		  t."dateTime" as "first_transaction_time",
	          t.first_buy,
	          t.second_buy,
	          t."period",
	          t."object"
      from test t
      right JOIN accounts a ON t."accountId"=a."id" WHERE a."type" != 'partner-demo'
      