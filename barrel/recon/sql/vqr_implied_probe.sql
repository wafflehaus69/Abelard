-- MR-14 order 2 follow-up: can a pool's virtual quote reserve be DERIVED from its own decoded buy events?
-- Fill model (validated 10/10 buys): base_out = B * x / (Q + V + x), reserves pre-swap, x = the smaller quote field.
-- So V = B * x / base_out - Q - x on every buy. Known truth: the 35 pool accounts read over RPC (vqr_check.json).
-- One row per sampled pool, buys on its graduation day. No threshold.
SELECT pool, count(*) AS n_buys,
       max_by(v_i, x) AS v_at_largest_buy, approx_percentile(v_i, 0.5) AS v_median,
       min(v_i) AS v_min, max(v_i) AS v_max
FROM (
  SELECT pool,
         least(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)) AS x,
         CAST(pool_base_token_reserves AS double)
           * least(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)) / CAST(base_amount_out AS double)
           - CAST(pool_quote_token_reserves AS double)
           - least(CAST(quote_amount_in AS double), CAST(user_quote_amount_in AS double)) AS v_i
  FROM pumpdotfun_solana.pump_amm_evt_buyevent
  WHERE evt_block_date IN (DATE '2026-07-06', DATE '2026-07-13', DATE '2026-07-19', DATE '2026-07-20', DATE '2026-07-21', DATE '2026-07-23', DATE '2026-07-25', DATE '2026-07-27', DATE '2026-07-29', DATE '2026-07-31', DATE '2026-08-02', DATE '2026-08-04', DATE '2026-08-06', DATE '2026-08-08', DATE '2026-08-10', DATE '2026-08-12', DATE '2026-08-14', DATE '2026-08-16', DATE '2026-08-18', DATE '2026-08-20', DATE '2026-08-22', DATE '2026-08-24', DATE '2026-08-26', DATE '2026-08-28', DATE '2026-08-30', DATE '2026-09-01', DATE '2026-09-03', DATE '2026-09-05', DATE '2026-09-07', DATE '2026-09-09', DATE '2026-09-11', DATE '2026-09-13', DATE '2026-09-15', DATE '2026-09-17', DATE '2026-09-19')
    AND pool IN ('24uLFBZvLN8GFNeZz4sGQTQPHD5b4nEoDTvJD7b4R3GR', '13JMpGciEgtbbgtqjJ7XMvmKxDVV4qcTUdtjUQ7oPvNA', '13JDt9tq1M2F6hLFrWLAnYUKvbrpXMmRZN9fAcymFZKS', '12GfsxfM7MNxM4pTmH4yXo919UzfVYzsRYUCvU4a8QaZ', '13BnR4LV5Z5enCZeoA3wguTG1afsLrtYfgVxiV4N1k5n', '14QnuB6biQR6CKDDfdd7MepkbeFgpUdJv4FBcac4jF5N', '12ceqGmDVVf5UTqPNosw8c6GgXMMkkFg9XmoPx4Wo5bR', '13FGS2GLTTmksKbyqJtsVxLWdpCbhQomSZU3sAVPAS8Z', '13KPsjh6zCTkt6iVA7e9kqLipsCQTGWdbi9QCYyepMb2', '12V2fbcBmQUqeamaqAm7vLmArvCNpzU9r5vFZ3QcKXdB', '128FEej5gjPhpUXXhvHTTDZM8kQUky3PU9FSAehnhxrf', '14bSLAH7s4ru6ZDBS34F8tC6n8fxKGwzkQcX9Vq8ZwUz', '12VoFmpBdZPr2huCDsAoFi7jZBeAfA6MRbMf8XzPtteS', '134nfzUwZMcEKt2EEaYysUzskpVQfdYWZZBVJGWtZr9s', '128zGxBdPQ8ngAmguVuBqpwqkH2DSRo7C7vCpuqVjxRF', '13rErrxFVxnDVctqKGAuLKLPZSwK3A1gTy3wLHrrJRhF', '123bfLXyJvUD4X9PqJ651BySXYLo3YckvwwJ2iQxYkqN', '12FcEfxp3iTEXi8pQgS7cMZbVe8U3Szuk9dXLLXjbFro', '143e89VSvmN5Ef9gRMqXQXGsamTZpgE4xQQraZUPTrM', '12iFDjXYEgQg6gqsU6aunHYhrzuhHfKpuBzzauWtEGCs', '13REcbvvnURGZCjrnL1Z6HV8i8126ooHjdxUJRgCgm2J', '12c4VMix53aYTh2GyEsgUSHZyME6zovNzZY2FZVkZwuu', '1238RuSA3nidB7GZn16FQb896VFXCchZPWwSmC8vsPxV', '12MLbtqYYDvMSaTZzhpCggKAh6xjCfuabaytAdZRx6fC', '12V2DxFKyMMwrtQzNyEcKou2XdRrpvqdhWfgqgd1y8Gw', '12ECe1qhWGFZfNcG9i1fUz3YBLTA3SaviC45WWNw6Bpe', '12RGmtpZFRJfx9UR1sjW4MQVnFnri5Ae3yQ28HLft9kj', '12sNtzzY3mdxMc1DDzB7BJnkvZRb4fQU2pNsTDdQehUS', '12kaapxghz9SKykaDgkcedZNEdBvvuV4m3mqWrBuTGdi', '12D68sS8qUkcgt8h6P9urw3XePpmeykYLypzFJhKkNhX', '11EsYdciwN5F3dAGZfb8z3M14EiB8U9XAHSrXRqme48', '12tHZJ1Qa8Nr4kXcAA5zaG43j4xxtLW54HYQUd58zwD', '12KgmB7vaAY1Mu236fzEzFxLd1vi1HzZou2NuJnBiC4Z', '12MKfj5EaqMVU18JgzCdR1WFeWG47ke7XP232C7jhd2j', '12z3NtPUix7kSjGfmo7mbVi5cJuawRQbC6KHamB7nWES') AND base_amount_out > 0)
GROUP BY 1
