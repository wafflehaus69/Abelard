-- Does Dune carry labelled Solana exchange wallets? Shape and count only.
SELECT count(*) AS n, count(DISTINCT address) AS addrs, count(DISTINCT cex_name) AS names,
       count(address) AS address_filled, min(length(address)) AS min_len, max(length(address)) AS max_len
FROM cex_solana.addresses
