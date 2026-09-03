SELECT COUNT(*) AS lignes_brutes,
       COUNT(DISTINCT customer_id) AS cles_distinctes,
       COUNT(DISTINCT _source_file) AS fichiers
FROM `booking-data-platform-b7768d.raw_booking.customers`;

// 12/500/1533

