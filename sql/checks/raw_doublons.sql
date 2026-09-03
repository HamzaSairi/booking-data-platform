SELECT COUNT(*) AS lignes_brutes,
       COUNT(DISTINCT customer_id) AS cles_distinctes,
       COUNT(DISTINCT _source_file) AS fichiers
FROM `booking-data-platform-b7768d.raw_booking.customers`;

// 12/500/1533


SELECT 'customers' AS t,
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.raw_booking.customers`) AS raw,
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.staging_booking.v_customers`) AS staging
UNION ALL
SELECT 'hotels',
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.raw_booking.hotels`),
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.staging_booking.v_hotels`)
UNION ALL
SELECT 'bookings',
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.raw_booking.bookings`),
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.staging_booking.v_bookings`)
UNION ALL
SELECT 'payments',
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.raw_booking.payments`),
       (SELECT COUNT(*) FROM `booking-data-platform-b7768d.staging_booking.v_payments`);

       // 500 / 50 / 2046 / 1741

