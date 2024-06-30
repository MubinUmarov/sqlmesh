MODEL (
  name SQLMESH_TEST.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders
FROM
  SQLMESH_TEST.incremental_model
GROUP BY item_id
