-- FUNCTION: public.get_partner_product_price_at(bigint, bigint, timestamp without time zone)

-- DROP FUNCTION IF EXISTS public.get_partner_product_price_at(bigint, bigint, timestamp without time zone);

CREATE OR REPLACE FUNCTION public.get_partner_product_price_at(
	p_partner_id bigint,
	p_product_id bigint,
	p_as_of timestamp without time zone)
    RETURNS TABLE(partner_id bigint, price_list_id bigint, product_id bigint, base_price numeric, final_price numeric, is_exceptional boolean, price_source text, override_fixed_price numeric, override_discount_rate numeric, override_discount_amount numeric) 
    LANGUAGE 'plpgsql'
    COST 100
    STABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
  RETURN QUERY
  WITH eplv AS (
    SELECT DISTINCT ON (pll.price_list_id)
      pll.price_list_id, pll.line_number
    FROM price_list_lines pll
    WHERE pll.closed = false
      AND p_as_of BETWEEN pll.start_date AND pll.end_date
    ORDER BY pll.price_list_id, pll.line_number DESC
  ),
  base AS (
    SELECT
      eplv.price_list_id,
      d.product_id,
      d.sales_price,
      d.discount_rate,
      d.discount_amount
    FROM eplv
    JOIN price_list_line_details d
      ON d.price_list_id = eplv.price_list_id
     AND d.line_number   = eplv.line_number
    WHERE d.product_id = p_product_id
  ),
  ov AS (
    SELECT DISTINCT ON (o.partner_id, o.product_id)
      o.partner_id, o.product_id, o.fixed_price, o.discount_rate, o.discount_amount
    FROM partner_price_overrides o
    WHERE o.active = true
      AND p_as_of BETWEEN o.valid_from AND o.valid_to
      AND o.partner_id = p_partner_id
      AND o.product_id = p_product_id
    ORDER BY o.partner_id, o.product_id, o.priority DESC, o.valid_from DESC, o.id DESC
  ),
  px AS (
    SELECT
      p.id as partner_id,
      p.price_list_id,
      b.product_id,
      b.sales_price as base_price,
      CASE
        WHEN ov.partner_id IS NOT NULL THEN
          COALESCE(
            ov.fixed_price,
            CASE
              WHEN ov.discount_rate > 0 THEN b.sales_price * (1 - ov.discount_rate/100.0)
              WHEN ov.discount_amount > 0 THEN GREATEST(b.sales_price - ov.discount_amount, 0)
              ELSE b.sales_price
            END
          )
        ELSE b.sales_price
      END AS final_price,
      (ov.partner_id IS NOT NULL) AS is_exceptional,
      CASE WHEN ov.partner_id IS NOT NULL THEN 'OVERRIDE' ELSE 'PRICELIST' END AS price_source,
      ov.fixed_price AS override_fixed_price,
      ov.discount_rate AS override_discount_rate,
      ov.discount_amount AS override_discount_amount
    FROM partners p
    JOIN base b ON p.price_list_id = b.price_list_id
    LEFT JOIN ov ON ov.partner_id = p.id AND ov.product_id = b.product_id
    WHERE p.id = p_partner_id
  )
  SELECT * FROM px;
END;
$BODY$;

ALTER FUNCTION public.get_partner_product_price_at(bigint, bigint, timestamp without time zone)
    OWNER TO postgres;
