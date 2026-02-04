
-- FUNCTION: public.get_effective_price_json(bigint, bigint, bigint)

-- DROP FUNCTION IF EXISTS public.get_effective_price_json(bigint, bigint, bigint);

CREATE OR REPLACE FUNCTION public.get_effective_price_json(
	p_partner_id bigint,
	p_product_id bigint,
	p_packaging_id bigint DEFAULT NULL::bigint)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    STABLE PARALLEL UNSAFE
AS $BODY$
DECLARE
    -- Product base data
    v_product_price      NUMERIC;
    v_product_unit_name  TEXT;
    v_base_unit_name     TEXT;
    v_has_colisage       BOOLEAN := FALSE;
    v_product_exists     BOOLEAN := FALSE;

    -- Product flags
    v_decimal_allowed    BOOLEAN;
    v_decimal_precision  INTEGER;
    v_decimal_step       NUMERIC;

    -- Partner override
    v_override_price     NUMERIC;
    v_override_discount  NUMERIC;
    v_override_priority  INTEGER;
    v_override_unit_name TEXT;
    v_override_found     BOOLEAN := FALSE;

    -- Partner validation
    v_partner_exists     BOOLEAN := FALSE;
    v_partner_has_pricelist BOOLEAN := FALSE;

    -- Price list detail
    v_detail_id          BIGINT;
    v_detail_code        TEXT;
    v_detail_name        TEXT;
    v_detail_price       NUMERIC;
    v_detail_return_price NUMERIC;
    v_detail_discount    NUMERIC;
    v_detail_discount_amt NUMERIC;
    v_detail_min_price   NUMERIC;
    v_detail_max_price   NUMERIC;
    v_detail_unit_name   TEXT;
    v_pricelist_found    BOOLEAN := FALSE;

    -- Packaging pricing (from price list)
    v_pack_sales_price   NUMERIC := NULL;
    v_pack_return_price  NUMERIC := NULL;
    v_pack_min_price     NUMERIC := NULL;
    v_pack_max_price     NUMERIC := NULL;
    v_pack_discount_rate NUMERIC := NULL;
    v_pack_discount_amt  NUMERIC := NULL;
    v_pack_unit_name     TEXT := NULL;
    v_pack_found         BOOLEAN := FALSE;
    v_sel_packaging_id   BIGINT := NULL;
    v_sel_packaging_qty  NUMERIC := NULL;
    v_pack_list          JSONB := '[]'::jsonb;

    -- Fallback packaging (from product_packagings table)
    v_fallback_pack_price NUMERIC := NULL;
    v_fallback_pack_qty   NUMERIC := NULL;
    v_fallback_pack_unit  TEXT := NULL;
    v_fallback_pack_id    BIGINT := NULL;

    -- Computed results
    v_final_price        NUMERIC;
    v_final_raw_price    NUMERIC;
    v_final_min          NUMERIC;
    v_final_max          NUMERIC;
    v_clamped_min        BOOLEAN := FALSE;
    v_clamped_max        BOOLEAN := FALSE;
    v_source             TEXT;
    v_source_priority    INTEGER;
    v_final_discount     NUMERIC;
    v_price_source_chain TEXT[];

BEGIN
    -----------------------------------------------------------------
    -- PHASE 0: Validate required parameters
    -----------------------------------------------------------------
    IF p_product_id IS NULL THEN
        RAISE WARNING '[PRICE_ENGINE] Product ID is required';
        RETURN jsonb_build_object(
            'error', 'Product ID is required',
            'product_id', NULL,
            'source', 'error',
            'source_priority', 999
        );
    END IF;

    -----------------------------------------------------------------
    -- PHASE 1: Load product base data and flags
    -----------------------------------------------------------------
    SELECT 
        pr.price,
        u.name,
        COALESCE(pr.has_colisage, FALSE),
        COALESCE(pf.decimal_quantity_allowed, FALSE),
        COALESCE(pf.decimal_precision, 0),
        COALESCE(pf.decimal_step, 1.0000)
    INTO 
        v_product_price,
        v_product_unit_name,
        v_has_colisage,
        v_decimal_allowed,
        v_decimal_precision,
        v_decimal_step
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    LEFT JOIN product_flags pf ON pf.product_id = pr.id
    WHERE pr.id = p_product_id;

    -- Check if product exists
    IF NOT FOUND THEN
        RAISE WARNING '[PRICE_ENGINE] Product %: not found', p_product_id;
        RETURN jsonb_build_object(
            'error', 'Product not found',
            'product_id', p_product_id,
            'source', 'error',
            'source_priority', 999
        );
    END IF;

    v_product_exists := TRUE;

    -- Check if product has a base price
    IF v_product_price IS NULL THEN
        RAISE WARNING '[PRICE_ENGINE] Product %: missing base price', p_product_id;
        RETURN jsonb_build_object(
            'error', 'Product missing base price',
            'product_id', p_product_id,
            'source', 'error',
            'source_priority', 999
        );
    END IF;

    v_base_unit_name := COALESCE(v_product_unit_name, 'unit');
    v_product_unit_name := COALESCE(v_product_unit_name, v_base_unit_name);

    -- Null-guard for decimal flags
    v_decimal_allowed := COALESCE(v_decimal_allowed, FALSE);
    v_decimal_precision := COALESCE(v_decimal_precision, 0);
    v_decimal_step := COALESCE(v_decimal_step, 1.0000);

    -----------------------------------------------------------------
    -- PHASE 2: Check Partner Override (Priority 1)
    -----------------------------------------------------------------
    IF p_partner_id IS NOT NULL THEN
        SELECT 
            po.fixed_price,
            po.discount_rate,
            po.priority,
            u.name
        INTO 
            v_override_price,
            v_override_discount,
            v_override_priority,
            v_override_unit_name
        FROM partner_price_overrides po
        JOIN products pr ON pr.id = po.product_id
        LEFT JOIN units u ON u.id = pr.unit_id
        WHERE po.partner_id = p_partner_id
          AND po.product_id = p_product_id
          AND po.active = TRUE
          AND (po.valid_from IS NULL OR po.valid_from <= CURRENT_TIMESTAMP)
          AND (po.valid_to IS NULL OR po.valid_to >= CURRENT_TIMESTAMP)
        ORDER BY po.priority DESC, po.valid_from DESC NULLS LAST
        LIMIT 1;

        IF FOUND THEN
            v_override_found := TRUE;
            v_price_source_chain := ARRAY['partner_override', 'end'];
            
            RETURN jsonb_build_object(
                'validated_price', v_override_price,
                'raw_price', v_override_price,
                'discount', COALESCE(v_override_discount, 0),
                'source', 'partner_override',
                'source_priority', 1,
                'price_source_chain', v_price_source_chain,
                'unit', COALESCE(v_override_unit_name, v_base_unit_name),
                'base_unit', v_base_unit_name,
                'is_packaging_enforced', v_has_colisage,
                'selected_packaging_id', NULL,
                'packaging_quantity', NULL,
                'packaging_unit', NULL,
                'detail', jsonb_build_object(
                    'type', 'partner_override',
                    'priority', COALESCE(v_override_priority, 0),
                    'packagings', '[]'::jsonb
                ),
                'validation_flags', jsonb_build_object(
                    'clamped_to_min', FALSE,
                    'clamped_to_max', FALSE,
                    'override_applied', TRUE
                ),
                'decimal_allowed', v_decimal_allowed,
                'decimal_precision', v_decimal_precision,
                'decimal_step', v_decimal_step
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- PHASE 3: Validate Partner & Price List Existence
    -----------------------------------------------------------------
    IF p_partner_id IS NOT NULL THEN
        PERFORM 1 FROM partners 
        WHERE id = p_partner_id AND price_list_id IS NOT NULL;
        
        IF NOT FOUND THEN
            RAISE DEBUG '[PRICE_ENGINE] Partner %: no price list configured, fallback to product base', p_partner_id;
            v_price_source_chain := ARRAY['product_base', 'end'];
            RETURN jsonb_build_object(
                'validated_price', v_product_price,
                'raw_price', v_product_price,
                'discount', 0,
                'source', 'product_base',
                'source_priority', 5,
                'price_source_chain', v_price_source_chain,
                'unit', v_product_unit_name,
                'base_unit', v_base_unit_name,
                'is_packaging_enforced', v_has_colisage,
                'selected_packaging_id', NULL,
                'packaging_quantity', NULL,
                'packaging_unit', NULL,
                'detail', jsonb_build_object(
                    'type', 'product_base',
                    'reason', 'partner_has_no_pricelist',
                    'packagings', '[]'::jsonb
                ),
                'validation_flags', jsonb_build_object(
                    'clamped_to_min', FALSE,
                    'clamped_to_max', FALSE,
                    'packaging_used', FALSE
                ),
                'decimal_allowed', v_decimal_allowed,
                'decimal_precision', v_decimal_precision,
                'decimal_step', v_decimal_step
            );
        END IF;
        
        v_partner_has_pricelist := TRUE;
    END IF;

    -----------------------------------------------------------------
    -- PHASE 4: Check Partner Price List (Priority 2-4)
    -----------------------------------------------------------------
    IF p_partner_id IS NOT NULL AND v_partner_has_pricelist THEN
        SELECT 
            pld.id,
            pl.code,
            pl.name,
            pld.sales_price,
            pld.return_price,
            pld.discount_rate,
            pld.discount_amount,
            pld.min_sales_price,
            pld.max_sales_price,
            u.name
        INTO 
            v_detail_id,
            v_detail_code,
            v_detail_name,
            v_detail_price,
            v_detail_return_price,
            v_detail_discount,
            v_detail_discount_amt,
            v_detail_min_price,
            v_detail_max_price,
            v_detail_unit_name
        FROM partners pa
        JOIN price_lists pl ON pa.price_list_id = pl.id
        JOIN price_list_lines pll ON pll.price_list_id = pl.id
        JOIN price_list_line_details pld 
             ON pld.price_list_id = pl.id AND pld.line_number = pll.line_number
        LEFT JOIN units u ON u.id = pld.unit_id
        WHERE pa.id = p_partner_id
          AND pld.product_id = p_product_id
          AND pll.closed = FALSE
          AND pll.start_date <= CURRENT_TIMESTAMP
          AND pll.end_date >= CURRENT_TIMESTAMP
        ORDER BY pll.line_number DESC, pll.start_date DESC NULLS LAST
        LIMIT 1;

        IF FOUND THEN
            v_pricelist_found := TRUE;

            -----------------------------------------------------------------
            -- PHASE 4A: Build packaging list for reference
            -----------------------------------------------------------------
            SELECT jsonb_agg(js ORDER BY (js->>'is_default')::BOOLEAN DESC, (js->>'quantity')::NUMERIC ASC)
            INTO v_pack_list
            FROM (
                SELECT jsonb_build_object(
                    'packaging_id', pp.id,
                    'sales_price', pplpp.sales_price,
                    'return_price', pplpp.return_price,
                    'min_price', pplpp.min_sales_price,
                    'max_price', pplpp.max_sales_price,
                    'discount_rate', COALESCE(pplpp.discount_rate, 0),
                    'discount_amount', COALESCE(pplpp.discount_amount, 0),
                    'quantity', pp.quantity,
                    'is_default', pp.is_default,
                    'unit', COALESCE(uu.name, v_base_unit_name),
                    'unit_price', CASE 
                        WHEN COALESCE(pp.quantity, 0) > 0 
                        THEN pplpp.sales_price / pp.quantity 
                        ELSE pplpp.sales_price 
                    END
                ) AS js
                FROM price_list_line_packaging_prices pplpp
                JOIN product_packagings pp ON pp.id = pplpp.packaging_id
                LEFT JOIN units uu ON uu.id = pp.unit_id
                WHERE pplpp.line_detail_id = v_detail_id
                ORDER BY pp.is_default DESC, pp.quantity ASC
            ) t;

            -----------------------------------------------------------------
            -- PHASE 4B: Select specific packaging or auto-select
            -----------------------------------------------------------------
            IF p_packaging_id IS NOT NULL THEN
                -- Explicit packaging requested
                SELECT 
                    pplpp.sales_price,
                    pplpp.return_price,
                    pplpp.min_sales_price,
                    pplpp.max_sales_price,
                    pplpp.discount_rate,
                    pplpp.discount_amount,
                    uu.name,
                    pp.id,
                    pp.quantity
                INTO 
                    v_pack_sales_price,
                    v_pack_return_price,
                    v_pack_min_price,
                    v_pack_max_price,
                    v_pack_discount_rate,
                    v_pack_discount_amt,
                    v_pack_unit_name,
                    v_sel_packaging_id,
                    v_sel_packaging_qty
                FROM price_list_line_packaging_prices pplpp
                JOIN product_packagings pp ON pp.id = pplpp.packaging_id
                LEFT JOIN units uu ON uu.id = pp.unit_id
                WHERE pplpp.line_detail_id = v_detail_id
                  AND pplpp.packaging_id = p_packaging_id;

                IF FOUND THEN
                    v_pack_found := TRUE;
                END IF;

            ELSE
                -- Auto-select: prefer default, then lowest price
                SELECT 
                    pplpp.sales_price,
                    pplpp.return_price,
                    pplpp.min_sales_price,
                    pplpp.max_sales_price,
                    pplpp.discount_rate,
                    pplpp.discount_amount,
                    uu.name,
                    pp.id,
                    pp.quantity
                INTO 
                    v_pack_sales_price,
                    v_pack_return_price,
                    v_pack_min_price,
                    v_pack_max_price,
                    v_pack_discount_rate,
                    v_pack_discount_amt,
                    v_pack_unit_name,
                    v_sel_packaging_id,
                    v_sel_packaging_qty
                FROM price_list_line_packaging_prices pplpp
                JOIN product_packagings pp ON pp.id = pplpp.packaging_id
                LEFT JOIN units uu ON uu.id = pp.unit_id
                WHERE pplpp.line_detail_id = v_detail_id
                ORDER BY pp.is_default DESC, pplpp.sales_price ASC
                LIMIT 1;

                IF FOUND THEN
                    v_pack_found := TRUE;
                END IF;
            END IF;

            -----------------------------------------------------------------
            -- PHASE 4C: Fallback to product_packagings if has_colisage and no pricelist packaging
            -----------------------------------------------------------------
            IF v_has_colisage AND NOT v_pack_found THEN
                SELECT 
                    pp.price,
                    pp.quantity,
                    uu.name,
                    pp.id
                INTO 
                    v_fallback_pack_price,
                    v_fallback_pack_qty,
                    v_fallback_pack_unit,
                    v_fallback_pack_id
                FROM product_packagings pp
                LEFT JOIN units uu ON uu.id = pp.unit_id
                WHERE pp.product_id = p_product_id
                ORDER BY pp.is_default DESC, pp.price ASC
                LIMIT 1;

                IF FOUND THEN
                    v_pack_sales_price := v_fallback_pack_price;
                    v_sel_packaging_id := v_fallback_pack_id;
                    v_sel_packaging_qty := v_fallback_pack_qty;
                    v_pack_unit_name := v_fallback_pack_unit;
                    v_pack_found := TRUE;
                END IF;
            END IF;

            -----------------------------------------------------------------
            -- PHASE 4D: Compute final price with proper min/max handling
            -----------------------------------------------------------------
            IF v_pack_found THEN
                -- Use packaging price
                v_final_raw_price := v_pack_sales_price;
                v_final_price := v_pack_sales_price;
                v_final_min := COALESCE(NULLIF(v_pack_min_price, 0), v_pack_sales_price);
                v_final_max := NULLIF(v_pack_max_price, 0);
                v_final_discount := COALESCE(v_pack_discount_rate, 0);
                
                -- Determine source based on fallback status
                IF v_fallback_pack_price IS NOT NULL THEN
                    v_source := 'product_packaging_fallback';
                    v_source_priority := 3;
                ELSE
                    v_source := 'pricelist_packaging';
                    v_source_priority := 2;
                END IF;

            ELSE
                -- Use detail price (list-level pricing)
                v_final_raw_price := v_detail_price;
                v_final_price := v_detail_price;
                v_final_min := COALESCE(NULLIF(v_detail_min_price, 0), v_detail_price);
                v_final_max := NULLIF(v_detail_max_price, 0);
                v_final_discount := COALESCE(v_detail_discount, 0);
                v_source := 'pricelist_detail';
                v_source_priority := 4;
            END IF;

            -- Apply min/max clamping
            IF v_final_min IS NOT NULL AND v_final_price < v_final_min THEN
                v_final_price := v_final_min;
                v_clamped_min := TRUE;
            END IF;

            IF v_final_max IS NOT NULL AND v_final_price > v_final_max THEN
                v_final_price := v_final_max;
                v_clamped_max := TRUE;
            END IF;

            -- Build price source chain
            v_price_source_chain := array_remove(ARRAY[
                CASE WHEN v_override_found THEN 'partner_override' END,
                CASE WHEN v_source = 'product_packaging_fallback' THEN 'product_packaging_fallback' END,
                CASE WHEN v_source = 'pricelist_packaging' THEN 'pricelist_packaging' END,
                CASE WHEN v_source = 'pricelist_detail' THEN 'pricelist_detail' END,
                'product_base', 'end'
            ], NULL);

            RETURN jsonb_build_object(
                'validated_price', v_final_price,
                'raw_price', v_final_raw_price,
                'discount', v_final_discount,
                'source', v_source,
                'source_priority', v_source_priority,
                'price_source_chain', v_price_source_chain,
                'unit', COALESCE(v_pack_unit_name, v_detail_unit_name, v_product_unit_name),
                'base_unit', v_base_unit_name,
                'is_packaging_enforced', v_has_colisage,
                'selected_packaging_id', v_sel_packaging_id,
                'packaging_quantity', v_sel_packaging_qty,
                'packaging_unit', v_pack_unit_name,
                'detail', jsonb_build_object(
                    'code', v_detail_code,
                    'name', v_detail_name,
                    'pricelist_min', v_detail_min_price,
                    'pricelist_max', v_detail_max_price,
                    'packagings', COALESCE(v_pack_list, '[]'::jsonb)
                ),
                'validation_flags', jsonb_build_object(
                    'clamped_to_min', v_clamped_min,
                    'clamped_to_max', v_clamped_max,
                    'packaging_used', v_pack_found,
                    'fallback_packaging_used', (v_has_colisage AND v_fallback_pack_price IS NOT NULL)
                ),
                'decimal_allowed', v_decimal_allowed,
                'decimal_precision', v_decimal_precision,
                'decimal_step', v_decimal_step
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- PHASE 5: Fallback to Product Base Price (Priority 5)
    -- This is reached when:
    -- 1. p_partner_id is NULL (no partner specified)
    -- 2. Partner has no price list
    -- 3. Partner price list has no entry for this product
    -----------------------------------------------------------------
    v_price_source_chain := ARRAY['product_base', 'end'];
    
    RETURN jsonb_build_object(
        'validated_price', v_product_price,
        'raw_price', v_product_price,
        'discount', 0,
        'source', 'product_base',
        'source_priority', 5,
        'price_source_chain', v_price_source_chain,
        'unit', v_product_unit_name,
        'base_unit', v_base_unit_name,
        'is_packaging_enforced', v_has_colisage,
        'selected_packaging_id', NULL,
        'packaging_quantity', NULL,
        'packaging_unit', NULL,
        'detail', jsonb_build_object(
            'type', 'product_base',
            'reason', CASE 
                WHEN p_partner_id IS NULL THEN 'no_partner_specified'
                ELSE 'no_pricelist_entry'
            END,
            'packagings', '[]'::jsonb
        ),
        'validation_flags', jsonb_build_object(
            'clamped_to_min', FALSE,
            'clamped_to_max', FALSE,
            'packaging_used', FALSE,
            'fallback_packaging_used', FALSE
        ),
        'decimal_allowed', v_decimal_allowed,
        'decimal_precision', v_decimal_precision,
        'decimal_step', v_decimal_step
    );

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '[PRICE_ENGINE] (partner=% product=% packaging=%): %',
        p_partner_id, p_product_id, p_packaging_id, SQLERRM;
    
    RETURN jsonb_build_object(
        'error', SQLERRM,
        'product_id', p_product_id,
        'partner_id', p_partner_id,
        'source', 'error',
        'source_priority', 999,
        'validated_price', NULL
    );
END;
$BODY$;

ALTER FUNCTION public.get_effective_price_json(bigint, bigint, bigint)
    OWNER TO postgres;
