-- is very important fo seach 
CREATE EXTENSION IF NOT EXISTS unaccent;

--update  categories  set rank=6 where name = 'Équipements INFORMATIQUE'

select * from users where id =87
select * from customers where user_id=87
select * from partners where customer_id= 35

select * from public.get_partner_product_price_at(2, 170, null)

select * from public.price_list_line_details

select * from public.units

SELECT * FROM get_effective_price_json(1, 5, null);

select * from product_flags

INSERT INTO partners (
    code,
    name,
    customer_id,
    price_list_id,
    created_at,
    updated_at,
    credit_limit,
    credit_used,
    payment_term_days,
    default_payment_method,
    allowed_payment_methods,
    currency,
    default_discount_rate,
    default_discount_amount,
    max_discount_rate,
    tax_number_ice,
    tax_number_if,
    tax_exempt,
    vat_group_code,
    partner_type,
    channel,
    risk_score,
    status,
    blocked_until,
    block_reason,
    phone,
    whatsapp,
    email,
    website,
    address_line1,
    address_line2,
    city,
    region,
    country,
    postal_code,
    geo_lat,
    geo_lng,
    opening_hours,
    delivery_instructions,
    min_order_amount,
    delivery_zone,
    parent_partner_id,
    salesperson_id
) VALUES (
    'P-0001',                          
    'Samhi FoodPlus',                  
    35,                                 
    1,                                 
    NOW(),                             
    NOW(),                             
    100000,                            
    0,                                 
    30,                                
    'CASH',                            
    '["CASH","TRANSFER"]'::jsonb,      
    'MAD',                             
    5,                                 
    0,                                 
    10,                                
    '1234567890',                      
    '987654321',                       
    false,                             
    'A',                               
    'CUSTOMER',                        
    'GROS',                            
    10,                                
    'ACTIVE',                          
    NULL,                              
    NULL,                              
    '+212600000000',                   
    '+212600000000',                   
    'contact@samhi-foodplus.ma',       
    'https://samhi-foodplus.ma',       
    'Zone Industrielle',               
    'Lot 45',                          
    'Casablanca',                      
    'Casablanca-Settat',               
    'MA',                              
    '20000',                           
    33.5731,                           
    -7.5898,                           
    '{"mon-fri":"08:00-18:00"}'::jsonb,
    'Livrer à l''entrepôt central',    
    1000,                              
    'Casablanca Zone',                 
    NULL,                              
    NULL
)
RETURNING id;

select * from price_list_line_details

SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'partners'
ORDER BY ordinal_position;

CREATE OR REPLACE FUNCTION get_effective_price(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS TABLE (
    price          NUMERIC,
    discount       NUMERIC,
    source         TEXT,
    source_detail  TEXT,
    unit           TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
BEGIN
    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name AS unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN QUERY SELECT 
            v_override.fixed_price,
            COALESCE(v_override.discount_rate,0)::NUMERIC,
            'override',
            ('priority=' || v_override.priority)::TEXT,
            v_override.unit_name::TEXT;
        RETURN;
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code          AS pricelist_code,
           pld.sales_price   AS sales_price,
           pld.discount_rate AS discount_rate,
           u.name            AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN QUERY SELECT 
            v_detail.sales_price,
            COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'pricelist',
            ('code=' || v_detail.pricelist_code)::TEXT,
            v_detail.unit_name::TEXT;
        RETURN;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Price
    -----------------------------------------------------------------
    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.price,
               pp.quantity,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id
        LIMIT 1;

        IF FOUND THEN
            RETURN QUERY SELECT 
                v_pack.price,
                0::NUMERIC,
                'packaging',
                ('qty=' || v_pack.quantity)::TEXT,
                v_pack.unit_name::TEXT;
            RETURN;
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN QUERY SELECT 
        v_product.price,
        0::NUMERIC,
        'product',
        'base'::TEXT,
        v_product.name::TEXT;
END;
$$;


CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code          AS pricelist_code,
           pld.sales_price   AS sales_price,
           pld.discount_rate AS discount_rate,
           u.name            AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object('code', v_detail.pricelist_code),
            'unit',     v_detail.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Price
    -----------------------------------------------------------------
    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.price, pp.quantity, u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id
        LIMIT 1;

        IF FOUND THEN
            v_result := jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object('qty', v_pack.quantity),
                'unit',     v_pack.unit_name
            );
            RETURN v_result;
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    v_result := jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.name
    );

    RETURN v_result;
END;
$$;

/**
function calculate price v2
**/
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code          AS pricelist_code,
           pld.sales_price   AS sales_price,
           pld.discount_rate AS discount_rate,
           u.name            AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object('code', v_detail.pricelist_code),
            'unit',     v_detail.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Price (explicit or default)
    -----------------------------------------------------------------
    -- إذا ما جاش packaging_id ندير default
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    -- إلا كان عندنا packaging_id
    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.price, pp.quantity, u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id
        LIMIT 1;

        IF FOUND THEN
            v_result := jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object('qty', v_pack.quantity, 'packaging_id', p_packaging_id),
                'unit',     v_pack.unit_name
            );
            RETURN v_result;
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    v_result := jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name
    );

    RETURN v_result;
END;
$$;

/**
funtion calculate price v3
**/
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code          AS pricelist_code,
           pld.sales_price   AS sales_price,
           pld.discount_rate AS discount_rate,
           u.name            AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object('code', v_detail.pricelist_code),
            'unit',     v_detail.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Price (explicit or default)
    -----------------------------------------------------------------
    -- إلا ما جاش packaging_id، ناخد default
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    -- نجيب تفاصيل الـ packaging
    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id
        LIMIT 1;

        IF FOUND THEN
            v_result := jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'price',        v_pack.price,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name
            );
            RETURN v_result;
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    v_result := jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name
    );

    RETURN v_result;
END;
$$;

/**
function calculate price and get more infos
*/

CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code             AS pricelist_code,
           pld.sales_price      AS sales_price,
           pld.discount_rate    AS discount_rate,
           pld.discount_amount  AS discount_amount,
           pld.min_sales_price  AS min_price,
           pld.max_sales_price  AS max_price,
           u.name               AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        v_result := jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object(
                            'code',            v_detail.pricelist_code,
                            'discount_amount', v_detail.discount_amount,
                            'min_price',       v_detail.min_price,
                            'max_price',       v_detail.max_price
                        ),
            'unit',     v_detail.unit_name
        );
        RETURN v_result;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Price (explicit or default)
    -----------------------------------------------------------------
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id
        LIMIT 1;

        IF FOUND THEN
            v_result := jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'price',        v_pack.price,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name
            );
            RETURN v_result;
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    v_result := jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name
    );

    RETURN v_result;
END;
$$;

/**
function calculate price with details v4
*/
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- 1. Partner Override (أعلى أولوية)
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name
        );
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List (مع packaging support)
    -----------------------------------------------------------------
    SELECT pl0.code             AS pricelist_code,
           pld.sales_price      AS sales_price,
           pld.return_price     AS return_price,
           pld.discount_rate    AS discount_rate,
           pld.discount_amount  AS discount_amount,
           pld.min_sales_price  AS min_price,
           pld.max_sales_price  AS max_price,
           pld.packaging_id     AS packaging_id,
           u.name               AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        -- نجبد infos ديال packaging إلا كان عندنا packaging_id
        IF v_detail.packaging_id IS NOT NULL THEN
            SELECT pp.id, pp.quantity, pp.price AS pack_base_price,
                   pp.unit_id, uu.name AS unit_name
            INTO v_pack
            FROM product_packagings pp
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pp.id = v_detail.packaging_id
              AND pp.product_id = p_product_id;
        END IF;

        RETURN jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object(
                            'code',            v_detail.pricelist_code,
                            'return_price',    v_detail.return_price,
                            'discount_amount', v_detail.discount_amount,
                            'min_price',       v_detail.min_price,
                            'max_price',       v_detail.max_price,
                            'packaging_id',    v_detail.packaging_id,
                            'pack_qty',        v_pack.quantity,
                            'pack_unit_id',    v_pack.unit_id,
                            'pack_unit_name',  v_pack.unit_name
                        ),
            'unit',     COALESCE(v_pack.unit_name, v_detail.unit_name)
        );
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Fallback (product_packagings مباشرة)
    -----------------------------------------------------------------
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id;

        IF FOUND THEN
            RETURN jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product (fallback النهائي)
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name
    );
END;
$$;


/**
trigger change status has colisage
*/

CREATE OR REPLACE FUNCTION sync_has_colisage()
RETURNS TRIGGER AS $$
BEGIN
  IF (TG_OP = 'INSERT') THEN
    UPDATE products SET has_colisage = TRUE WHERE id = NEW.product_id;
  ELSIF (TG_OP = 'DELETE') THEN
    UPDATE products
    SET has_colisage = EXISTS (
      SELECT 1 FROM product_packagings WHERE product_id = OLD.product_id
    )
    WHERE id = OLD.product_id;
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_sync_has_colisage
AFTER INSERT OR DELETE ON product_packagings
FOR EACH ROW EXECUTE FUNCTION sync_has_colisage();


/**
allow 3bar
*/

CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_flags    RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- نجيب flags ديال product (decimal, precision…)
    -----------------------------------------------------------------
    SELECT decimal_quantity_allowed, decimal_precision
    INTO v_flags
    FROM product_flags
    WHERE product_id = p_product_id;

    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name,
            'decimal_allowed',  COALESCE(v_flags.decimal_quantity_allowed, false),
            'decimal_precision',COALESCE(v_flags.decimal_precision, 0)
        );
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code             AS pricelist_code,
           pld.sales_price      AS sales_price,
           pld.return_price     AS return_price,
           pld.discount_rate    AS discount_rate,
           pld.discount_amount  AS discount_amount,
           pld.min_sales_price  AS min_price,
           pld.max_sales_price  AS max_price,
           pld.packaging_id     AS packaging_id,
           u.name               AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        -- packaging info
        IF v_detail.packaging_id IS NOT NULL THEN
            SELECT pp.id, pp.quantity, pp.price AS pack_base_price,
                   pp.unit_id, uu.name AS unit_name
            INTO v_pack
            FROM product_packagings pp
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pp.id = v_detail.packaging_id
              AND pp.product_id = p_product_id;
        END IF;

        RETURN jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object(
                            'code',            v_detail.pricelist_code,
                            'return_price',    v_detail.return_price,
                            'discount_amount', v_detail.discount_amount,
                            'min_price',       v_detail.min_price,
                            'max_price',       v_detail.max_price,
                            'packaging_id',    v_detail.packaging_id,
                            'pack_qty',        v_pack.quantity,
                            'pack_unit_id',    v_pack.unit_id,
                            'pack_unit_name',  v_pack.unit_name
                        ),
            'unit',     COALESCE(v_pack.unit_name, v_detail.unit_name),
            'decimal_allowed',  COALESCE(v_flags.decimal_quantity_allowed, false),
            'decimal_precision',COALESCE(v_flags.decimal_precision, 0)
        );
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Fallback
    -----------------------------------------------------------------
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id;

        IF FOUND THEN
            RETURN jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name,
                'decimal_allowed',  COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision',COALESCE(v_flags.decimal_precision, 0)
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name,
        'decimal_allowed',  COALESCE(v_flags.decimal_quantity_allowed, false),
        'decimal_precision',COALESCE(v_flags.decimal_precision, 0)
    );
END;
$$;

/**
function 3bar v1 
*/

CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_product  RECORD;
    v_flags    RECORD;
    v_result   JSONB;
BEGIN
    -----------------------------------------------------------------
    -- Product flags (decimal settings)
    -----------------------------------------------------------------
    SELECT decimal_quantity_allowed,
           decimal_precision,
           decimal_step
    INTO v_flags
    FROM product_flags
    WHERE product_id = p_product_id;

    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND po.valid_from <= now()
      AND po.valid_to   >= now()
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name,
            'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
            'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
            'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
        );
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List
    -----------------------------------------------------------------
    SELECT pl0.code             AS pricelist_code,
           pld.sales_price      AS sales_price,
           pld.return_price     AS return_price,
           pld.discount_rate    AS discount_rate,
           pld.discount_amount  AS discount_amount,
           pld.min_sales_price  AS min_price,
           pld.max_sales_price  AS max_price,
           pld.packaging_id     AS packaging_id,
           u.name               AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pl ON pl.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pl.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pl.closed = FALSE
      AND pl.start_date <= now()
      AND pl.end_date   >= now()
      AND (p_packaging_id IS NULL OR pld.packaging_id = p_packaging_id)
    ORDER BY pl.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        -- packaging info
        IF v_detail.packaging_id IS NOT NULL THEN
            SELECT pp.id, pp.quantity, pp.price AS pack_base_price,
                   pp.unit_id, uu.name AS unit_name
            INTO v_pack
            FROM product_packagings pp
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pp.id = v_detail.packaging_id
              AND pp.product_id = p_product_id;
        END IF;

        RETURN jsonb_build_object(
            'price',    v_detail.sales_price,
            'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
            'source',   'pricelist',
            'detail',   jsonb_build_object(
                            'code',            v_detail.pricelist_code,
                            'return_price',    v_detail.return_price,
                            'discount_amount', v_detail.discount_amount,
                            'min_price',       v_detail.min_price,
                            'max_price',       v_detail.max_price,
                            'packaging_id',    v_detail.packaging_id,
                            'pack_qty',        v_pack.quantity,
                            'pack_unit_id',    v_pack.unit_id,
                            'pack_unit_name',  v_pack.unit_name
                        ),
            'unit',     COALESCE(v_pack.unit_name, v_detail.unit_name),
            'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
            'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
            'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
        );
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Fallback
    -----------------------------------------------------------------
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id;

        IF FOUND THEN
            RETURN jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name,
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name,
        'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
        'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
        'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
    );
END;
$$;


SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name   = 'price_list_line_packaging_prices'
ORDER BY ordinal_position;


CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_pack_price RECORD;
    v_product  RECORD;
    v_flags    RECORD;
BEGIN
    -----------------------------------------------------------------
    -- Product flags (decimal settings)
    -----------------------------------------------------------------
    SELECT decimal_quantity_allowed,
           decimal_precision,
           decimal_step
    INTO v_flags
    FROM product_flags
    WHERE product_id = p_product_id;

    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND (po.valid_from IS NULL OR po.valid_from <= now())
      AND (po.valid_to IS NULL   OR po.valid_to   >= now())
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN jsonb_build_object(
            'price',    v_override.fixed_price,
            'discount', COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',   'override',
            'detail',   jsonb_build_object('priority', v_override.priority),
            'unit',     v_override.unit_name,
            'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
            'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
            'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
        );
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List → LineDetail + PackagingPrice
    -----------------------------------------------------------------
    SELECT pl0.code             AS pricelist_code,
           pld.id               AS detail_id,
           pld.sales_price      AS base_sales_price,
           pld.return_price     AS return_price,
           pld.discount_rate    AS discount_rate,
           pld.discount_amount  AS discount_amount,
           pld.min_sales_price  AS min_price,
           pld.max_sales_price  AS max_price,
           u.name               AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pll ON pll.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pll.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pll.closed = FALSE
      AND pll.start_date <= now()
      AND pll.end_date   >= now()
    ORDER BY pll.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        -- packaging price override for this line
        IF p_packaging_id IS NOT NULL THEN
            SELECT pplpp.id,
                   pplpp.sales_price,
                   pplpp.return_price,
                   pplpp.min_sales_price,
                   pplpp.max_sales_price,
                   pplpp.discount_amount,
                   pplpp.discount_rate,
                   pp.quantity,
                   uu.name AS unit_name
            INTO v_pack_price
            FROM price_list_line_packaging_prices pplpp
            JOIN product_packagings pp ON pp.id = pplpp.packaging_id
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pplpp.detail_id = v_detail.detail_id
              AND pplpp.packaging_id = p_packaging_id;
        END IF;

        IF FOUND THEN
            RETURN jsonb_build_object(
                'price',    v_pack_price.sales_price,
                'discount', COALESCE(v_pack_price.discount_rate,0)::NUMERIC,
                'source',   'pricelist_packaging',
                'detail',   jsonb_build_object(
                                'code',            v_detail.pricelist_code,
                                'return_price',    v_pack_price.return_price,
                                'discount_amount', v_pack_price.discount_amount,
                                'min_price',       v_pack_price.min_sales_price,
                                'max_price',       v_pack_price.max_sales_price,
                                'packaging_id',    p_packaging_id,
                                'pack_qty',        v_pack_price.quantity
                            ),
                'unit',     v_pack_price.unit_name,
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        ELSE
            -- return base unit price list detail
            RETURN jsonb_build_object(
                'price',    v_detail.base_sales_price,
                'discount', COALESCE(v_detail.discount_rate,0)::NUMERIC,
                'source',   'pricelist_detail',
                'detail',   jsonb_build_object(
                                'code',            v_detail.pricelist_code,
                                'return_price',    v_detail.return_price,
                                'discount_amount', v_detail.discount_amount,
                                'min_price',       v_detail.min_price,
                                'max_price',       v_detail.max_price
                            ),
                'unit',     v_detail.unit_name,
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Fallback (ProductPackaging direct price)
    -----------------------------------------------------------------
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id;

        IF FOUND THEN
            RETURN jsonb_build_object(
                'price',    v_pack.price,
                'discount', 0::NUMERIC,
                'source',   'packaging',
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name,
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN jsonb_build_object(
        'price',    v_product.price,
        'discount', 0::NUMERIC,
        'source',   'product',
        'detail',   jsonb_build_object('base', true),
        'unit',     v_product.unit_name,
        'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
        'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
        'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
    );
END;
$$;

/**
more advance pricing managment:
validated_price : Prix final après validation.
raw_price : Prix initial avant toute modification.
validation_flags : Indique si le prix a changé.
source_priority : Classement de la source (1 – 5 – plus fort).
*/

CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_override RECORD;
    v_detail   RECORD;
    v_pack     RECORD;
    v_pack_price RECORD;
    v_product  RECORD;
    v_flags    RECORD;

    v_final_price NUMERIC;
    v_final_min NUMERIC;
    v_final_max NUMERIC;

    v_clamped_min BOOLEAN := false;
    v_clamped_max BOOLEAN := false;
BEGIN
    -----------------------------------------------------------------
    -- Product flags (decimal settings)
    -----------------------------------------------------------------
    SELECT decimal_quantity_allowed,
           decimal_precision,
           decimal_step
    INTO v_flags
    FROM product_flags
    WHERE product_id = p_product_id;

    -----------------------------------------------------------------
    -- 1. Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price,
           po.discount_rate,
           po.priority,
           u.name as unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND (po.valid_from IS NULL OR po.valid_from <= now())
      AND (po.valid_to IS NULL   OR po.valid_to   >= now())
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        v_final_price := v_override.fixed_price;

        RETURN jsonb_build_object(
            'validated_price', v_final_price,
            'raw_price',       v_override.fixed_price,
            'discount',        COALESCE(v_override.discount_rate,0)::NUMERIC,
            'source',          'override',
            'source_priority', 1,
            'detail',          jsonb_build_object('priority', v_override.priority),
            'unit',            v_override.unit_name,
            'validation_flags', jsonb_build_object(
                                    'clamped_to_min', false,
                                    'clamped_to_max', false
                                ),
            'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
            'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
            'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
        );
    END IF;

    -----------------------------------------------------------------
    -- 2. Partner Price List → LineDetail + PackagingPrice
    -----------------------------------------------------------------
    SELECT pl0.code             AS pricelist_code,
           pld.id               AS detail_id,
           pld.sales_price      AS base_sales_price,
           pld.return_price     AS return_price,
           pld.discount_rate    AS discount_rate,
           pld.discount_amount  AS discount_amount,
           pld.min_sales_price  AS min_price,
           pld.max_sales_price  AS max_price,
           u.name               AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0 ON pa.price_list_id = pl0.id
    JOIN price_list_lines pll ON pll.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pll.line_number
    LEFT JOIN units u ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pll.closed = FALSE
      AND pll.start_date <= now()
      AND pll.end_date   >= now()
    ORDER BY pll.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        -- packaging price override for this line
        IF p_packaging_id IS NOT NULL THEN
            SELECT pplpp.id,
                   pplpp.sales_price,
                   pplpp.return_price,
                   pplpp.min_sales_price,
                   pplpp.max_sales_price,
                   pplpp.discount_amount,
                   pplpp.discount_rate,
                   pp.quantity,
                   uu.name AS unit_name
            INTO v_pack_price
            FROM price_list_line_packaging_prices pplpp
            JOIN product_packagings pp ON pp.id = pplpp.packaging_id
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pplpp.detail_id = v_detail.detail_id
              AND pplpp.packaging_id = p_packaging_id;
        END IF;

        IF FOUND THEN
            v_final_price := v_pack_price.sales_price;
            v_final_min   := COALESCE(v_pack_price.min_sales_price, v_final_price);
            v_final_max   := COALESCE(v_pack_price.max_sales_price, v_final_price);

            IF v_final_price < v_final_min THEN v_final_price := v_final_min; v_clamped_min := true; END IF;
            IF v_final_price > v_final_max THEN v_final_price := v_final_max; v_clamped_max := true; END IF;

            RETURN jsonb_build_object(
                'validated_price', v_final_price,
                'raw_price',       v_pack_price.sales_price,
                'discount',        COALESCE(v_pack_price.discount_rate,0)::NUMERIC,
                'source',          'pricelist_packaging',
                'source_priority', 2,
                'detail',   jsonb_build_object(
                                'code',            v_detail.pricelist_code,
                                'return_price',    v_pack_price.return_price,
                                'discount_amount', v_pack_price.discount_amount,
                                'min_price',       v_final_min,
                                'max_price',       v_final_max,
                                'packaging_id',    p_packaging_id,
                                'pack_qty',        v_pack_price.quantity
                            ),
                'unit',     v_pack_price.unit_name,
                'validation_flags', jsonb_build_object(
                                        'clamped_to_min', v_clamped_min,
                                        'clamped_to_max', v_clamped_max
                                    ),
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        ELSE
            v_final_price := v_detail.base_sales_price;
            v_final_min   := COALESCE(v_detail.min_price, v_final_price);
            v_final_max   := COALESCE(v_detail.max_price, v_final_price);

            IF v_final_price < v_final_min THEN v_final_price := v_final_min; v_clamped_min := true; END IF;
            IF v_final_price > v_final_max THEN v_final_price := v_final_max; v_clamped_max := true; END IF;

            RETURN jsonb_build_object(
                'validated_price', v_final_price,
                'raw_price',       v_detail.base_sales_price,
                'discount',        COALESCE(v_detail.discount_rate,0)::NUMERIC,
                'source',          'pricelist_detail',
                'source_priority', 3,
                'detail',   jsonb_build_object(
                                'code',            v_detail.pricelist_code,
                                'return_price',    v_detail.return_price,
                                'discount_amount', v_detail.discount_amount,
                                'min_price',       v_final_min,
                                'max_price',       v_final_max
                            ),
                'unit',     v_detail.unit_name,
                'validation_flags', jsonb_build_object(
                                        'clamped_to_min', v_clamped_min,
                                        'clamped_to_max', v_clamped_max
                                    ),
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 3. Packaging Fallback (ProductPackaging direct price)
    -----------------------------------------------------------------
    IF p_packaging_id IS NULL THEN
        SELECT pp.id
        INTO p_packaging_id
        FROM product_packagings pp
        WHERE pp.product_id = p_product_id
          AND pp.is_default = TRUE
        LIMIT 1;
    END IF;

    IF p_packaging_id IS NOT NULL THEN
        SELECT pp.id AS packaging_id,
               pp.price,
               pp.quantity,
               pp.unit_id,
               u.name AS unit_name
        INTO v_pack
        FROM product_packagings pp
        LEFT JOIN units u ON u.id = pp.unit_id
        WHERE pp.id = p_packaging_id
          AND pp.product_id = p_product_id;

        IF FOUND THEN
            v_final_price := v_pack.price;

            RETURN jsonb_build_object(
                'validated_price', v_final_price,
                'raw_price',       v_pack.price,
                'discount',        0::NUMERIC,
                'source',          'packaging',
                'source_priority', 4,
                'detail',   jsonb_build_object(
                                'packaging_id', v_pack.packaging_id,
                                'qty',          v_pack.quantity,
                                'unit_id',      v_pack.unit_id,
                                'unit_name',    v_pack.unit_name
                            ),
                'unit',     v_pack.unit_name,
                'validation_flags', jsonb_build_object(
                                        'clamped_to_min', false,
                                        'clamped_to_max', false
                                    ),
                'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
                'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
                'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- 4. Base Product
    -----------------------------------------------------------------
    SELECT pr.price, u.name as unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN jsonb_build_object(
        'validated_price', v_product.price,
        'raw_price',       v_product.price,
        'discount',        0::NUMERIC,
        'source',          'product',
        'source_priority', 5,
        'detail',          jsonb_build_object('base', true),
        'unit',            v_product.unit_name,
        'validation_flags', jsonb_build_object(
                                'clamped_to_min', false,
                                'clamped_to_max', false
                            ),
        'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, false),
        'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
        'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
    );
END;
$$;


/**
v10
*/

CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- partner override
    v_override RECORD;

    -- price list detail
    v_detail RECORD;

    -- product base + flags
    v_product RECORD;
    v_flags   RECORD;

    -- product base unit + b2b colisage enforcement
    v_base_unit_name TEXT;
    v_has_colisage   BOOLEAN := FALSE;

    -- packaging (chosen or auto)
    v_pack_sales_price   NUMERIC := NULL;
    v_pack_min_price     NUMERIC := NULL;
    v_pack_max_price     NUMERIC := NULL;
    v_pack_discount_rate NUMERIC := NULL;
    v_pack_unit_name     TEXT    := NULL;
    v_pack_found         BOOLEAN := FALSE;

    v_sel_packaging_id   BIGINT  := NULL;
    v_sel_packaging_qty  NUMERIC := NULL;

    v_pack_list JSONB := '[]'::jsonb;

    -- computed
    v_final_price NUMERIC;
    v_final_min   NUMERIC;
    v_final_max   NUMERIC;
    v_clamped_min BOOLEAN := FALSE;
    v_clamped_max BOOLEAN := FALSE;
BEGIN
    -----------------------------------------------------------------
    -- 0) base unit + has_colisage + flags
    -----------------------------------------------------------------
    SELECT u.name, COALESCE(pr.has_colisage, FALSE)
    INTO   v_base_unit_name, v_has_colisage
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    SELECT decimal_quantity_allowed, decimal_precision, decimal_step
    INTO v_flags
    FROM product_flags
    WHERE product_id = p_product_id;

    -----------------------------------------------------------------
    -- 1) Partner Override
    -----------------------------------------------------------------
    SELECT po.fixed_price, po.discount_rate, po.priority, u.name AS unit_name
    INTO v_override
    FROM partner_price_overrides po
    JOIN products pr ON pr.id = po.product_id
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE po.partner_id = p_partner_id
      AND po.product_id = p_product_id
      AND po.active = TRUE
      AND (po.valid_from IS NULL OR po.valid_from <= now())
      AND (po.valid_to   IS NULL OR po.valid_to   >= now())
    ORDER BY po.priority DESC, po.valid_from DESC
    LIMIT 1;

    IF FOUND THEN
        RETURN jsonb_build_object(
            'validated_price', v_override.fixed_price,
            'raw_price',       v_override.fixed_price,
            'discount',        COALESCE(v_override.discount_rate,0),
            'source',          'override',
            'source_priority', 1,
            'unit',            v_override.unit_name,
            'base_unit',       v_base_unit_name,
            'is_packaging_enforced', v_has_colisage,
            'detail',          jsonb_build_object('priority', v_override.priority, 'packagings', '[]'::jsonb),
            'validation_flags', jsonb_build_object('clamped_to_min', FALSE, 'clamped_to_max', FALSE),
            'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, FALSE),
            'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
            'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
        );
    END IF;

    -----------------------------------------------------------------
    -- 2) Partner Price List (+ packaging)
    -----------------------------------------------------------------
    SELECT pl0.code AS pricelist_code,
           pl0.name AS pricelist_name,
           pld.id   AS detail_id,
           pld.sales_price,
           pld.return_price,
           pld.discount_rate,
           pld.discount_amount,
           pld.min_sales_price,
           pld.max_sales_price,
           u.name   AS unit_name
    INTO v_detail
    FROM partners pa
    JOIN price_lists pl0          ON pa.price_list_id = pl0.id
    JOIN price_list_lines pll     ON pll.price_list_id = pl0.id
    JOIN price_list_line_details pld 
         ON pld.price_list_id = pl0.id AND pld.line_number = pll.line_number
    LEFT JOIN units u             ON u.id = pld.unit_id
    WHERE pa.id = p_partner_id
      AND pld.product_id = p_product_id
      AND pll.closed = FALSE
      AND pll.start_date <= now()
      AND pll.end_date   >= now()
    ORDER BY pll.line_number DESC
    LIMIT 1;

    IF FOUND THEN
        -- 2.a) list packagings
        SELECT jsonb_agg(
            jsonb_build_object(
                'packaging_id',    pp.id,
                'sales_price',     pplpp.sales_price,
                'return_price',    pplpp.return_price,
                'min_price',       pplpp.min_sales_price,
                'max_price',       pplpp.max_sales_price,
                'discount_rate',   COALESCE(pplpp.discount_rate,0),
                'discount_amount', COALESCE(pplpp.discount_amount,0),
                'quantity',        pp.quantity,
                'is_default',      pp.is_default,
                'unit',            uu.name,
                'base_unit_name',  v_base_unit_name,
                'unit_price',      CASE WHEN COALESCE(pp.quantity,0) > 0
                                         THEN pplpp.sales_price/pp.quantity
                                         ELSE pplpp.sales_price END
            )
        )
        INTO v_pack_list
        FROM price_list_line_packaging_prices pplpp
        JOIN product_packagings pp ON pp.id = pplpp.packaging_id
        LEFT JOIN units uu         ON uu.id = pp.unit_id
        WHERE pplpp.line_detail_id = v_detail.detail_id;

        -----------------------------------------------------------------
        -- 2.b) choose packaging (explicit or auto)
        -----------------------------------------------------------------
        IF p_packaging_id IS NOT NULL THEN
            SELECT pplpp.sales_price,
                   pplpp.min_sales_price,
                   pplpp.max_sales_price,
                   pplpp.discount_rate,
                   uu.name AS unit_name,
                   pp.id   AS sel_pack_id,
                   pp.quantity AS sel_pack_qty
            INTO  v_pack_sales_price,
                  v_pack_min_price,
                  v_pack_max_price,
                  v_pack_discount_rate,
                  v_pack_unit_name,
                  v_sel_packaging_id,
                  v_sel_packaging_qty
            FROM price_list_line_packaging_prices pplpp
            JOIN product_packagings pp ON pp.id = pplpp.packaging_id
            LEFT JOIN units uu         ON uu.id = pp.unit_id
            WHERE pplpp.line_detail_id = v_detail.detail_id
              AND pplpp.packaging_id  = p_packaging_id;

            IF FOUND THEN v_pack_found := TRUE; END IF;

        ELSE
            SELECT pplpp.sales_price,
                   pplpp.min_sales_price,
                   pplpp.max_sales_price,
                   pplpp.discount_rate,
                   uu.name AS unit_name,
                   pp.id   AS sel_pack_id,
                   pp.quantity AS sel_pack_qty
            INTO  v_pack_sales_price,
                  v_pack_min_price,
                  v_pack_max_price,
                  v_pack_discount_rate,
                  v_pack_unit_name,
                  v_sel_packaging_id,
                  v_sel_packaging_qty
            FROM price_list_line_packaging_prices pplpp
            JOIN product_packagings pp ON pp.id = pplpp.packaging_id
            LEFT JOIN units uu         ON uu.id = pp.unit_id
            WHERE pplpp.line_detail_id = v_detail.detail_id
            ORDER BY pp.is_default DESC, pplpp.sales_price ASC
            LIMIT 1;

            IF FOUND THEN v_pack_found := TRUE; END IF;
        END IF;

        -----------------------------------------------------------------
        -- 🆕 2.c) fallback packaging price from product_packagings if no price_list entries
        -----------------------------------------------------------------
        IF v_has_colisage AND NOT v_pack_found THEN
            SELECT pp.price, pp.quantity, uu.name, pp.id
            INTO v_pack_sales_price, v_sel_packaging_qty, v_pack_unit_name, v_sel_packaging_id
            FROM product_packagings pp
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pp.product_id = p_product_id
            ORDER BY pp.is_default DESC, pp.price ASC
            LIMIT 1;

            IF FOUND THEN
                v_pack_found := TRUE;
            END IF;
        END IF;

        -----------------------------------------------------------------
        -- 2.d) compute (0 → NULL guards)
        -----------------------------------------------------------------
        IF v_pack_found THEN
            v_final_price := v_pack_sales_price;
            v_final_min   := COALESCE(v_pack_sales_price, v_final_price);
            v_final_max   := NULL;
        ELSE
            v_final_price := COALESCE(v_detail.sales_price, 0);
            v_final_min   := COALESCE(NULLIF(v_detail.min_sales_price,0), v_final_price);
            v_final_max   := NULLIF(v_detail.max_sales_price,0);
        END IF;

        IF v_final_price < v_final_min THEN 
            v_final_price := v_final_min; 
            v_clamped_min := TRUE; 
        END IF;

        RETURN jsonb_build_object(
            'validated_price', v_final_price,
            'raw_price',       v_final_price,
            'discount',        (CASE WHEN v_pack_found THEN COALESCE(v_pack_discount_rate,0)
                                     ELSE COALESCE(v_detail.discount_rate,0) END),
            'source',          (CASE WHEN v_pack_found THEN 'packaging_fallback'
                                     ELSE 'pricelist_detail' END),
            'source_priority', 2,
            'unit',            COALESCE(v_pack_unit_name, v_detail.unit_name),
            'base_unit',       v_base_unit_name,
            'is_packaging_enforced', v_has_colisage,
            'selected_packaging_id', v_sel_packaging_id,
            'packaging_quantity',    v_sel_packaging_qty,
            'packaging_unit',        v_pack_unit_name,
            'detail', jsonb_build_object(
                'code',       v_detail.pricelist_code,
                'name',       v_detail.pricelist_name,
                'packagings', COALESCE(v_pack_list, '[]'::jsonb)
            ),
            'validation_flags', jsonb_build_object(
                'clamped_to_min', v_clamped_min,
                'clamped_to_max', v_clamped_max
            ),
            'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, FALSE),
            'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
            'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
        );
    END IF;

    -----------------------------------------------------------------
    -- 3) fallback: product
    -----------------------------------------------------------------
    SELECT pr.price, u.name AS unit_name
    INTO v_product
    FROM products pr
    LEFT JOIN units u ON u.id = pr.unit_id
    WHERE pr.id = p_product_id;

    RETURN jsonb_build_object(
        'validated_price', v_product.price,
        'raw_price',       v_product.price,
        'discount',        0,
        'source',          'product',
        'source_priority', 3,
        'unit',            v_product.unit_name,
        'base_unit',       v_base_unit_name,
        'is_packaging_enforced', v_has_colisage,
        'selected_packaging_id', NULL,
        'packaging_quantity',    NULL,
        'packaging_unit',        NULL,
        'detail', jsonb_build_object('base', TRUE, 'packagings', '[]'::jsonb),
        'validation_flags', jsonb_build_object('clamped_to_min', FALSE, 'clamped_to_max', FALSE),
        'decimal_allowed',   COALESCE(v_flags.decimal_quantity_allowed, FALSE),
        'decimal_precision', COALESCE(v_flags.decimal_precision, 0),
        'decimal_step',      COALESCE(v_flags.decimal_step, 1.0000)
    );
END;
$$;

/**
v11
*/
/**
 * get_effective_price_json - Advanced price resolution with colisage (packaging) support
 * 
 * PRICE RESOLUTION HIERARCHY:
 * 1. Partner Override (fixed price, highest priority)
 * 2. Partner Price List → Packaging Prices (if has_colisage=TRUE or packaging-specific pricing)
 * 3. Partner Price List → Detail Price (fallback from list)
 * 4. Product Base Price (lowest priority)
 * 
 * COLISAGE ENFORCEMENT:
 * - If product.has_colisage=TRUE, packaging prices are preferred/required
 * - Auto-selects default packaging if not explicitly requested
 * - Falls back to product_packagings table if price_list has no packaging entries
 * 
 * PARAMETERS:
 *   p_partner_id   - Partner identifier (NULL allowed, uses product base price)
 *   p_product_id   - Product identifier (required)
 *   p_packaging_id - Specific packaging to use (optional, auto-select if NULL and has_colisage=TRUE)
 * 
 * RETURNS JSONB with structure:
 *   {
 *     validated_price: NUMERIC,           -- final price (after min/max clamping)
 *     raw_price: NUMERIC,                 -- price before clamping
 *     discount: NUMERIC,                  -- discount rate %
 *     source: TEXT,                       -- 'override'|'packaging'|'pricelist'|'product'
 *     source_priority: INTEGER,           -- 1-4 (1=highest)
 *     unit: TEXT,                         -- unit of this price
 *     base_unit: TEXT,                    -- product base unit
 *     is_packaging_enforced: BOOLEAN,     -- product.has_colisage
 *     selected_packaging_id: BIGINT,      -- packaging used (if applicable)
 *     packaging_quantity: NUMERIC,        -- qty per packaging unit
 *     packaging_unit: TEXT,               -- unit for this packaging
 *     detail: JSONB,                      -- source metadata
 *     validation_flags: JSONB,            -- clamping info
 *     decimal_allowed: BOOLEAN,           -- allows fractional qty
 *     decimal_precision: INTEGER,         -- decimal places
 *     decimal_step: NUMERIC               -- minimum increment
 *   }
 */
 
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
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

BEGIN
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

    v_base_unit_name := COALESCE(v_product_unit_name, 'unit');
    v_product_unit_name := COALESCE(v_product_unit_name, v_base_unit_name);

    -- Verify product exists and has a price
    IF v_product_price IS NULL THEN
        RAISE WARNING 'Product % not found or has no base price', p_product_id;
        RETURN jsonb_build_object(
            'error', 'Product not found or missing price',
            'product_id', p_product_id,
            'source', 'error'
        );
    END IF;

    v_product_exists := TRUE;

    -----------------------------------------------------------------
    -- PHASE 2: Check Partner Override (Highest Priority)
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
            RETURN jsonb_build_object(
                'validated_price', v_override_price,
                'raw_price', v_override_price,
                'discount', COALESCE(v_override_discount, 0),
                'source', 'partner_override',
                'source_priority', 1,
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
    -- PHASE 3: Check Partner Price List
    -----------------------------------------------------------------
    IF p_partner_id IS NOT NULL THEN
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
            -- PHASE 3A: Build packaging list for reference
            -----------------------------------------------------------------
            SELECT jsonb_agg(
                jsonb_build_object(
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
                )
                ORDER BY pp.is_default DESC, pp.quantity ASC
            )
            INTO v_pack_list
            FROM price_list_line_packaging_prices pplpp
            JOIN product_packagings pp ON pp.id = pplpp.packaging_id
            LEFT JOIN units uu ON uu.id = pp.unit_id
            WHERE pplpp.line_detail_id = v_detail_id;

            -----------------------------------------------------------------
            -- PHASE 3B: Select specific packaging or auto-select
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
            -- PHASE 3C: Fallback to product_packagings if has_colisage and no price list packaging
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
            -- PHASE 3D: Compute final price with proper min/max handling
            -----------------------------------------------------------------
            IF v_pack_found THEN
                -- Use packaging price
                v_final_raw_price := v_pack_sales_price;
                v_final_price := v_pack_sales_price;
                v_final_min := COALESCE(NULLIF(v_pack_min_price, 0), v_pack_sales_price);
                v_final_max := NULLIF(v_pack_max_price, 0);
                v_final_discount := COALESCE(v_pack_discount_rate, 0);
                v_source := 'packaging_price';
                v_source_priority := 2;

            ELSE
                -- Use detail price (list-level pricing)
                v_final_raw_price := v_detail_price;
                v_final_price := v_detail_price;
                v_final_min := COALESCE(NULLIF(v_detail_min_price, 0), v_detail_price);
                v_final_max := NULLIF(v_detail_max_price, 0);
                v_final_discount := COALESCE(v_detail_discount, 0);
                v_source := 'pricelist_detail';
                v_source_priority := 3;
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

            RETURN jsonb_build_object(
                'validated_price', v_final_price,
                'raw_price', v_final_raw_price,
                'discount', v_final_discount,
                'source', v_source,
                'source_priority', v_source_priority,
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
                    'fallback_packaging_used', (v_pack_found AND v_has_colisage AND v_fallback_pack_price IS NOT NULL)
                ),
                'decimal_allowed', v_decimal_allowed,
                'decimal_precision', v_decimal_precision,
                'decimal_step', v_decimal_step
            );
        END IF;
    END IF;

    -----------------------------------------------------------------
    -- PHASE 4: Fallback to Product Base Price
    -----------------------------------------------------------------
    RETURN jsonb_build_object(
        'validated_price', v_product_price,
        'raw_price', v_product_price,
        'discount', 0,
        'source', 'product_base',
        'source_priority', 4,
        'unit', v_product_unit_name,
        'base_unit', v_base_unit_name,
        'is_packaging_enforced', v_has_colisage,
        'selected_packaging_id', NULL,
        'packaging_quantity', NULL,
        'packaging_unit', NULL,
        'detail', jsonb_build_object(
            'type', 'product_base',
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

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error in get_effective_price_json(partner=%,product=%,packaging=%): %',
        p_partner_id, p_product_id, p_packaging_id, SQLERRM;
    
    RETURN jsonb_build_object(
        'error', SQLERRM,
        'product_id', p_product_id,
        'partner_id', p_partner_id,
        'source', 'error',
        'validated_price', NULL
    );
END;
$$;

/**
v15
*/
/**
 * get_effective_price_json - Advanced price resolution with colisage (packaging) support
 * 
 * PRICE RESOLUTION HIERARCHY:
 * 1. Partner Override (fixed price, highest priority)
 * 2. Partner Price List → Packaging Prices (if has_colisage=TRUE or packaging-specific pricing)
 * 3. Partner Price List → Detail Price (fallback from list)
 * 4. Product Packaging Fallback (from product_packagings if no pricelist packaging)
 * 5. Product Base Price (lowest priority)
 * 
 * COLISAGE ENFORCEMENT:
 * - If product.has_colisage=TRUE, packaging prices are preferred/required
 * - Auto-selects default packaging if not explicitly requested
 * - Falls back to product_packagings table if price_list has no packaging entries
 * 
 * PARAMETERS:
 *   p_partner_id   - Partner identifier (NULL allowed, uses product base price)
 *   p_product_id   - Product identifier (required)
 *   p_packaging_id - Specific packaging to use (optional, auto-select if NULL and has_colisage=TRUE)
 * 
 * RETURNS JSONB with structure:
 *   {
 *     validated_price: NUMERIC,              -- final price (after min/max clamping)
 *     raw_price: NUMERIC,                    -- price before clamping
 *     discount: NUMERIC,                     -- discount rate %
 *     source: TEXT,                          -- pricing source used
 *     source_priority: INTEGER,              -- 1-5 (1=highest, 5=lowest)
 *     price_source_chain: TEXT[],            -- ordered array of attempted sources
 *     unit: TEXT,                            -- unit of this price
 *     base_unit: TEXT,                       -- product base unit
 *     is_packaging_enforced: BOOLEAN,        -- product.has_colisage
 *     selected_packaging_id: BIGINT,         -- packaging used (if applicable)
 *     packaging_quantity: NUMERIC,           -- qty per packaging unit
 *     packaging_unit: TEXT,                  -- unit for this packaging
 *     detail: JSONB,                         -- source metadata
 *     validation_flags: JSONB,               -- clamping info
 *     decimal_allowed: BOOLEAN,              -- allows fractional qty
 *     decimal_precision: INTEGER,            -- decimal places
 *     decimal_step: NUMERIC                  -- minimum increment
 *   }
 */
 
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
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

    v_base_unit_name := COALESCE(v_product_unit_name, 'unit');
    v_product_unit_name := COALESCE(v_product_unit_name, v_base_unit_name);

    -- Verify product exists and has a price
    IF v_product_price IS NULL THEN
        RAISE WARNING '[PRICE_ERROR] Product % not found or missing base price', p_product_id;
        RETURN jsonb_build_object(
            'error', 'Product not found or missing price',
            'product_id', p_product_id,
            'source', 'error',
            'source_priority', 999
        );
    END IF;

    v_product_exists := TRUE;

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
            v_price_source_chain := ARRAY['partner_override'];
            
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
            RAISE DEBUG '[PRICE_DEBUG] Partner % has no price list, falling back to product base', p_partner_id;
            v_price_source_chain := ARRAY['product_base'];
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
    -- PHASE 4: Check Partner Price List (Priority 2-3)
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
                    v_source_priority := 2;
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
                v_source_priority := 3;
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
                'product_base'
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
    -----------------------------------------------------------------
    v_price_source_chain := ARRAY['product_base'];
    
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
    RAISE WARNING '[PRICE_CRITICAL] Error in get_effective_price_json(partner=%, product=%, packaging=%): %',
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
$$;


/**
final version
**/
/**
 * get_effective_price_json - Advanced price resolution with colisage (packaging) support
 * 
 * PRICE RESOLUTION HIERARCHY:
 * 1. Partner Override (fixed price, highest priority)
 * 2. Partner Price List → Packaging Prices (if has_colisage=TRUE or packaging-specific pricing)
 * 3. Partner Price List → Detail Price (fallback from list)
 * 4. Product Packaging Fallback (from product_packagings if no pricelist packaging)
 * 5. Product Base Price (lowest priority)
 * 
 * COLISAGE ENFORCEMENT:
 * - If product.has_colisage=TRUE, packaging prices are preferred/required
 * - Auto-selects default packaging if not explicitly requested
 * - Falls back to product_packagings table if price_list has no packaging entries
 * 
 * PARAMETERS:
 *   p_partner_id   - Partner identifier (NULL allowed, uses product base price)
 *   p_product_id   - Product identifier (required)
 *   p_packaging_id - Specific packaging to use (optional, auto-select if NULL and has_colisage=TRUE)
 * 
 * RETURNS JSONB with structure:
 *   {
 *     validated_price: NUMERIC,              -- final price (after min/max clamping)
 *     raw_price: NUMERIC,                    -- price before clamping
 *     discount: NUMERIC,                     -- discount rate %
 *     source: TEXT,                          -- pricing source used
 *     source_priority: INTEGER,              -- 1-5 (1=highest, 5=lowest)
 *     price_source_chain: TEXT[],            -- ordered array of attempted sources
 *     unit: TEXT,                            -- unit of this price
 *     base_unit: TEXT,                       -- product base unit
 *     is_packaging_enforced: BOOLEAN,        -- product.has_colisage
 *     selected_packaging_id: BIGINT,         -- packaging used (if applicable)
 *     packaging_quantity: NUMERIC,           -- qty per packaging unit
 *     packaging_unit: TEXT,                  -- unit for this packaging
 *     detail: JSONB,                         -- source metadata
 *     validation_flags: JSONB,               -- clamping info
 *     decimal_allowed: BOOLEAN,              -- allows fractional qty
 *     decimal_precision: INTEGER,            -- decimal places
 *     decimal_step: NUMERIC                  -- minimum increment
 *   }
 */
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $func$
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

    v_base_unit_name := COALESCE(v_product_unit_name, 'unit');
    v_product_unit_name := COALESCE(v_product_unit_name, v_base_unit_name);

    -- Null-guard for decimal flags
    v_decimal_allowed := COALESCE(v_decimal_allowed, FALSE);
    v_decimal_precision := COALESCE(v_decimal_precision, 0);
    v_decimal_step := COALESCE(v_decimal_step, 1.0000);

    -- Verify product exists and has a price
    IF v_product_price IS NULL THEN
        RAISE WARNING '[PRICE_ENGINE] Product %: not found or missing base price', p_product_id;
        RETURN jsonb_build_object(
            'error', 'Product not found or missing price',
            'product_id', p_product_id,
            'source', 'error',
            'source_priority', 999
        );
    END IF;

    v_product_exists := TRUE;

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
$func$;


/**
ilaf
**/
/**
 * get_effective_price_json - Advanced price resolution with colisage (packaging) support
 * 
 * PRICE RESOLUTION HIERARCHY:
 * 1. Partner Override (fixed price, highest priority)
 * 2. Partner Price List → Packaging Prices (if has_colisage=TRUE or packaging-specific pricing)
 * 3. Partner Price List → Detail Price (fallback from list)
 * 4. Product Packaging Fallback (from product_packagings if no pricelist packaging)
 * 5. Product Base Price (lowest priority)
 * 
 * COLISAGE ENFORCEMENT:
 * - If product.has_colisage=TRUE, packaging prices are preferred/required
 * - Auto-selects default packaging if not explicitly requested
 * - Falls back to product_packagings table if price_list has no packaging entries
 * 
 * PARAMETERS:
 *   p_partner_id   - Partner identifier (NULL allowed, uses product base price)
 *   p_product_id   - Product identifier (required)
 *   p_packaging_id - Specific packaging to use (optional, auto-select if NULL and has_colisage=TRUE)
 * 
 * RETURNS JSONB with structure:
 *   {
 *     validated_price: NUMERIC,              -- final price (after min/max clamping)
 *     raw_price: NUMERIC,                    -- price before clamping
 *     discount: NUMERIC,                     -- discount rate %
 *     source: TEXT,                          -- pricing source used
 *     source_priority: INTEGER,              -- 1-5 (1=highest, 5=lowest)
 *     price_source_chain: TEXT[],            -- ordered array of attempted sources
 *     unit: TEXT,                            -- unit of this price
 *     base_unit: TEXT,                       -- product base unit
 *     is_packaging_enforced: BOOLEAN,        -- product.has_colisage
 *     selected_packaging_id: BIGINT,         -- packaging used (if applicable)
 *     packaging_quantity: NUMERIC,           -- qty per packaging unit
 *     packaging_unit: TEXT,                  -- unit for this packaging
 *     detail: JSONB,                         -- source metadata
 *     validation_flags: JSONB,               -- clamping info
 *     decimal_allowed: BOOLEAN,              -- allows fractional qty
 *     decimal_precision: INTEGER,            -- decimal places
 *     decimal_step: NUMERIC                  -- minimum increment
 *   }
 */
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $func$
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
$func$;


--- fial price ttc
/**
 * get_effective_price_json - Advanced price resolution with colisage (packaging) support
 * 
 * PRICE RESOLUTION HIERARCHY:
 * 1. Partner Override (fixed price, highest priority)
 * 2. Partner Price List → Packaging Prices (if has_colisage=TRUE or packaging-specific pricing)
 * 3. Partner Price List → Detail Price (fallback from list)
 * 4. Product Packaging Fallback (from product_packagings if no pricelist packaging)
 * 5. Product Base Price TTC (lowest priority) - USES price_ttc field
 * 
 * COLISAGE ENFORCEMENT:
 * - If product.has_colisage=TRUE, packaging prices are preferred/required
 * - Auto-selects default packaging if not explicitly requested
 * - Falls back to product_packagings table if price_list has no packaging entries
 * 
 * PARAMETERS:
 *   p_partner_id   - Partner identifier (NULL allowed, uses product base price_ttc)
 *   p_product_id   - Product identifier (required)
 *   p_packaging_id - Specific packaging to use (optional, auto-select if NULL and has_colisage=TRUE)
 * 
 * RETURNS JSONB with structure:
 *   {
 *     validated_price: NUMERIC,              -- final price (after min/max clamping)
 *     raw_price: NUMERIC,                    -- price before clamping
 *     discount: NUMERIC,                     -- discount rate %
 *     source: TEXT,                          -- pricing source used
 *     source_priority: INTEGER,              -- 1-5 (1=highest, 5=lowest)
 *     price_source_chain: TEXT[],            -- ordered array of attempted sources
 *     unit: TEXT,                            -- unit of this price
 *     base_unit: TEXT,                       -- product base unit
 *     is_packaging_enforced: BOOLEAN,        -- product.has_colisage
 *     selected_packaging_id: BIGINT,         -- packaging used (if applicable)
 *     packaging_quantity: NUMERIC,           -- qty per packaging unit
 *     packaging_unit: TEXT,                  -- unit for this packaging
 *     detail: JSONB,                         -- source metadata
 *     validation_flags: JSONB,               -- clamping info
 *     decimal_allowed: BOOLEAN,              -- allows fractional qty
 *     decimal_precision: INTEGER,            -- decimal places
 *     decimal_step: NUMERIC                  -- minimum increment
 *   }
 */
CREATE OR REPLACE FUNCTION get_effective_price_json(
    p_partner_id   BIGINT,
    p_product_id   BIGINT,
    p_packaging_id BIGINT DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $func$
DECLARE
    -- Product base data
    v_product_price      NUMERIC;
    v_product_price_ttc  NUMERIC;  -- NEW: TTC price field
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
    -- MODIFIED: Now fetches both price and price_ttc
    -----------------------------------------------------------------
    SELECT 
        pr.price,
        pr.price_ttc,  -- NEW: Fetch price_ttc
        u.name,
        COALESCE(pr.has_colisage, FALSE),
        COALESCE(pf.decimal_quantity_allowed, FALSE),
        COALESCE(pf.decimal_precision, 0),
        COALESCE(pf.decimal_step, 1.0000)
    INTO 
        v_product_price,
        v_product_price_ttc,  -- NEW: Store price_ttc
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

    -- MODIFIED: Use price_ttc as primary, fallback to price if needed
    -- Prioritize price_ttc if available, otherwise use price
    IF v_product_price_ttc IS NOT NULL THEN
        v_product_price := v_product_price_ttc;
    ELSIF v_product_price IS NULL THEN
        RAISE WARNING '[PRICE_ENGINE] Product %: missing both price_ttc and price', p_product_id;
        RETURN jsonb_build_object(
            'error', 'Product missing price',
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
    -- MODIFIED: Returns price_ttc when partner has no pricelist
    -----------------------------------------------------------------
    IF p_partner_id IS NOT NULL THEN
        PERFORM 1 FROM partners 
        WHERE id = p_partner_id AND price_list_id IS NOT NULL;
        
        IF NOT FOUND THEN
            RAISE DEBUG '[PRICE_ENGINE] Partner %: no price list configured, fallback to product base (price_ttc)', p_partner_id;
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
                    'price_field', 'price_ttc',  -- NEW: Indicate which field was used
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
    -- MODIFIED: Now uses price_ttc when type is product_base
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
            'price_field', 'price_ttc',  -- NEW: Indicate which field was used
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
$func$;