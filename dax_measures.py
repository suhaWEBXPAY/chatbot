dax_measures = [
    # ================ SECTION 1: tbl_payment_gateway TABLE MEASURES ================
    {
        "name": "AbandedCount",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 1 && LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id])))",
        "description": "Counts abandoned orders for the current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "AbandodaAmount",
        "formula": "CALCULATE(SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 1 && LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id])),'webxpay_master tbl_order'[total_amount]))",
        "description": "Calculates total amount of abandoned orders for the current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "Abandoned%",
        "formula": "DIVIDE([AbanVolTotal], [AllCountFor%], 0)",
        "description": "Percentage of abandoned transactions across all gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "AbanValTotal",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [AbandodaAmount])",
        "description": "Total abandoned amount aggregated across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "AbanVolTotal",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [AbandedCount])",
        "description": "Total abandoned count aggregated across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "ActiveGatewayDisplay",
        "formula": "CALCULATE(SELECTEDVALUE('webxpay_master tbl_payment_gateway'[description]), 'webxpay_master tbl_payment_gateway'[active] = 1)",
        "description": "Displays description of active payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "AllCountFor%",
        "formula": "[AppVolTotal] + [DecVolTotal]",
        "description": "Total volume for percentage calculations (approved + declined).",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "Approved%",
        "formula": "DIVIDE([AppVolTotal], [AllCountFor%], 0)",
        "description": "Percentage of approved transactions across all gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "ApprovedCountPercentage",
        "formula": "DIVIDE(CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2 && LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id]))), CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order',LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id]))))",
        "description": "Percentage of approved orders for the current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "AppValTotal",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [OrderAmountPerGateway])",
        "description": "Total approved amount aggregated across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "AppVolTotal",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [OrderCountPerGateway])",
        "description": "Total approved count aggregated across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "BankRateAmountTotal (ForOrder)",
        "formula": "CALCULATE(SUMX('webxpay_master tbl_order', VAR ParentRate = LOOKUPVALUE('webxpay_master tbl_order_parent_gateway'[parent_gateway_rate],'webxpay_master tbl_order_parent_gateway'[order_id],'webxpay_master tbl_order'[order_id]) VAR BankRate = 'webxpay_master tbl_order'[BankGatewayRateAsNumber] VAR OrderType = 'webxpay_master tbl_order'[order_type_id] VAR BaseAmount = 'webxpay_master tbl_order'[OrderBaseAmount] RETURN IF(OrderType = 3, BaseAmount * ((BankRate + ParentRate) / 100), BaseAmount * (BankRate / 100))), 'webxpay_master tbl_order'[payment_status_id] = 2, TREATAS({ MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id]) }, 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id]))",
        "description": "Calculates total bank cost amount for approved orders of current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "Declined %",
        "formula": "DIVIDE([DecVolTotal], [AllCountFor%], 0)",
        "description": "Percentage of declined transactions across all gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "DeclinedAmount",
        "formula": "VAR Gateways = VALUES ( 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) RETURN SUMX ( Gateways, VAR CurrentGatewayID = [payment_gateway_id] VAR StoreGatewayIDs = SELECTCOLUMNS ( FILTER ( ALL ( 'webxpay_master tbl_store_payment_gateway_2' ), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] = CurrentGatewayID ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN CALCULATE ( SUM ( 'webxpay_master tbl_order'[OrderBaseAmount] ), 'webxpay_master tbl_order'[payment_status_id] = 3, 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs ) )",
        "description": "Calculates total declined amount for all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "DeclinedCount",
        "formula": "VAR Gateways = VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]) RETURN SUMX( Gateways, CALCULATE( COUNTROWS('webxpay_master tbl_order'), FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 3 && LOOKUPVALUE( 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id], 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id], 'webxpay_master tbl_order'[store_payment_gateway_id] ) = 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) ) )",
        "description": "Counts declined orders for all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "DeclinedCountPercentage",
        "formula": "DIVIDE(CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 3 && LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id]))), CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order',LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id]))))",
        "description": "Percentage of declined orders for the current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "DecValTotal",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [DeclinedAmount])",
        "description": "Total declined amount aggregated across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "DecVolTotal",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [DeclinedCount])",
        "description": "Total declined count aggregated across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "GatewayBankRateAmountTotal",
        "formula": "VAR Gateways = VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]) RETURN SUMX( Gateways, VAR CurrentGateway = [payment_gateway_id] RETURN CALCULATE( SUMX( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 2 && LOOKUPVALUE( 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id], 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id], 'webxpay_master tbl_order'[store_payment_gateway_id] ) = CurrentGateway ), VAR baseAmount = 'webxpay_master tbl_order'[total_amount] VAR bankRate = 'webxpay_master tbl_order'[BankGatewayRateAsNumber] VAR orderType = 'webxpay_master tbl_order'[order_type_id] VAR parentRate = LOOKUPVALUE( 'webxpay_master tbl_order_parent_gateway'[parent_gateway_rate], 'webxpay_master tbl_order_parent_gateway'[order_id], 'webxpay_master tbl_order'[order_id] ) RETURN IF( orderType = 3, baseAmount * ((bankRate + parentRate) / 100), baseAmount * (bankRate / 100) ) ) ) )",
        "description": "Total bank cost amount for approved orders across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "GatewayRateAmountTotal",
        "formula": "VAR Gateways = VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]) RETURN SUMX( Gateways, VAR CurrentGateway = [payment_gateway_id] RETURN CALCULATE( SUMX( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 2 && LOOKUPVALUE( 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id], 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id], 'webxpay_master tbl_order'[store_payment_gateway_id] ) = CurrentGateway ), 'webxpay_master tbl_order'[total_amount] * ('webxpay_master tbl_order'[PaymentGatewayRateAsNumber] / 100) ) ) )",
        "description": "Total gateway revenue from merchant rates for approved orders across all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "GatewayRateAmountTotal (ForOrder)",
        "formula": "VAR SelectedGateway = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id]) RETURN CALCULATE( SUMX( 'webxpay_master tbl_order', VAR ParentRate = LOOKUPVALUE( 'webxpay_master tbl_order_parent_gateway'[parent_gateway_rate], 'webxpay_master tbl_order_parent_gateway'[order_id], 'webxpay_master tbl_order'[order_id] ) VAR BaseAmount = 'webxpay_master tbl_order'[OrderBaseAmount] VAR GatewayRate = 'webxpay_master tbl_order'[PaymentGatewayRateAsNumber] VAR OrderType = 'webxpay_master tbl_order'[order_type_id] RETURN BaseAmount * (GatewayRate / 100) ), 'webxpay_master tbl_order'[payment_status_id] = 2, TREATAS( { SelectedGateway }, 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] ) )",
        "description": "Total gateway revenue for approved orders of current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "IPG_Provider_Name",
        "formula": "RELATED('webxpay_master tbl_ipg_provider'[ipg_provider])",
        "description": "Returns IPG provider name from related table.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "OrderAmountPerGateway",
        "formula": "VAR Gateways = VALUES ( 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) RETURN SUMX ( Gateways, CALCULATE ( SUMX ( FILTER ( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 2 && LOOKUPVALUE ( 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id], 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id], 'webxpay_master tbl_order'[store_payment_gateway_id] ) = MAX ( 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) ), 'webxpay_master tbl_order'[total_amount] ) ) )",
        "description": "Total amount of approved orders for all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "OrderCountLKR",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2 && 'webxpay_master tbl_order'[processing_currency_id] = '5' && LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id])))",
        "description": "Counts LKR transactions for the current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "OrderCountPerGateway",
        "formula": "VAR Gateways = VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]) RETURN SUMX( Gateways, CALCULATE( COUNTROWS('webxpay_master tbl_order'), FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 2 && LOOKUPVALUE( 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id], 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id], 'webxpay_master tbl_order'[store_payment_gateway_id] ) = 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) ) )",
        "description": "Counts approved orders for all payment gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "OrderCountUSD",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2 && 'webxpay_master tbl_order'[processing_currency_id] = '2' && LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id],'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id],'webxpay_master tbl_order'[store_payment_gateway_id]) = MAX('webxpay_master tbl_payment_gateway'[payment_gateway_id])))",
        "description": "Counts USD transactions for the current payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "Revenue",
        "formula": "'webxpay_master tbl_payment_gateway'[GatewayRateAmountTotal] - [GatewayBankRateAmountTotal]",
        "description": "Revenue calculation (merchant rate - bank rate) for payment gateway.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumIPGCost(LKR)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 5), [GatewayBankRateAmountTotal])",
        "description": "Total IPG bank cost for LKR currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumIPGCost(USD)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 2), [GatewayBankRateAmountTotal])",
        "description": "Total IPG bank cost for USD currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumMerchantCost(LKR)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 5), [GatewayRateAmountTotal])",
        "description": "Total merchant cost for LKR currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumMerchantCost(USD)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 2), [GatewayRateAmountTotal])",
        "description": "Total merchant cost for USD currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumRevenue (LKR)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 5), [Revenue])",
        "description": "Total revenue for LKR currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumRevenue (USD)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 2), [Revenue])",
        "description": "Total revenue for USD currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumTotalAmount(LKR)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 5), [OrderAmountPerGateway])",
        "description": "Total approved amount for LKR currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumTotalAmount(USD)",
        "formula": "SUMX(FILTER(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), CALCULATE(MAX('webxpay_master tbl_payment_gateway'[currency_id])) = 2), [OrderAmountPerGateway])",
        "description": "Total approved amount for USD currency gateways.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "SumTotalCount",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [OrderCountLKR] + [OrderCountUSD])",
        "description": "Total count of approved transactions across all gateways and currencies.",
        "table": "tbl_payment_gateway"
    },
    {
        "name": "Transaction Count by Payment Gateway",
        "formula": "VAR SelectedStatus = SELECTEDVALUE('webxpay_master tbl_order'[payment_status_id]) RETURN COUNTROWS( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = SelectedStatus ) )",
        "description": "Counts transactions based on selected payment status.",
        "table": "tbl_payment_gateway"
    },

    # ================ SECTION 2: tbl_pos_transactions TABLE MEASURES ================
    {
        "name": "Amex_POS_revenue",
        "formula": "IF ( ISBLANK ( 'webxpay_master tbl_pos_transactions'[txn_type] ) || TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) = '', BLANK (), ((('webxpay_master tbl_pos_transactions'[mdr_rate] - 3) / 100)) * 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount] )",
        "description": "Calculates revenue from Amex POS transactions (MDR rate - 3%).",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "ApprovedPOSPaymentCount",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_pos_transactions'), 'webxpay_master tbl_pos_transactions'[txn_status] = 'Settled')",
        "description": "Counts settled POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Bank_Cost_Amount",
        "formula": "VAR TxnType = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) ) VAR Card = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[POS_Card_Narration] ) ) VAR CostPercent = SWITCH( TRUE(), Card = 'visa_master', 1.7, Card = 'amex', 3, BLANK() ) RETURN IF ( ISBLANK ( TxnType ) || TxnType = '', BLANK(), IF ( TxnType = 'amex' || TxnType = 'sale', IF ( NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount] ), ( CostPercent / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount], ( CostPercent / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_NoAmexSettledAmount] ), BLANK() ) )",
        "description": "Calculates bank cost amount for POS transactions based on card type.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Bank_Cost_Amount_Table",
        "formula": "VAR TxnType = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) ) VAR BaseAmount = IF ( NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount] ), ( 3 / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount], ( 1.7 / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_NoAmexSettledAmount] ) VAR Result = IF ( TxnType = 'amex' || TxnType = 'sale', BaseAmount, BLANK () ) RETURN Result",
        "description": "Calculates bank cost amount as a table column for POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "BankName",
        "formula": "VAR TxnType = LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) RETURN IF ( TxnType <> 'void_amex' && TxnType <> 'void_sale' && TxnType <> '', SWITCH( 'webxpay_master tbl_pos_transactions'[ipg_provider_id], 6, 'DFCC', 5, 'HNB' ), BLANK() )",
        "description": "Returns bank name based on IPG provider ID for non-void transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "BR",
        "formula": "VAR TxnType = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) ) VAR Card = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[POS_Card_Narration] ) ) RETURN IF ( TxnType = 'amex' || TxnType = 'sale', SWITCH( TRUE(), Card = 'visa_master', 1.7, Card = 'amex', 3, BLANK() ), BLANK() )",
        "description": "Returns bank rate percentage based on card type for sale transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "DFCC_POS_revenue",
        "formula": "IF ( ISBLANK ( 'webxpay_master tbl_pos_transactions'[txn_type] ) || TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) = '', BLANK (), ( ('webxpay_master tbl_pos_transactions'[mdr_rate] - 1.7) / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_NoAmexSettledAmount] )",
        "description": "Calculates revenue from DFCC POS transactions (MDR rate - 1.7%).",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "DistinctMerchants_AmexSale",
        "formula": "CALCULATE(DISTINCTCOUNT('webxpay_master tbl_pos_transactions'[pos_transaction_id]), FILTER('webxpay_master tbl_pos_transactions', LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) = 'amex' || LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) = 'sale'))",
        "description": "Counts distinct merchants with Amex or sale transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "FormattedTranDate",
        "formula": "VAR TxnType = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) ) RETURN IF ( TxnType = 'sale' || TxnType = 'amex', FORMAT ( 'webxpay_master tbl_pos_transactions'[transaction_date], 'dd/mm/yyyy' ), BLANK () )",
        "description": "Returns formatted transaction date for sale/amex transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "MDR_Amount",
        "formula": "VAR TxnType = LOWER ( TRIM ( 'webxpay_master tbl_pos_transactions'[txn_type] ) ) RETURN IF ( TxnType = 'sale' || TxnType = 'amex', IF ( NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount] ), ( 'webxpay_master tbl_pos_transactions'[mdr_rate] / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount], ( 'webxpay_master tbl_pos_transactions'[mdr_rate] / 100 ) * 'webxpay_master tbl_pos_transactions'[POS_NoAmexSettledAmount] ), BLANK () )",
        "description": "Calculates MDR amount for sale/amex transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "MDR_Amount_Table",
        "formula": "VAR TxnType = TRIM('webxpay_master tbl_pos_transactions'[txn_type]) VAR BaseAmount = IF ( NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount] ), ('webxpay_master tbl_pos_transactions'[mdr_rate] / 100) * 'webxpay_master tbl_pos_transactions'[POS_AmexSettledAmount], ('webxpay_master tbl_pos_transactions'[mdr_rate] / 100) * 'webxpay_master tbl_pos_transactions'[POS_NoAmexSettledAmount] ) RETURN VAR Result = IF ( TxnType = 'amex' || TxnType = 'sale', BaseAmount, BLANK () ) RETURN Result",
        "description": "Calculates MDR amount as a table column.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "MDR_Display",
        "formula": "VAR TxnType = LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) RETURN IF ( TxnType = 'sale' || TxnType = 'amex', 'webxpay_master tbl_pos_transactions'[mdr_rate], BLANK() )",
        "description": "Displays MDR rate for sale/amex transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Net Amount (Settled)",
        "formula": "CALCULATE(SUMX(FILTER('webxpay_master tbl_pos_transactions','webxpay_master tbl_pos_transactions'[txn_status] = 'settled'),'webxpay_master tbl_pos_transactions'[amount]*(1 - ('webxpay_master tbl_pos_transactions'[mdr_rate]/100))))",
        "description": "Calculates net settled amount after MDR deduction.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "POS_AmexSettledAmount",
        "formula": "IF ( ( LEFT ( 'webxpay_master tbl_pos_transactions'[POS_Card_Narration], 30 ) = 'amex' ) && 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[amount], BLANK () )",
        "description": "Returns Amex settled amount for LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "POS_Card_Narration",
        "formula": "CALCULATE(MAX('webxpay_master tbl_pos_store_bank_mid'[narration]), FILTER('webxpay_master tbl_pos_store_bank_mid','webxpay_master tbl_pos_store_bank_mid'[bank_merchant_mid] = 'webxpay_master tbl_pos_transactions'[bank_merchant_mid]))",
        "description": "Gets card narration from POS store bank MID table.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "POS_Card_Narration_Filtered",
        "formula": "VAR TxnType = LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) RETURN SWITCH( TxnType, 'amex', 'Amex', 'sale', 'Visa Master', BLANK() )",
        "description": "Returns simplified card type based on transaction type.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "POS_Margin",
        "formula": "DIVIDE ( [Total_Revenue], [Total Amount], 0 )",
        "description": "Calculates POS margin percentage.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "POS_NoAmexSettledAmount",
        "formula": "IF ( LEFT ( 'webxpay_master tbl_pos_transactions'[POS_Card_Narration], 30 ) = 'visa_master' && ( 'webxpay_master tbl_pos_transactions'[currency] = 'LKR' ), 'webxpay_master tbl_pos_transactions'[amount], BLANK () )",
        "description": "Returns non-Amex settled amount for LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "SuccessfulMerchantPOSAttempt",
        "formula": "CALCULATE(DISTINCTCOUNT('webxpay_master tbl_pos_transactions'[store_id]), FILTER('webxpay_master tbl_pos_transactions', VAR TxnType = LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) RETURN TxnType <> 'void_sale' && TxnType <> 'void_amex' && TxnType <> '' && NOT ISBLANK(TxnType)))",
        "description": "Counts distinct stores with non-void POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount",
        "formula": "CALCULATE ( SUM ( 'webxpay_master tbl_pos_transactions'[amount] ), 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[txn_type] IN { 'sale', 'amex' } )",
        "description": "Total amount of LKR sale/amex POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount (PVI 5)",
        "formula": "CALCULATE ( SUM ( 'webxpay_master tbl_pos_transactions'[amount] ), 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[ipg_provider_id] = 5, NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[amount] ), ( 'webxpay_master tbl_pos_transactions'[txn_type] = 'sale' || 'webxpay_master tbl_pos_transactions'[txn_type] = 'amex' ) )",
        "description": "Total amount for IPG provider 5 (HNB) LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount (PVI 6)",
        "formula": "CALCULATE ( SUM ( 'webxpay_master tbl_pos_transactions'[amount] ), 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[ipg_provider_id] = 6, NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[amount] ), ( 'webxpay_master tbl_pos_transactions'[txn_type] = 'sale' || 'webxpay_master tbl_pos_transactions'[txn_type] = 'amex' ) )",
        "description": "Total amount for IPG provider 6 (DFCC) LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount (settled)",
        "formula": "CALCULATE(SUM('webxpay_master tbl_pos_transactions'[amount]), FILTER('webxpay_master tbl_pos_transactions','webxpay_master tbl_pos_transactions'[txn_status] = 'settled'))",
        "description": "Total amount of settled POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount Count",
        "formula": "CALCULATE ( COUNTROWS ( 'webxpay_master tbl_pos_transactions' ), 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[txn_type] IN { 'sale', 'amex' } )",
        "description": "Count of LKR sale/amex POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount Count (PVI 5)",
        "formula": "CALCULATE ( COUNTROWS ( 'webxpay_master tbl_pos_transactions' ), 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[ipg_provider_id] = 5, NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[amount] ), 'webxpay_master tbl_pos_transactions'[txn_type] = 'sale' || 'webxpay_master tbl_pos_transactions'[txn_type] = 'amex' )",
        "description": "Count of IPG provider 5 (HNB) LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount Count (PVI 6)",
        "formula": "CALCULATE ( COUNTROWS ( 'webxpay_master tbl_pos_transactions' ), 'webxpay_master tbl_pos_transactions'[currency] = 'LKR', 'webxpay_master tbl_pos_transactions'[ipg_provider_id] = 6, NOT ISBLANK ( 'webxpay_master tbl_pos_transactions'[amount] ), 'webxpay_master tbl_pos_transactions'[txn_type] = 'sale' || 'webxpay_master tbl_pos_transactions'[txn_type] = 'amex' )",
        "description": "Count of IPG provider 6 (DFCC) LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total Amount Count (settled)",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_pos_transactions'[amount]), FILTER('webxpay_master tbl_pos_transactions','webxpay_master tbl_pos_transactions'[txn_status] = 'settled'))",
        "description": "Count of settled POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total MDR Revenue (Settled)",
        "formula": "CALCULATE(SUMX(FILTER('webxpay_master tbl_pos_transactions','webxpay_master tbl_pos_transactions'[txn_status] = 'settled'),'webxpay_master tbl_pos_transactions'[amount]*('webxpay_master tbl_pos_transactions'[mdr_rate]/100)))",
        "description": "Total MDR revenue from settled POS transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total_Amount_Column",
        "formula": "VAR TxnType = LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) VAR AmountVal = 'webxpay_master tbl_pos_transactions'[amount] RETURN IF ( ISBLANK(TxnType) || TxnType = '', BLANK(), IF ( 'webxpay_master tbl_pos_transactions'[currency] = 'LKR' && NOT ISBLANK(AmountVal), SWITCH( TRUE(), TxnType = 'sale' || TxnType = 'amex', AmountVal, TxnType = 'void_sale' || TxnType = 'void_amex', -AmountVal, BLANK() ), BLANK() ) )",
        "description": "Calculates amount column with void adjustments for LKR transactions.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total_Revenue",
        "formula": "SUMX ( 'webxpay_master tbl_pos_transactions', VAR TxnType = TRIM('webxpay_master tbl_pos_transactions'[txn_type]) VAR RowRevenue = 'webxpay_master tbl_pos_transactions'[DFCC_POS_revenue] + 'webxpay_master tbl_pos_transactions'[Amex_POS_revenue] RETURN IF ( TxnType = 'void_sale' || TxnType = 'void_amex', -RowRevenue, RowRevenue ) )",
        "description": "Total POS revenue with void transaction adjustments.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "Total_Revenue_Column",
        "formula": "VAR TxnType = LOWER(TRIM('webxpay_master tbl_pos_transactions'[txn_type])) VAR BaseRevenue = 'webxpay_master tbl_pos_transactions'[DFCC_POS_revenue] + 'webxpay_master tbl_pos_transactions'[Amex_POS_revenue] RETURN IF ( ISBLANK(TxnType) || TxnType = '', BLANK(), SWITCH( TRUE(), TxnType = 'sale' || TxnType = 'amex', BaseRevenue, TxnType = 'void_sale' || TxnType = 'void_amex', -BaseRevenue, BLANK() ) )",
        "description": "Revenue column with void adjustments for table display.",
        "table": "tbl_pos_transactions"
    },
    {
        "name": "TotalPosTransactionAmount",
        "formula": "SUM('webxpay_master tbl_pos_transactions'[amount])",
        "description": "Sum of all POS transaction amounts.",
        "table": "tbl_pos_transactions"
    },

    # ================ SECTION 3: tbl_store TABLE MEASURES ================
    {
        "name": "Aband %",
        "formula": "DIVIDE([AbandonedOrderCount],[AllCountFor %],0)",
        "description": "Percentage of abandoned transactions.",
        "table": "tbl_store"
    },
    {
        "name": "AbandodValue",
        "formula": "CALCULATE(SUMX('webxpay_master tbl_order','webxpay_master tbl_order'[OrderBaseAmount]), 'webxpay_master tbl_order'[payment_status_id] = 1)",
        "description": "Total value of abandoned orders.",
        "table": "tbl_store"
    },
    {
        "name": "AbandonedOrderCount",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), 'webxpay_master tbl_order'[payment_status_id] = 1)",
        "description": "Count of abandoned orders.",
        "table": "tbl_store"
    },
    {
        "name": "AbandonedRate",
        "formula": "FORMAT(DIVIDE(CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), 'webxpay_master tbl_order'[payment_status_id] = 1), CALCULATE(COUNT('webxpay_master tbl_order'[order_id]))), '0.00%')",
        "description": "Formatted percentage of abandoned orders.",
        "table": "tbl_store"
    },
    {
        "name": "ActiveMerchants",
        "formula": "CALCULATE(SELECTEDVALUE('webxpay_master tbl_store'[doing_business_name]), 'webxpay_master tbl_store'[is_active] = TRUE())",
        "description": "Returns active merchant business name.",
        "table": "tbl_store"
    },
    {
        "name": "AllCountFor %",
        "formula": "[AbandonedOrderCount]+[DeclinedOrderCount]+[SuccessfulOrderCount]",
        "description": "Total count for percentage calculations (abandoned + declined + successful).",
        "table": "tbl_store"
    },
    {
        "name": "App %",
        "formula": "DIVIDE([SuccessfulOrderCount],[AllCountFor %],0)",
        "description": "Percentage of successful transactions.",
        "table": "tbl_store"
    },
    {
        "name": "ApprovedOrderCount",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Count of approved orders.",
        "table": "tbl_store"
    },
    {
        "name": "DeclinedOrderCount",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), 'webxpay_master tbl_order'[payment_status_id] = 3)",
        "description": "Count of declined orders.",
        "table": "tbl_store"
    },
    {
        "name": "DeclinedRate",
        "formula": "FORMAT(DIVIDE(CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), 'webxpay_master tbl_order'[payment_status_id] = 3), CALCULATE(COUNT('webxpay_master tbl_order'[order_id]))), '0.00%')",
        "description": "Formatted percentage of declined orders.",
        "table": "tbl_store"
    },
    {
        "name": "DeclinedValue",
        "formula": "CALCULATE(SUMX('webxpay_master tbl_order','webxpay_master tbl_order'[OrderBaseAmount]), 'webxpay_master tbl_order'[payment_status_id] = 3)",
        "description": "Total value of declined orders.",
        "table": "tbl_store"
    },
    {
        "name": "Declnd %",
        "formula": "DIVIDE([DeclinedOrderCount],[AllCountFor %],0)",
        "description": "Percentage of declined transactions.",
        "table": "tbl_store"
    },
    {
        "name": "GrossProfit",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2), VAR BaseAmount = IF('webxpay_master tbl_order'[CurrencyLabel] = 'USD','webxpay_master tbl_order'[total_amount] * 'webxpay_master tbl_order'[ExchangeRateAsNumber],'webxpay_master tbl_order'[total_amount]) VAR BankRate = 'webxpay_master tbl_order'[BankGatewayRateAsNumber] VAR GatewayRate = 'webxpay_master tbl_order'[PaymentGatewayRateAsNumber] VAR OrderType = 'webxpay_master tbl_order'[order_type_id] VAR ParentRate = LOOKUPVALUE('webxpay_master tbl_order_parent_gateway'[parent_gateway_rate],'webxpay_master tbl_order_parent_gateway'[order_id],'webxpay_master tbl_order'[order_id]) VAR TotalRate = IF(OrderType = 3, GatewayRate + ParentRate, GatewayRate) RETURN BaseAmount - (BaseAmount * BankRate / 100) - (BaseAmount * TotalRate / 100))",
        "description": "Calculates gross profit from approved orders.",
        "table": "tbl_store"
    },
    {
        "name": "SuccessfulOrderCount",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Count of successful orders.",
        "table": "tbl_store"
    },
    {
        "name": "TotalAbandonedAmount",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 1), VAR ExchangeRate = 'webxpay_master tbl_order'[ExchangeRateAsNumber] VAR OrderCurrency = IF(ISBLANK('webxpay_master tbl_order'[processing_currency_id]) || TRIM('webxpay_master tbl_order'[processing_currency_id]) = '', BLANK(), VALUE('webxpay_master tbl_order'[processing_currency_id])) VAR OrderDate = 'webxpay_master tbl_order'[date_added] VAR FallbackRate = CALCULATE(MAX('webxpay_master tbl_exchange_rate'[buying_rate]), FILTER('webxpay_master tbl_exchange_rate','webxpay_master tbl_exchange_rate'[currency_id] = OrderCurrency && 'webxpay_master tbl_exchange_rate'[date] <= OrderDate)) VAR ShouldUseFallback = ISBLANK(ExchangeRate) && 'webxpay_master tbl_order'[processing_currency_id] = 'webxpay_master tbl_order'[payment_currency_id] && 'webxpay_master tbl_order'[CurrencyLabel] <> 'LKR' VAR FinalRate = IF(ShouldUseFallback, FallbackRate, ExchangeRate) VAR BaseAmount = IF('webxpay_master tbl_order'[CurrencyLabel] <> 'LKR','webxpay_master tbl_order'[total_amount] * FinalRate,'webxpay_master tbl_order'[total_amount]) RETURN BaseAmount)",
        "description": "Total amount of abandoned orders with currency conversion.",
        "table": "tbl_store"
    },
    {
        "name": "TotalAmount",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2), 'webxpay_master tbl_order'[OrderBaseAmount])",
        "description": "Total amount of approved orders.",
        "table": "tbl_store"
    },
    {
        "name": "TotalApprovedAmount",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2), VAR ExchangeRate = 'webxpay_master tbl_order'[ExchangeRateAsNumber] VAR OrderCurrency = IF(ISBLANK('webxpay_master tbl_order'[processing_currency_id]) || TRIM('webxpay_master tbl_order'[processing_currency_id]) = '', BLANK(), VALUE('webxpay_master tbl_order'[processing_currency_id])) VAR OrderDate = 'webxpay_master tbl_order'[date_added] VAR FallbackRate = CALCULATE(MAX('webxpay_master tbl_exchange_rate'[buying_rate]), FILTER('webxpay_master tbl_exchange_rate','webxpay_master tbl_exchange_rate'[currency_id] = OrderCurrency && 'webxpay_master tbl_exchange_rate'[date] <= OrderDate)) VAR ShouldUseFallback = ISBLANK(ExchangeRate) && 'webxpay_master tbl_order'[processing_currency_id] = 'webxpay_master tbl_order'[payment_currency_id] && 'webxpay_master tbl_order'[CurrencyLabel] <> 'LKR' VAR FinalRate = IF(ShouldUseFallback, FallbackRate, ExchangeRate) VAR BaseAmount = IF('webxpay_master tbl_order'[CurrencyLabel] <> 'LKR','webxpay_master tbl_order'[total_amount] * FinalRate,'webxpay_master tbl_order'[total_amount]) RETURN BaseAmount)",
        "description": "Total approved amount with currency conversion.",
        "table": "tbl_store"
    },
    {
        "name": "TotalDeclinedAmount",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 3), VAR ExchangeRate = 'webxpay_master tbl_order'[ExchangeRateAsNumber] VAR OrderCurrency = IF(ISBLANK('webxpay_master tbl_order'[processing_currency_id]) || TRIM('webxpay_master tbl_order'[processing_currency_id]) = '', BLANK(), VALUE('webxpay_master tbl_order'[processing_currency_id])) VAR OrderDate = 'webxpay_master tbl_order'[date_added] VAR FallbackRate = CALCULATE(MAX('webxpay_master tbl_exchange_rate'[buying_rate]), FILTER('webxpay_master tbl_exchange_rate','webxpay_master tbl_exchange_rate'[currency_id] = OrderCurrency && 'webxpay_master tbl_exchange_rate'[date] <= OrderDate)) VAR ShouldUseFallback = ISBLANK(ExchangeRate) && 'webxpay_master tbl_order'[processing_currency_id] = 'webxpay_master tbl_order'[payment_currency_id] && 'webxpay_master tbl_order'[CurrencyLabel] <> 'LKR' VAR FinalRate = IF(ShouldUseFallback, FallbackRate, ExchangeRate) VAR BaseAmount = IF('webxpay_master tbl_order'[CurrencyLabel] <> 'LKR','webxpay_master tbl_order'[total_amount] * FinalRate,'webxpay_master tbl_order'[total_amount]) RETURN BaseAmount)",
        "description": "Total declined amount with currency conversion.",
        "table": "tbl_store"
    },
    {
        "name": "TotalGatewayRevenue",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [GatewayRateAmountTotal (ForOrder)] - [BankRateAmountTotal (ForOrder)])",
        "description": "Total revenue from all payment gateways.",
        "table": "tbl_store"
    },

    # ================ SECTION 4: tbl_order TABLE MEASURES ================
    {
        "name": "AbandonedPercentage",
        "formula": "DIVIDE('webxpay_master tbl_store'[AbandonedOrderCount],[AllCount],0)",
        "description": "Percentage of abandoned orders for store.",
        "table": "tbl_order"
    },
    {
        "name": "All Net Profit",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2), VAR BaseAmount = 'webxpay_master tbl_order'[total_amount] RETURN BaseAmount)",
        "description": "Total net profit from approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "All_Value",
        "formula": "[IPG_TotalAmount_LKR] + [IPG_TotalAmount_USD]",
        "description": "Total value across LKR and USD currencies.",
        "table": "tbl_order"
    },
    {
        "name": "AllCount",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[total_amount]))",
        "description": "Count of all orders.",
        "table": "tbl_order"
    },
    {
        "name": "App_Dec_Transaction_Count_All",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_refference_number]), 'webxpay_master tbl_order'[payment_status_id] <> 1)",
        "description": "Count of non-abandoned transactions.",
        "table": "tbl_order"
    },
    {
        "name": "ApprovedPaymentCount",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), CONTAINSSTRING(TRIM('webxpay_master tbl_order'[created_payment_status]), 'Approved'))",
        "description": "Count of orders with 'Approved' payment status.",
        "table": "tbl_order"
    },
    {
        "name": "ApprovedPercentage",
        "formula": "DIVIDE('webxpay_master tbl_store'[ApprovedOrderCount],[AllCount],0)",
        "description": "Percentage of approved orders for store.",
        "table": "tbl_order"
    },
    {
        "name": "BankApprovedPercentage",
        "formula": "DIVIDE('webxpay_master tbl_store'[ApprovedOrderCount],[App_Dec_Transaction_Count_All],0)",
        "description": "Bank approval percentage (excluding abandoned).",
        "table": "tbl_order"
    },
    {
        "name": "BankDeclinedPercentage",
        "formula": "DIVIDE('webxpay_master tbl_store'[DeclinedOrderCount],[App_Dec_Transaction_Count_All],0)",
        "description": "Bank decline percentage (excluding abandoned).",
        "table": "tbl_order"
    },
    {
        "name": "BankFee",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [BankRateAmountTotal (ForOrder)])",
        "description": "Total bank fee across all payment gateways.",
        "table": "tbl_order"
    },
    {
        "name": "BankGatewayRateAsNumber",
        "formula": "IF(NOT(ISBLANK('webxpay_master tbl_order'[bank_payment_gateway_rate])) && NOT(ISERROR(VALUE('webxpay_master tbl_order'[bank_payment_gateway_rate]))), VALUE('webxpay_master tbl_order'[bank_payment_gateway_rate]), BLANK())",
        "description": "Converts bank gateway rate to number.",
        "table": "tbl_order"
    },
    {
        "name": "BankRateCombined",
        "formula": "VAR OrderType = 'webxpay_master tbl_order'[order_type_id] VAR BankRate = 'webxpay_master tbl_order'[BankGatewayRateAsNumber] VAR ParentRate = LOOKUPVALUE('webxpay_master tbl_order_parent_gateway'[parent_gateway_rate],'webxpay_master tbl_order_parent_gateway'[order_id],'webxpay_master tbl_order'[order_id]) RETURN IF(OrderType = 3, BankRate + ParentRate, BankRate)",
        "description": "Combines bank rate with parent gateway rate for type 3 orders.",
        "table": "tbl_order"
    },
    {
        "name": "DateSlicerUsed",
        "formula": "IF(ISFILTERED('webxpay_master tbl_order'[created_at].[Date]), 1, 0)",
        "description": "Indicates if date slicer is being used.",
        "table": "tbl_order"
    },
    {
        "name": "DeclinedOrderCountNew",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), 'webxpay_master tbl_order'[payment_status_id] = 3)",
        "description": "Count of declined orders (alternative).",
        "table": "tbl_order"
    },
    {
        "name": "DeclinedPercentage",
        "formula": "DIVIDE('webxpay_master tbl_store'[DeclinedOrderCount],[AllCount],0)",
        "description": "Percentage of declined orders for store.",
        "table": "tbl_order"
    },
    {
        "name": "Distinct Merchants for ID 2",
        "formula": "CALCULATE(DISTINCTCOUNT( 'webxpay_master tbl_order'[MerchantsName] ), 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Count of distinct merchants with approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "EachRevenueColumn",
        "formula": "IF ( 'webxpay_master tbl_order'[payment_status_id] = 2, ('webxpay_master tbl_order'[OrderBaseAmount] * 'webxpay_master tbl_order'[PM]) / 100, BLANK())",
        "description": "Revenue per order calculated from profit margin.",
        "table": "tbl_order"
    },
    {
        "name": "EachRevenueMeasure",
        "formula": "SUMX ( FILTER ( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 2 ), ('webxpay_master tbl_order'[OrderBaseAmount] * 'webxpay_master tbl_order'[PM]) / 100 )",
        "description": "Total revenue measure from profit margin.",
        "table": "tbl_order"
    },
    {
        "name": "EffectiveExchangeRate",
        "formula": "VAR OrderCurrency = SELECTEDVALUE('webxpay_master tbl_order'[processing_currency_id]) VAR TransactionDate = SELECTEDVALUE('webxpay_master tbl_order'[TransactionDate]) VAR ExchangeRateRaw = SELECTEDVALUE('webxpay_master tbl_order'[exchange_rate]) VAR PaymentStatus = SELECTEDVALUE('webxpay_master tbl_order'[payment_status_id]) VAR ValidExchangeRate = IF(NOT(ISBLANK(ExchangeRateRaw)) && NOT(ISERROR(VALUE(ExchangeRateRaw))), VALUE(ExchangeRateRaw), BLANK()) VAR BuyingRate = CALCULATE(MAX('webxpay_master tbl_exchange_rate'[buying_rate]), 'webxpay_master tbl_exchange_rate'[currency_id] = OrderCurrency, 'webxpay_master tbl_exchange_rate'[date] <= TransactionDate) RETURN IF(PaymentStatus = 2, IF(NOT(ISBLANK(ValidExchangeRate)), ValidExchangeRate, BuyingRate))",
        "description": "Effective exchange rate for approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "EffectiveExchangeRateColumn",
        "formula": "VAR OrderCurrency = 'webxpay_master tbl_order'[processing_currency_id] VAR TransactionDate = 'webxpay_master tbl_order'[TransactionDate] VAR ExchangeRateRaw = 'webxpay_master tbl_order'[exchange_rate] VAR PaymentStatus = 'webxpay_master tbl_order'[payment_status_id] VAR ValidExchangeRate = IF(NOT(ISBLANK(ExchangeRateRaw)) && NOT(ISERROR(VALUE(ExchangeRateRaw))), VALUE(ExchangeRateRaw), BLANK()) VAR LatestRateDate = CALCULATE(MAX('webxpay_master tbl_exchange_rate'[date]), FILTER('webxpay_master tbl_exchange_rate','webxpay_master tbl_exchange_rate'[currency_id] = OrderCurrency && 'webxpay_master tbl_exchange_rate'[date] <= TransactionDate)) VAR BuyingRate = CALCULATE(MAX('webxpay_master tbl_exchange_rate'[buying_rate]), FILTER('webxpay_master tbl_exchange_rate','webxpay_master tbl_exchange_rate'[currency_id] = OrderCurrency && 'webxpay_master tbl_exchange_rate'[date] = LatestRateDate)) RETURN IF(PaymentStatus = 2, IF(NOT(ISBLANK(ValidExchangeRate)), ValidExchangeRate, BuyingRate), BLANK())",
        "description": "Effective exchange rate column for table calculations.",
        "table": "tbl_order"
    },
    {
        "name": "ExchangeRateAsNumber",
        "formula": "IF(NOT(ISBLANK('webxpay_master tbl_order'[exchange_rate])) && NOT(ISERROR(VALUE('webxpay_master tbl_order'[exchange_rate]))), VALUE('webxpay_master tbl_order'[exchange_rate]), BLANK())",
        "description": "Converts exchange rate to number.",
        "table": "tbl_order"
    },
    {
        "name": "ExchangeRateAsNumberApproved",
        "formula": "CALCULATE(MAX('webxpay_master tbl_order'[ExchangeRateAsNumber]), 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Exchange rate for approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "FinalProfit",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [GatewayRateAmountTotal (ForOrder)] - [BankRateAmountTotal (ForOrder)])",
        "description": "Final profit calculation across all gateways.",
        "table": "tbl_order"
    },
    {
        "name": "IPG_TotalAmount_LKR",
        "formula": "CALCULATE ( SUMX ( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[OrderBaseAmount] ), ( 'webxpay_master tbl_order'[CurrencyLabel] = 'LKR' ), 'webxpay_master tbl_order'[payment_status_id] = 2 )",
        "description": "Total LKR amount for approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "IPG_TotalAmount_USD",
        "formula": "CALCULATE(SUMX('webxpay_master tbl_order','webxpay_master tbl_order'[OrderBaseAmount]), 'webxpay_master tbl_order'[processing_currency_id] = '2', 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Total USD amount for approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "IPG_TotalAmount_USD (All)",
        "formula": "CALCULATE(SUM('webxpay_master tbl_order'[total_amount]), 'webxpay_master tbl_order'[processing_currency_id] = '2')",
        "description": "Total USD amount for all orders.",
        "table": "tbl_order"
    },
    {
        "name": "IPG_TotalAmount_USD (Not into LKR)",
        "formula": "CALCULATE(SUM('webxpay_master tbl_order'[total_amount]), 'webxpay_master tbl_order'[processing_currency_id] = '2', 'webxpay_master tbl_order'[created_payment_status] = 'Approved')",
        "description": "Total USD amount for approved orders not converted to LKR.",
        "table": "tbl_order"
    },
    {
        "name": "LKR_Transaction_Count",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2 && 'webxpay_master tbl_order'[processing_currency_id] = '5'))",
        "description": "Count of LKR transactions for approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "Margin",
        "formula": "DIVIDE([FinalProfit], [Total Amount (Paid)], 0)",
        "description": "Calculates margin percentage.",
        "table": "tbl_order"
    },
    {
        "name": "MDR",
        "formula": "CALCULATE(SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2), VAR baseAmount = 'webxpay_master tbl_order'[total_amount] VAR GatewayRate = 'webxpay_master tbl_order'[BankGatewayRateAsNumber] RETURN baseAmount * (GatewayRate / 100)))",
        "description": "Calculates total MDR (Merchant Discount Rate).",
        "table": "tbl_order"
    },
    {
        "name": "Net Profit",
        "formula": "[IPG_TotalAmount_LKR] + [IPG_TotalAmount_USD]",
        "description": "Total net profit from LKR and USD orders.",
        "table": "tbl_order"
    },
    {
        "name": "new_pos_amount",
        "formula": "CALCULATE(SUM('webxpay_master tbl_pos_transactions'[amount]), 'webxpay_master tbl_pos_transactions'[txn_type] IN {'sale', 'amex'})",
        "description": "Total POS amount for sale and amex transactions.",
        "table": "tbl_order"
    },
    {
        "name": "OrderBaseAmount",
        "formula": "VAR IsNotLKR = 'webxpay_master tbl_order'[CurrencyLabel] <> 'LKR' VAR IsSameCurrency = 'webxpay_master tbl_order'[processing_currency_id] = 'webxpay_master tbl_order'[payment_currency_id] VAR HasNoExchangeRate = ISBLANK('webxpay_master tbl_order'[ExchangeRateAsNumber]) RETURN IF( IsSameCurrency && HasNoExchangeRate && IsNotLKR, 'webxpay_master tbl_order'[total_amount] * 'webxpay_master tbl_order'[EffectiveExchangeRateColumn], IF( IsNotLKR, 'webxpay_master tbl_order'[total_amount] * 'webxpay_master tbl_order'[ExchangeRateAsNumber], 'webxpay_master tbl_order'[total_amount] ) )",
        "description": "Calculates base order amount with currency conversion.",
        "table": "tbl_order"
    },
    {
        "name": "OrderStatusPercentage",
        "formula": "VAR TotalOrdersPerMonth = CALCULATE(COUNT('webxpay_master tbl_order'[order_id]), REMOVEFILTERS('webxpay_master tbl_order'[payment_status_id])) VAR StatusOrdersPerMonth = COUNT('webxpay_master tbl_order'[order_id]) RETURN DIVIDE(StatusOrdersPerMonth, TotalOrdersPerMonth, 0) * 100",
        "description": "Percentage of orders by status per month.",
        "table": "tbl_order"
    },
    {
        "name": "PaidTransactionCount",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[total_amount]), 'webxpay_master tbl_order'[created_payment_status] = 'Approved')",
        "description": "Count of paid transactions.",
        "table": "tbl_order"
    },
    {
        "name": "PaymentGatewayRateAsNumber",
        "formula": "IF(NOT(ISBLANK('webxpay_master tbl_order'[payment_gateway_rate])) && NOT(ISERROR(VALUE('webxpay_master tbl_order'[payment_gateway_rate]))), VALUE('webxpay_master tbl_order'[payment_gateway_rate]), BLANK())",
        "description": "Converts payment gateway rate to number.",
        "table": "tbl_order"
    },
    {
        "name": "PM",
        "formula": "'webxpay_master tbl_order'[PaymentGatewayRateAsNumber]-'webxpay_master tbl_order'[BankRateCombined]",
        "description": "Calculates profit margin (Payment Gateway Rate - Bank Rate Combined).",
        "table": "tbl_order"
    },
    {
        "name": "Revenue1",
        "formula": "SUMX(VALUES('webxpay_master tbl_payment_gateway'[payment_gateway_id]), [GatewayRateAmountTotal (ForOrder)])",
        "description": "Total revenue from all payment gateways.",
        "table": "tbl_order"
    },
    {
        "name": "SuccessfulMerchantAttempt",
        "formula": "CALCULATE(DISTINCTCOUNT('webxpay_master tbl_order'[store_id]), 'webxpay_master tbl_order'[created_payment_status] = 'Approved')",
        "description": "Counts distinct merchants with approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "Tenure",
        "formula": "LOOKUPVALUE('webxpay_master tbl_payment_gateway'[tenure], 'webxpay_master tbl_payment_gateway'[payment_gateway_id], LOOKUPVALUE('webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id], 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id], 'webxpay_master tbl_order'[store_payment_gateway_id]))",
        "description": "Gets tenure from payment gateway based on order's store payment gateway.",
        "table": "tbl_order"
    },
    {
        "name": "Total Amount (All)",
        "formula": "CALCULATE(SUM('webxpay_master tbl_order'[total_amount]))",
        "description": "Total amount of all orders.",
        "table": "tbl_order"
    },
    {
        "name": "Total Amount (Day Wise)",
        "formula": "VAR EndDate = TODAY() VAR StartDate = EndDate - 2 RETURN CALCULATE ( SUMX ( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[OrderBaseAmount] ), 'webxpay_master tbl_order'[payment_status_id] = 2, 'webxpay_master tbl_order'[TransactionDate] >= StartDate, 'webxpay_master tbl_order'[TransactionDate] <= EndDate )",
        "description": "Total amount of approved orders for last 3 days.",
        "table": "tbl_order"
    },
    {
        "name": "Total Amount (Margin)",
        "formula": "SUMX(FILTER('webxpay_master tbl_order','webxpay_master tbl_order'[payment_status_id] = 2), 'webxpay_master tbl_order'[total_amount])",
        "description": "Total amount for margin calculation.",
        "table": "tbl_order"
    },
    {
        "name": "Total Amount (Month Wise)",
        "formula": "VAR TodayDate = TODAY() VAR DayLimit = DAY(TodayDate) VAR StartMonthDate = DATE(YEAR(EOMONTH(TodayDate, -2)), MONTH(EOMONTH(TodayDate, -2)), 1) VAR EndDate = DATE( YEAR(TodayDate), MONTH(TodayDate), DayLimit ) RETURN CALCULATE( SUM('webxpay_master tbl_order'[OrderBaseAmount]), 'webxpay_master tbl_order'[payment_status_id] = 2, 'webxpay_master tbl_order'[TransactionDate] >= StartMonthDate && DAY('webxpay_master tbl_order'[TransactionDate]) <= DayLimit )",
        "description": "Total amount of approved orders for last 3 months.",
        "table": "tbl_order"
    },
    {
        "name": "Total Amount (Paid)",
        "formula": "CALCULATE(SUMX('webxpay_master tbl_order','webxpay_master tbl_order'[OrderBaseAmount]), 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Total amount of paid orders.",
        "table": "tbl_order"
    },
    {
        "name": "Total Amount (Year Wise)",
        "formula": "VAR TodayDate = TODAY() VAR CurrentYear = YEAR(TodayDate) VAR MonthLimit = MONTH(TodayDate) VAR DayLimit = DAY(TodayDate) VAR StartDate = DATE(CurrentYear - 2, 1, 1) RETURN CALCULATE( SUM('webxpay_master tbl_order'[OrderBaseAmount]), 'webxpay_master tbl_order'[payment_status_id] = 2, 'webxpay_master tbl_order'[TransactionDate] >= StartDate, ( MONTH('webxpay_master tbl_order'[TransactionDate]) < MonthLimit || ( MONTH('webxpay_master tbl_order'[TransactionDate]) = MonthLimit && DAY('webxpay_master tbl_order'[TransactionDate]) <= DayLimit ) ) )",
        "description": "Total amount of approved orders for last 3 years.",
        "table": "tbl_order"
    },
    {
        "name": "Total Negative Revenue",
        "formula": "SUMX ( FILTER ( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[EachRevenueColumn] < 0 ), 'webxpay_master tbl_order'[EachRevenueColumn] )",
        "description": "Total negative revenue from orders.",
        "table": "tbl_order"
    },
    {
        "name": "Transaction_Count_All (Day Wise)",
        "formula": "VAR EndDate = TODAY() VAR StartDate = EndDate - 2 RETURN CALCULATE( [LKR_Transaction_Count] + [USD_Transaction_Count], 'webxpay_master tbl_order'[TransactionDate] >= StartDate, 'webxpay_master tbl_order'[TransactionDate] <= EndDate )",
        "description": "Total transaction count for last 3 days.",
        "table": "tbl_order"
    },
    {
        "name": "Transaction_Count_All (Last 3 Months)",
        "formula": "VAR TodayDate = TODAY() VAR DayLimit = DAY(TodayDate) VAR StartMonthDate = DATE( YEAR(EOMONTH(TodayDate, -2)), MONTH(EOMONTH(TodayDate, -2)), 1 ) RETURN CALCULATE( [LKR_Transaction_Count] + [USD_Transaction_Count], 'webxpay_master tbl_order'[TransactionDate] >= StartMonthDate, DAY('webxpay_master tbl_order'[TransactionDate]) <= DayLimit )",
        "description": "Total transaction count for last 3 months.",
        "table": "tbl_order"
    },
    {
        "name": "Transaction_Count_All (Paid)",
        "formula": "[LKR_Transaction_Count] + [USD_Transaction_Count]",
        "description": "Total paid transaction count across currencies.",
        "table": "tbl_order"
    },
    {
        "name": "Transaction_Count_All_(Year Wise)",
        "formula": "VAR TodayDate = TODAY() VAR CurrentYear = YEAR(TodayDate) VAR MonthLimit = MONTH(TodayDate) VAR DayLimit = DAY(TodayDate) VAR StartDate = DATE(CurrentYear - 2, 1, 1) RETURN CALCULATE( [LKR_Transaction_Count] + [USD_Transaction_Count], 'webxpay_master tbl_order'[TransactionDate] >= StartDate, ( MONTH('webxpay_master tbl_order'[TransactionDate]) < MonthLimit || ( MONTH('webxpay_master tbl_order'[TransactionDate]) = MonthLimit && DAY('webxpay_master tbl_order'[TransactionDate]) <= DayLimit ) ) )",
        "description": "Total transaction count for last 3 years.",
        "table": "tbl_order"
    },
    {
        "name": "TransactionsThroughInvoice",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_refference_number]), LEFT('webxpay_master tbl_order'[order_refference_number], 1) = 'D')",
        "description": "Count of transactions through invoice.",
        "table": "tbl_order"
    },
    {
        "name": "TransactionsThroughLankaQR",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_refference_number]), LEFT('webxpay_master tbl_order'[order_refference_number], 2) = 'QR')",
        "description": "Count of transactions through Lanka QR.",
        "table": "tbl_order"
    },
    {
        "name": "TransactionsThroughMerchantsWebsite",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_refference_number]), FILTER('webxpay_master tbl_order', LEFT('webxpay_master tbl_order'[order_refference_number], 1) = 'T' && 'webxpay_master tbl_order'[tenure] = 0))",
        "description": "Count of transactions through merchant's website.",
        "table": "tbl_order"
    },
    {
        "name": "TransactionsThroughQR",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_refference_number]), LEFT('webxpay_master tbl_order'[order_refference_number], 1) = 'A')",
        "description": "Count of transactions through QR.",
        "table": "tbl_order"
    },
    {
        "name": "TransactionsThroughTokenization",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[Category Name]), LEFT('webxpay_master tbl_order'[Category Name], LEN('tokenization')) = 'tokenization')",
        "description": "Count of transactions through tokenization.",
        "table": "tbl_order"
    },
    {
        "name": "TransactionsThroughXSplit",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[tenure]), ('webxpay_master tbl_order'[tenure] > 0))",
        "description": "Count of transactions through XSplit.",
        "table": "tbl_order"
    },
    {
        "name": "USD_Transaction_Count",
        "formula": "CALCULATE(COUNTROWS('webxpay_master tbl_order'), 'webxpay_master tbl_order'[processing_currency_id] = '2', 'webxpay_master tbl_order'[payment_status_id] = 2)",
        "description": "Count of USD transactions for approved orders.",
        "table": "tbl_order"
    },
    {
        "name": "USD_Transaction_Count_All",
        "formula": "CALCULATE(COUNT('webxpay_master tbl_order'[order_refference_number]), 'webxpay_master tbl_order'[payment_status_id] = 2, 'webxpay_master tbl_order'[CurrencyLabel] = 'USD')",
        "description": "Count of all USD transactions.",
        "table": "tbl_order"
    },

    # ================ SECTION 5: tbl_ipg_provider TABLE MEASURES ================
    {
        "name": "AbandCount",
        "formula": "VAR SelectedProviderID = SELECTEDVALUE('webxpay_master tbl_ipg_provider'[ipg_provider_id]) VAR GatewayIDs = SELECTCOLUMNS( FILTER( ALL('webxpay_master tbl_payment_gateway'), 'webxpay_master tbl_payment_gateway'[ipg_provider_id] = SelectedProviderID ), 'payment_gateway_id', 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) VAR StoreGatewayIDs = SELECTCOLUMNS( FILTER( ALL('webxpay_master tbl_store_payment_gateway_2'), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] IN GatewayIDs ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN CALCULATE( COUNTROWS('webxpay_master tbl_order'), KEEPFILTERS( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs && 'webxpay_master tbl_order'[payment_status_id] = 1 ) ) )",
        "description": "Counts abandoned orders for the selected IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "AbandodAmountMeas",
        "formula": "VAR SelectedProviderID = SELECTEDVALUE('webxpay_master tbl_ipg_provider'[ipg_provider_id]) VAR GatewayIDs = SELECTCOLUMNS( FILTER( ALL ( 'webxpay_master tbl_payment_gateway' ), 'webxpay_master tbl_payment_gateway'[ipg_provider_id] = SelectedProviderID ), 'payment_gateway_id', 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) VAR StoreGatewayIDs = SELECTCOLUMNS( FILTER( ALL ( 'webxpay_master tbl_store_payment_gateway_2' ), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] IN GatewayIDs ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN CALCULATE( SUM('webxpay_master tbl_order'[total_amount]), KEEPFILTERS( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs && 'webxpay_master tbl_order'[payment_status_id] = 1 ) ) )",
        "description": "Calculates total amount of abandoned orders for the selected IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "AppPercentage",
        "formula": "DIVIDE([TotalAppVol],[Total_App_Dec_Volume],0)",
        "description": "Percentage of approved transactions for IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "ApprovedAmountMeas",
        "formula": "VAR SelectedProviderID = SELECTEDVALUE('webxpay_master tbl_ipg_provider'[ipg_provider_id]) VAR GatewayIDs = SELECTCOLUMNS ( FILTER ( ALL ( 'webxpay_master tbl_payment_gateway' ), 'webxpay_master tbl_payment_gateway'[ipg_provider_id] = SelectedProviderID ), 'payment_gateway_id', 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) VAR StoreGatewayIDs = SELECTCOLUMNS ( FILTER ( ALL ( 'webxpay_master tbl_store_payment_gateway_2' ), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] IN GatewayIDs ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN SUMX ( FILTER ( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[payment_status_id] = 2 && 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs ), IF ( 'webxpay_master tbl_order'[CurrencyLabel] <> 'LKR', 'webxpay_master tbl_order'[total_amount] * 'webxpay_master tbl_order'[EffectiveExchangeRateColumn], 'webxpay_master tbl_order'[total_amount] ) )",
        "description": "Calculates total approved amount for the selected IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "ApprovedCount",
        "formula": "VAR SelectedProviderID = SELECTEDVALUE('webxpay_master tbl_ipg_provider'[ipg_provider_id]) VAR GatewayIDs = SELECTCOLUMNS( FILTER( ALL('webxpay_master tbl_payment_gateway'), 'webxpay_master tbl_payment_gateway'[ipg_provider_id] = SelectedProviderID ), 'payment_gateway_id', 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) VAR StoreGatewayIDs = SELECTCOLUMNS( FILTER( ALL('webxpay_master tbl_store_payment_gateway_2'), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] IN GatewayIDs ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN CALCULATE( COUNTROWS('webxpay_master tbl_order'), KEEPFILTERS( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs && 'webxpay_master tbl_order'[payment_status_id] = 2 ) ) )",
        "description": "Counts approved orders for the selected IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "DeclinedAmountMeas",
        "formula": "VAR SelectedProviderID = SELECTEDVALUE('webxpay_master tbl_ipg_provider'[ipg_provider_id]) VAR GatewayIDs = SELECTCOLUMNS ( FILTER ( ALL ( 'webxpay_master tbl_payment_gateway' ), 'webxpay_master tbl_payment_gateway'[ipg_provider_id] = SelectedProviderID ), 'payment_gateway_id', 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) VAR StoreGatewayIDs = SELECTCOLUMNS ( FILTER ( ALL ( 'webxpay_master tbl_store_payment_gateway_2' ), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] IN GatewayIDs ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN CALCULATE ( SUM ( 'webxpay_master tbl_order'[OrderBaseAmount] ), 'webxpay_master tbl_order'[payment_status_id] = 3, 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs )",
        "description": "Calculates total declined amount for the selected IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "DeclinedOrderCounts",
        "formula": "VAR SelectedProviderID = SELECTEDVALUE('webxpay_master tbl_ipg_provider'[ipg_provider_id]) VAR GatewayIDs = SELECTCOLUMNS( FILTER( ALL('webxpay_master tbl_payment_gateway'), 'webxpay_master tbl_payment_gateway'[ipg_provider_id] = SelectedProviderID ), 'payment_gateway_id', 'webxpay_master tbl_payment_gateway'[payment_gateway_id] ) VAR StoreGatewayIDs = SELECTCOLUMNS( FILTER( ALL('webxpay_master tbl_store_payment_gateway_2'), 'webxpay_master tbl_store_payment_gateway_2'[payment_gateway_id] IN GatewayIDs ), 'store_payment_gateway_id', 'webxpay_master tbl_store_payment_gateway_2'[store_payment_gateway_id] ) RETURN CALCULATE( COUNTROWS('webxpay_master tbl_order'), KEEPFILTERS( FILTER( 'webxpay_master tbl_order', 'webxpay_master tbl_order'[store_payment_gateway_id] IN StoreGatewayIDs && 'webxpay_master tbl_order'[payment_status_id] = 3 ) ) )",
        "description": "Counts declined orders for the selected IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "DecPercentage",
        "formula": "DIVIDE([TotalDecVol],[Total_App_Dec_Volume],0)",
        "description": "Percentage of declined transactions for IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "Total_App_Dec_Volume",
        "formula": "[TotalAppVol] + [TotalDecVol]",
        "description": "Total volume for approval/decline calculations for IPG provider.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "TotalAbandVal",
        "formula": "SUMX(VALUES('webxpay_master tbl_ipg_provider'[ipg_provider_id]), [AbandodAmountMeas])",
        "description": "Total abandoned amount aggregated across all IPG providers.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "TotalAbandVol",
        "formula": "SUMX(VALUES('webxpay_master tbl_ipg_provider'[ipg_provider_id]), [AbandCount])",
        "description": "Total abandoned count aggregated across all IPG providers.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "TotalAppVal",
        "formula": "SUMX(VALUES('webxpay_master tbl_ipg_provider'[ipg_provider_id]), [ApprovedAmountMeas])",
        "description": "Total approved amount aggregated across all IPG providers.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "TotalAppVol",
        "formula": "SUMX(VALUES('webxpay_master tbl_ipg_provider'[ipg_provider_id]), [ApprovedCount])",
        "description": "Total approved count aggregated across all IPG providers.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "TotalDecVal",
        "formula": "SUMX(VALUES('webxpay_master tbl_ipg_provider'[ipg_provider_id]), [DeclinedAmountMeas])",
        "description": "Total declined amount aggregated across all IPG providers.",
        "table": "tbl_ipg_provider"
    },
    {
        "name": "TotalDecVol",
        "formula": "SUMX(VALUES('webxpay_master tbl_ipg_provider'[ipg_provider_id]), [DeclinedOrderCounts])",
        "description": "Total declined count aggregated across all IPG providers.",
        "table": "tbl_ipg_provider"
    }
]