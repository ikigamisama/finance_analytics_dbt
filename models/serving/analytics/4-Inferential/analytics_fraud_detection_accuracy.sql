{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'inferential', 'serving', 'fraud']
    )
}}

WITH fraud_predictions AS (
    SELECT
        CASE
            WHEN fraud_score >= 0.8 THEN 'High Risk'
            WHEN fraud_score >= 0.5 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS predicted_risk,
        is_fraud_flag AS actual_fraud,
        fraud_score,
        transaction_amount_abs
    FROM {{ ref('fact_transactions') }}
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
),

confusion_matrix AS (
    SELECT
        predicted_risk,
        SUM(CASE WHEN actual_fraud = 1 THEN 1 ELSE 0 END) AS true_positive,
        SUM(CASE WHEN actual_fraud = 0 THEN 1 ELSE 0 END) AS false_positive,
        SUM(CASE WHEN actual_fraud = 1 THEN 0 ELSE 1 END) AS false_negative,
        SUM(CASE WHEN actual_fraud = 0 THEN 0 ELSE 1 END) AS true_negative,
        COUNT(*) AS total_predictions
    FROM fraud_predictions
    GROUP BY predicted_risk
)

SELECT
    predicted_risk,
    total_predictions,
    true_positive,
    false_positive,
    false_negative,
    true_negative,
    
    -- Precision
    ROUND(true_positive * 100.0 / NULLIF(true_positive + false_positive, 0), 2) AS precision_pct,
    
    -- Recall (Sensitivity)
    ROUND(true_positive * 100.0 / NULLIF(true_positive + false_negative, 0), 2) AS recall_pct,
    
    -- Specificity
    ROUND(true_negative * 100.0 / NULLIF(true_negative + false_positive, 0), 2) AS specificity_pct,
    
    -- F1 Score
    ROUND(2 * true_positive * 1.0 / NULLIF(2 * true_positive + false_positive + false_negative, 0) * 100, 2) AS f1_score_pct,
    
    -- Accuracy
    ROUND((true_positive + true_negative) * 100.0 / total_predictions, 2) AS accuracy_pct,
    
    -- False Positive Rate
    ROUND(false_positive * 100.0 / NULLIF(false_positive + true_negative, 0), 2) AS false_positive_rate_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM confusion_matrix
ORDER BY 
    CASE predicted_risk
        WHEN 'High Risk' THEN 1
        WHEN 'Medium Risk' THEN 2
        WHEN 'Low Risk' THEN 3
    END