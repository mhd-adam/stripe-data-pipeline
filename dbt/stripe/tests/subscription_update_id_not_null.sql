SELECT
  * 
FROM stripe_data.subscription_updates
WHERE data.object.id IS NULL