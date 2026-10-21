param([ValidateSet('orders_batch_1.json','orders_batch_2.json')][string]$Batch = 'orders_batch_1.json')
$env:BATCH_FILE = $Batch
try { docker compose run --rm spark }
finally { Remove-Item Env:BATCH_FILE -ErrorAction SilentlyContinue }
