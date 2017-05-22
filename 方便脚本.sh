celery -A tasks.workers -Q  repost_crawler,repost_page_crawler worker -l info --concurrency=15 -Ofair
celery flower -A tasks.workers --address=0.0.0.0 --port=80