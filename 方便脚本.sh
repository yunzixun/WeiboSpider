celery -A tasks.workers -Q  search_crawler,comment_crawler,comment_page_crawler,repost_crawler,repost_page_crawler worker -l info --concurrency=1 -Ofair

celery flower -A tasks.workers --address=0.0.0.0 --port=80