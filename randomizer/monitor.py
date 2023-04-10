from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler(job_defaults={'max_instances': 5000})
