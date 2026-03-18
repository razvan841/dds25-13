import gevent.monkey
gevent.monkey.patch_all()

bind = "0.0.0.0:8000"
worker_class = "gevent"
workers = 1
worker_connections = 500
timeout = 15
loglevel = "info"
