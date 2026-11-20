# Superset settings for the prototype dashboard (a throw-away container; not for production).
SECRET_KEY = "prototype-only-key-isit312"
SUPERSET_WEBSERVER_TIMEOUT = 600      # a Hive query on MapReduce takes 15 to 30 s per tile
SQLLAB_TIMEOUT = 600
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False
ROW_LIMIT = 5000
