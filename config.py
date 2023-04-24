from environs import Env

env = Env()
env.read_env()

REDIS_PORT = env.str("REDIS_PORT")
REDIS_HOST = env.str("REDIS_HOST")
