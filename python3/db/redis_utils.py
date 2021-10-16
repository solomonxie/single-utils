import os
import redis


def get_redis_connection(connect_url):
    r = redis.Redis.from_url(connect_url)
    return r


def main():
    url = os.environ.get('REDIS_CONN_URL')
    r = get_redis_connection(url)
    r.set('foo', 'bar')
    print(r.get('foo'))
    print('OK.')


if __name__ == '__main__':
    main()
