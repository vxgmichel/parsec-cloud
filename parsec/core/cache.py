from aiocache import caches

caches.set_config({
    'default': {
        'cache': "aiocache.SimpleMemoryCache",
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
        }
    },
})

cache = caches.get('default')
