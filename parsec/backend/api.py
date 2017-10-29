from parsec.backend import vlob, user_vlob, message, group, pubkey


def init_api(app):
    # app.register_cmd('subscribe_event', api_subscribe_event)
    # app.register_cmd('unsubscribe_event', api_unsubscribe_event)

    # app.register_cmd('blockstore_get_url', api_blockstore_get_url)

    app.register_cmd('vlob_create', vlob.api_vlob_create)
    app.register_cmd('vlob_read', vlob.api_vlob_read)
    app.register_cmd('vlob_update', vlob.api_vlob_update)

    app.register_cmd('user_vlob_read', user_vlob.api_user_vlob_read)
    app.register_cmd('user_vlob_update', user_vlob.api_user_vlob_update)

    # app.register_cmd('group_read', group.api_group_read)
    # app.register_cmd('group_create', group.api_group_create)
    # app.register_cmd('group_add_identities', group.api_group_add_identities)
    # app.register_cmd('group_remove_identities', group.api_group_remove_identities)

    app.register_cmd('message_get', message.api_message_get)
    app.register_cmd('message_new', message.api_message_new)

    app.register_cmd('pubkey_get', pubkey.api_pubkey_get)
