from marshmallow import fields

from parsec.service import BaseService, cmd, event
from parsec.tools import BaseCmdSchema


class cmd_PATH_INFO_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    group = fields.String(missing=None)


class cmd_FOLDER_CREATE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    parents = fields.Boolean(missing=False)
    group = fields.String(missing=None)


class cmd_FOLDER_RENAME_Schema(BaseCmdSchema):
    old_path = fields.String(required=True)
    new_path = fields.String(required=True)
    group = fields.String(missing=None)


class cmd_FOLDER_DELETE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    group = fields.String(missing=None)


class FolderAPIMixin(BaseService):

    on_folder_changed = event('folder_changed')

    @cmd('path_info')
    async def _cmd_PATH_INFO(self, session, msg):
        msg = cmd_PATH_INFO_Schema().load(msg)
        response = await self.path_info(msg['path'], msg['group'])
        return response

    @cmd('folder_create')
    async def _cmd_FOLDER_CREATE(self, session, msg):
        msg = cmd_FOLDER_CREATE_Schema().load(msg)
        await self.folder_create(msg['path'], msg['parents'], msg['group'])
        return {'status': 'ok'}

    @cmd('folder_rename')
    async def _cmd_FOLDER_RENAME(self, session, msg):
        msg = cmd_FOLDER_RENAME_Schema().load(msg)
        await self.folder_rename(msg['old_path'], msg['new_path'], msg['group'])
        return {'status': 'ok'}

    @cmd('folder_delete')
    async def _cmd_FOLDER_DELETE(self, session, msg):
        msg = cmd_FOLDER_DELETE_Schema().load(msg)
        await self.folder_delete(msg['path'], msg['group'])
        return {'status': 'ok'}

    async def path_info(self, path, group=None):
        manifest = await self.get_manifest(group)
        entry = await manifest.path_info(path, children=False)
        if entry:
            entry['type'] = 'file'
            for key in ['key', 'read_trust_seed', 'write_trust_seed']:
                del entry[key]
            # TODO time and size
        else:
            entry = {}
            # Skip mtime and size given that they are too complicated to provide for folder
            entry['type'] = 'folder'
            # TODO time except mtime
            children = await manifest.path_info(path, children=True)
            entry['items'] = sorted(list(children.keys()))
        entry['status'] = 'ok'
        return entry

    async def folder_create(self, path, parents, group):
        manifest = await self.get_manifest(group)
        await manifest.make_folder(path, parents)
        await manifest.commit()

    async def folder_rename(self, old_path, new_path, group=None):
        manifest = await self.get_manifest(group)
        await manifest.rename_file(old_path, new_path)
        await manifest.commit()

    async def folder_delete(self, path, group):
        manifest = await self.get_manifest(group)
        await manifest.remove_folder(path)
        await manifest.commit()
