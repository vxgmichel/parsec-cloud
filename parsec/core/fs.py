class FSPipeline:
    def start(self):
        pass

    def stop(self):
        pass

    def _cmd_FILE_CREATE(self, app, req):
        return {'status': 'ok'}

    def _cmd_FILE_READ(self, app, req):
        return {'status': 'ok'}

    def _cmd_FILE_WRITE(self, app, req):
        return {'status': 'ok'}

    def _cmd_STAT(self, app, req):
        return {'status': 'ok'}

    def _cmd_FOLDER_CREATE(self, app, req):
        return {'status': 'ok'}

    def _cmd_MOVE(self, app, req):
        return {'status': 'ok'}

    def _cmd_DELETE(self, app, req):
        return {'status': 'ok'}

    def _cmd_FILE_TRUNCATE(self, app, req):
        return {'status': 'ok'}
